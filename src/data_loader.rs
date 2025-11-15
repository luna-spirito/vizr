use crate::filters::Filters;
use anyhow::Result;
use datafusion::arrow::array::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::{col, lit};
use datafusion::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

// Core
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplexNumber {
    pub real: f64,
    pub imag: f64,
}

impl ComplexNumber {
    pub fn magnitude(&self) -> f64 {
        (self.real * self.real + self.imag * self.imag).sqrt()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputedValue {
    pub n: i32,
    pub accel_value: Option<ComplexNumber>,
    pub partial_sum: Option<ComplexNumber>,
    pub accel_value_deviation: Option<ComplexNumber>,
    pub partial_sum_deviation: Option<ComplexNumber>,
    pub series_value: Option<ComplexNumber>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesInfo {
    pub name: String,
    pub arguments: HashMap<String, String>,
    pub lim: Option<ComplexNumber>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccelInfo {
    pub name: String,
    pub m_value: i32,
    pub additional_args: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataItem {
    pub precision: String,
    pub series: SeriesInfo,
    pub accel: AccelInfo,
    pub computed: Vec<ComputedValue>,
    pub stack_id: Option<i32>,
    pub error: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub precisions: Vec<String>,
    pub series_names: Vec<String>,
    pub accel_names: Vec<String>,
    pub m_values: Vec<i32>,
    pub accel_param_info: HashMap<String, Vec<String>>,
    pub series_param_info: HashMap<String, Vec<String>>,
}

pub struct DataLoader {
    ctx: SessionContext,
    pub metadata: Metadata,
}

impl DataLoader {
    pub async fn new(path: &str) -> Result<Self> {
        let ctx = SessionContext::new();

        // Register Hive-partitioned parquet dataset as a table
        let mut options = ParquetReadOptions::default();
        options = options.table_partition_cols(vec![
            (
                "precision".to_string(),
                datafusion::arrow::datatypes::DataType::Utf8,
            ),
            (
                "series_name".to_string(),
                datafusion::arrow::datatypes::DataType::Utf8,
            ),
            (
                "accel_name".to_string(),
                datafusion::arrow::datatypes::DataType::Utf8,
            ),
        ]);

        ctx.register_parquet("data", path, options)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to register parquet: {}", e))?;

        let metadata = Self::compute_metadata(&ctx).await?;
        Ok(Self { ctx, metadata })
    }

    async fn compute_metadata(ctx: &SessionContext) -> Result<Metadata> {
        // Get unique values using simple SELECT DISTINCT queries
        let precisions = Self::get_unique_strings(ctx, "precision").await?;
        let series_names = Self::get_unique_strings(ctx, "series_name").await?;
        let accel_names = Self::get_unique_strings(ctx, "accel_name").await?;

        Ok(Metadata {
            precisions,
            series_names,
            accel_names,
            m_values: vec![],                  // TODO: extract from accel struct
            accel_param_info: HashMap::new(),  // TODO: extract from struct
            series_param_info: HashMap::new(), // TODO: extract from struct
        })
    }

    async fn get_unique_strings(ctx: &SessionContext, column: &str) -> Result<Vec<String>> {
        let df = ctx.table("data").await?;
        let df = df.select(vec![col(column)])?.distinct()?;
        let batches: Vec<RecordBatch> = df
            .collect()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get unique {}: {}", column, e))?;

        let mut values = Vec::new();
        for batch in batches {
            if let Some(array) = batch.column_by_name(column) {
                if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
                    for val in string_array.iter() {
                        if let Some(val) = val {
                            values.push(val.to_string());
                        }
                    }
                }
            }
        }
        Ok(values)
    }

    pub async fn filter_data(&self, filters: &Filters) -> Result<Vec<DataItem>> {
        let mut df = self.ctx.table("data").await?;

        // Apply filters using DataFusion's predicate pushdown
        if !filters.precisions.is_empty() {
            let mut filter_expr =
                col("precision").eq(lit(filters.precisions.iter().next().unwrap().clone()));
            for p in filters.precisions.iter().skip(1) {
                filter_expr = filter_expr.or(col("precision").eq(lit(p.clone())));
            }
            df = df.filter(filter_expr)?;
        }

        if !filters.base_series.is_empty() {
            let mut filter_expr =
                col("series_name").eq(lit(filters.base_series.iter().next().unwrap().clone()));
            for s in filters.base_series.iter().skip(1) {
                filter_expr = filter_expr.or(col("series_name").eq(lit(s.clone())));
            }
            df = df.filter(filter_expr)?;
        }

        if !filters.base_accel.is_empty() {
            let mut filter_expr =
                col("accel_name").eq(lit(filters.base_accel.iter().next().unwrap().clone()));
            for a in filters.base_accel.iter().skip(1) {
                filter_expr = filter_expr.or(col("accel_name").eq(lit(a.clone())));
            }
            df = df.filter(filter_expr)?;
        }

        // Execute query with filters
        let batches: Vec<RecordBatch> = df
            .collect()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to execute query: {}", e))?;

        // Convert to DataItems
        let mut items = Vec::new();
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                if let Ok(item) = self.row_to_data_item(&batch, row_idx) {
                    items.push(item);
                }
            }
        }

        // Group by (series, accel, m_value, args) and select best item
        let grouped_items = self.group_and_select_best(items);

        Ok(grouped_items)
    }

    fn row_to_data_item(&self, batch: &RecordBatch, row_idx: usize) -> Result<DataItem> {
        // Get partition columns (Hive partitions become regular columns)
        let precision_col = batch.column_by_name("precision").unwrap();
        let precision_array = precision_col
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let precision = precision_array.value(row_idx).to_string();

        let series_name_col = batch.column_by_name("series_name").unwrap();
        let series_name_array = series_name_col
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let series_name = series_name_array.value(row_idx).to_string();

        let accel_name_col = batch.column_by_name("accel_name").unwrap();
        let accel_name_array = accel_name_col
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let accel_name = accel_name_array.value(row_idx).to_string();

        // Parse series info from struct
        let series_col = batch.column_by_name("series").unwrap();
        let series_struct_array = series_col.as_any().downcast_ref::<StructArray>().unwrap();
        let series_data = self.extract_struct_as_json(&series_struct_array, row_idx)?;
        let series_data: serde_json::Value = serde_json::from_str(&series_data)
            .map_err(|e| anyhow::anyhow!("Failed to parse series JSON: {}", e))?;

        let series = SeriesInfo {
            name: series_name,
            arguments: series_data
                .get("arguments")
                .and_then(|v| v.as_object())
                .map(|obj| {
                    obj.iter()
                        .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                        .collect()
                })
                .unwrap_or_default(),
            lim: series_data
                .get("lim")
                .and_then(|v| v.as_str())
                .and_then(|s| self.parse_complex_number(s).ok()),
        };

        // Parse accel info from struct
        let accel_col = batch.column_by_name("accel").unwrap();
        let accel_struct_array = accel_col.as_any().downcast_ref::<StructArray>().unwrap();
        let accel_data = self.extract_struct_as_json(&accel_struct_array, row_idx)?;
        let accel_data: serde_json::Value = serde_json::from_str(&accel_data)
            .map_err(|e| anyhow::anyhow!("Failed to parse accel JSON: {}", e))?;

        let accel = AccelInfo {
            name: accel_name,
            m_value: accel_data
                .get("m_value")
                .and_then(|v| v.as_i64())
                .unwrap_or(0) as i32,
            additional_args: accel_data
                .get("additional_args")
                .and_then(|v| v.as_object())
                .map(|obj| {
                    obj.iter()
                        .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                        .collect()
                })
                .unwrap_or_default(),
        };

        // Parse computed values from list struct
        let computed = if let Some(computed_col) = batch.column_by_name("computed") {
            let computed_list_array = computed_col.as_any().downcast_ref::<ListArray>().unwrap();
            self.extract_computed_values(&computed_list_array, row_idx)?
        } else {
            Vec::new()
        };

        // Get optional stack_id (string) and error (struct)
        let stack_id = batch
            .column_by_name("stack_id")
            .and_then(|col| col.as_any().downcast_ref::<StringArray>())
            .and_then(|arr| arr.value(row_idx).parse::<i32>().ok());

        // Note: error is now a struct, not a float, so we'll ignore it for now
        let error = None;

        Ok(DataItem {
            precision,
            series,
            accel,
            computed,
            stack_id,
            error,
        })
    }

    fn parse_computed_value(&self, value: &serde_json::Value) -> Option<ComputedValue> {
        let obj = value.as_object()?;

        Some(ComputedValue {
            n: obj.get("n").and_then(|v| v.as_i64()).unwrap_or(0) as i32,
            accel_value: obj
                .get("accel_value")
                .and_then(|v| v.as_str())
                .and_then(|s| self.parse_complex_number(s).ok()),
            partial_sum: obj
                .get("partial_sum")
                .and_then(|v| v.as_str())
                .and_then(|s| self.parse_complex_number(s).ok()),
            accel_value_deviation: obj
                .get("accel_value_deviation")
                .and_then(|v| v.as_str())
                .and_then(|s| self.parse_complex_number(s).ok()),
            partial_sum_deviation: obj
                .get("partial_sum_deviation")
                .and_then(|v| v.as_str())
                .and_then(|s| self.parse_complex_number(s).ok()),
            series_value: obj
                .get("series_value")
                .and_then(|v| v.as_str())
                .and_then(|s| self.parse_complex_number(s).ok()),
        })
    }

    fn parse_complex_number(&self, value: &str) -> Result<ComplexNumber> {
        let value = value.trim();

        // Handle complex numbers in format "real + imag * i" or "real - imag * i"
        let re = Regex::new(
            r"^([+-]?\d*\.?\d+(?:[eE][+-]?\d+)?)\s*([+-])\s*(\d*\.?\d+(?:[eE][+-]?\d+)?)\s*\*\s*i$",
        )?;

        if let Some(caps) = re.captures(value) {
            let real = caps[1].parse::<f64>()?;
            let imag_sign = if &caps[2] == "-" { -1.0 } else { 1.0 };
            let imag = caps[3].parse::<f64>()? * imag_sign;
            return Ok(ComplexNumber { real, imag });
        }

        // Handle simple real numbers (including scientific notation)
        if let Ok(real) = value.parse::<f64>() {
            return Ok(ComplexNumber { real, imag: 0.0 });
        }

        // Try to extract number using regex for complex cases
        let simple_re = Regex::new(r"([+-]?\d*\.?\d+(?:[eE][+-]?\d+)?)")?;
        if let Some(caps) = simple_re.captures(value) {
            let real = caps[1].parse::<f64>()?;
            return Ok(ComplexNumber { real, imag: 0.0 });
        }

        Err(anyhow::anyhow!("Could not parse complex number: {}", value))
    }

    fn group_and_select_best(&self, items: Vec<DataItem>) -> Vec<DataItem> {
        let mut groups: HashMap<String, Vec<DataItem>> = HashMap::new();

        for item in items {
            let group_key = format!(
                "{}|{}|{}|{}|{}",
                item.series.name,
                item.accel.name,
                item.accel.m_value,
                serde_json::to_string(&item.accel.additional_args).unwrap_or_default(),
                serde_json::to_string(&item.series.arguments).unwrap_or_default()
            );

            groups.entry(group_key).or_default().push(item);
        }

        // Select best item from each group (minimal final error)
        let mut result = Vec::new();
        for group_items in groups.values() {
            if let Some(best_item) = group_items.iter().min_by(|a, b| {
                let error_a = self.get_final_error(a);
                let error_b = self.get_final_error(b);
                error_a
                    .partial_cmp(&error_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }) {
                result.push(best_item.clone());
            }
        }

        result
    }

    fn get_final_error(&self, item: &DataItem) -> f64 {
        if item.computed.is_empty() {
            return f64::INFINITY;
        }

        let last_computed = &item.computed[item.computed.len() - 1];
        if let Some(deviation) = &last_computed.accel_value_deviation {
            deviation.magnitude()
        } else {
            f64::INFINITY
        }
    }

    fn extract_struct_as_json(&self, struct_array: &StructArray, row_idx: usize) -> Result<String> {
        let mut obj = serde_json::Map::new();

        for (field_idx, field_name) in struct_array.column_names().iter().enumerate() {
            let field_array = struct_array.column(field_idx);

            if let Some(string_array) = field_array.as_any().downcast_ref::<StringArray>() {
                let value = string_array.value(row_idx);
                obj.insert(
                    field_name.to_string(),
                    serde_json::Value::String(value.to_string()),
                );
            } else if let Some(int_array) = field_array.as_any().downcast_ref::<Int64Array>() {
                let value = int_array.value(row_idx);
                obj.insert(
                    field_name.to_string(),
                    serde_json::Value::Number(serde_json::Number::from(value)),
                );
            }
        }

        Ok(serde_json::Value::Object(obj).to_string())
    }

    fn extract_computed_values(
        &self,
        list_array: &ListArray,
        row_idx: usize,
    ) -> Result<Vec<ComputedValue>> {
        let mut computed_values = Vec::new();

        let value = list_array.value(row_idx);
        if let Some(struct_array) = value.as_any().downcast_ref::<StructArray>() {
            for i in 0..struct_array.len() {
                if let Ok(computed_value) = self.extract_single_computed_value(struct_array, i) {
                    computed_values.push(computed_value);
                }
            }
        }

        Ok(computed_values)
    }

    fn extract_single_computed_value(
        &self,
        struct_array: &StructArray,
        row_idx: usize,
    ) -> Result<ComputedValue> {
        let mut n = 0;
        let mut accel_value = None;
        let mut partial_sum = None;
        let mut accel_value_deviation = None;
        let mut partial_sum_deviation = None;
        let mut series_value = None;

        for (field_idx, field_name) in struct_array.column_names().iter().enumerate() {
            let field_array = struct_array.column(field_idx);

            match *field_name {
                "n" => {
                    if let Some(int_array) = field_array.as_any().downcast_ref::<Int64Array>() {
                        n = int_array.value(row_idx) as i32;
                    }
                }
                "accel_value" => {
                    if let Some(string_array) = field_array.as_any().downcast_ref::<StringArray>() {
                        let value = string_array.value(row_idx);
                        accel_value = self.parse_complex_number(value).ok();
                    }
                }
                "partial_sum" => {
                    if let Some(string_array) = field_array.as_any().downcast_ref::<StringArray>() {
                        let value = string_array.value(row_idx);
                        partial_sum = self.parse_complex_number(value).ok();
                    }
                }
                "accel_value_deviation" => {
                    if let Some(string_array) = field_array.as_any().downcast_ref::<StringArray>() {
                        let value = string_array.value(row_idx);
                        accel_value_deviation = self.parse_complex_number(value).ok();
                    }
                }
                "partial_sum_deviation" => {
                    if let Some(string_array) = field_array.as_any().downcast_ref::<StringArray>() {
                        let value = string_array.value(row_idx);
                        partial_sum_deviation = self.parse_complex_number(value).ok();
                    }
                }
                "series_value" => {
                    if let Some(string_array) = field_array.as_any().downcast_ref::<StringArray>() {
                        let value = string_array.value(row_idx);
                        series_value = self.parse_complex_number(value).ok();
                    }
                }
                _ => {}
            }
        }

        Ok(ComputedValue {
            n,
            accel_value,
            partial_sum,
            accel_value_deviation,
            partial_sum_deviation,
            series_value,
        })
    }
}
