use crate::filters::Filters;
use anyhow::Result;
use datafusion::arrow::array::*;


use datafusion::arrow::record_batch::RecordBatch;

use datafusion::logical_expr::{col, lit};
use datafusion::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
    pub value: ComplexNumber,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesInfo {
    pub series_id: i32,
    pub name: String,
    pub arguments: HashMap<String, String>,
    pub series_limit: ComplexNumber,
    pub computed: Vec<ComputedValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccelInfo {
    pub name: String,
    pub m_value: i32,
    pub additional_args: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccelRecord {
    pub accel_info: AccelInfo,
    pub computed: Vec<ComplexNumber>,
}

pub type DataItem = (SeriesInfo, Vec<AccelRecord>);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub precisions: Vec<String>,
    pub series_names: Vec<String>,
    pub accel_names: Vec<String>,
    pub m_values: Vec<i32>,
    pub accel_param_info: HashMap<String, Vec<String>>,
    pub series_param_info: HashMap<String, Vec<String>>,
}

#[derive(Clone)]
pub struct DataLoader {
    ctx: SessionContext,
    pub metadata: Metadata,
}

impl DataLoader {
    fn extract_string_from_array(array: &dyn Array, row_idx: usize) -> Result<String> {
        // Use DataFusion's utility functions which handle all string types safely
        if let Ok(string_array) = datafusion::common::cast::as_string_array(array) {
            return Ok(string_array.value(row_idx).to_string());
        }
        
        if let Ok(large_string_array) = datafusion::common::cast::as_large_string_array(array) {
            return Ok(large_string_array.value(row_idx).to_string());
        }
        
        if let Ok(string_view_array) = datafusion::common::cast::as_string_view_array(array) {
            return Ok(string_view_array.value(row_idx).to_string());
        }
        
        // For dictionary arrays, use matches! pattern
        use datafusion::arrow::datatypes::DataType;
        if matches!(array.data_type(), DataType::Dictionary(_, _)) {
            // For dictionary arrays, try to get the value as a string
            // This is a simplified approach - can be enhanced later
            return Ok(format!("dictionary_value_{}", row_idx));
        }
        
        Err(anyhow::anyhow!(
            "Cannot extract string from array type {} at row {}",
            array.data_type(), 
            row_idx
        ))
    }

    fn extract_strings_from_array(array: &dyn Array) -> Result<Vec<String>> {
        // Use DataFusion's utility functions which handle all string types safely
        if let Ok(string_array) = datafusion::common::cast::as_string_array(array) {
            return Ok((0..string_array.len())
                .map(|i| string_array.value(i).to_string())
                .collect());
        }
        
        if let Ok(large_string_array) = datafusion::common::cast::as_large_string_array(array) {
            return Ok((0..large_string_array.len())
                .map(|i| large_string_array.value(i).to_string())
                .collect());
        }
        
        if let Ok(string_view_array) = datafusion::common::cast::as_string_view_array(array) {
            return Ok((0..string_view_array.len())
                .map(|i| string_view_array.value(i).to_string())
                .collect());
        }
        
        // For dictionary arrays, use matches! pattern
        use datafusion::arrow::datatypes::DataType;
        if matches!(array.data_type(), DataType::Dictionary(_, _)) {
            // For dictionary arrays, return empty vec for now
            // This can be enhanced later if needed
            return Ok(vec![]);
        }
        
        Err(anyhow::anyhow!(
            "Cannot extract strings from array type {}",
            array.data_type()
        ))
    }

    pub async fn new(path: &str) -> Result<Self> {
        let ctx = SessionContext::new();

        // Register series table
        let series_options = ParquetReadOptions::default().table_partition_cols(vec![(
            "series_name".to_string(),
            datafusion::arrow::datatypes::DataType::Utf8,
        )]);
        ctx.register_parquet("series", &format!("{}/series", path), series_options)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to register series parquet: {}", e))?;

        // Register accelerations table
        let accel_options = ParquetReadOptions::default().table_partition_cols(vec![(
            "series_id".to_string(),
            datafusion::arrow::datatypes::DataType::Int32,
        )]);
        ctx.register_parquet(
            "accelerations",
            &format!("{}/accelerations", path),
            accel_options,
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to register accelerations parquet: {}", e))?;

        let metadata = Self::compute_metadata(&ctx).await?;
        Ok(Self { ctx, metadata })
    }

    async fn compute_metadata(ctx: &SessionContext) -> Result<Metadata> {
        let precisions = Self::get_unique_strings(ctx, "series", "precision").await?;
        let series_names = Self::get_unique_strings(ctx, "series", "series_name").await?;
        let accel_names = Self::get_unique_strings(ctx, "accelerations", "accel_name").await?;

        Ok(Metadata {
            precisions,
            series_names,
            accel_names,
            m_values: vec![],                  // TODO: extract from accel struct
            accel_param_info: HashMap::new(),  // TODO: extract from struct
            series_param_info: HashMap::new(), // TODO: extract from struct
        })
    }

    async fn get_unique_strings(
        ctx: &SessionContext,
        table: &str,
        column: &str,
    ) -> Result<Vec<String>> {
        let df = ctx.table(table).await?;
        let df = df.select(vec![col(column)])?.distinct()?;
        let batches: Vec<RecordBatch> = df.collect().await.map_err(|e| {
            anyhow::anyhow!("Failed to get unique {} from {}: {}", column, table, e)
        })?;

        let mut values = Vec::new();
        for batch in batches {
            if let Some(array) = batch.column_by_name(column) {
                let batch_values = Self::extract_strings_from_array(array)?;
                values.extend(batch_values);
            }
        }
        Ok(values)
    }

    pub async fn filter_data(&self, filters: &Filters) -> Result<Vec<DataItem>> {
        let mut df = self.ctx.table("series").await?;

        // Apply series filters
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

        let batches: Vec<RecordBatch> = df
            .collect()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to execute series query: {}", e))?;

        let mut result = Vec::new();
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                if let Ok(series) = self.row_to_series_info(&batch, row_idx) {
                    let accel_records =
                        self.load_accelerations_for_series(&series, filters).await?;
                    result.push((series, accel_records));
                }
            }
        }

        Ok(result)
    }

    async fn load_accelerations_for_series(
        &self,
        series: &SeriesInfo,
        filters: &Filters,
    ) -> Result<Vec<AccelRecord>> {
        let mut df = self.ctx.table("accelerations").await?;

        // Filter by series_id
        df = df.filter(col("series_id").eq(lit(series.series_id)))?;

        // Apply accel filters
        if !filters.base_accel.is_empty() {
            let mut filter_expr =
                col("accel_name").eq(lit(filters.base_accel.iter().next().unwrap().clone()));
            for a in filters.base_accel.iter().skip(1) {
                filter_expr = filter_expr.or(col("accel_name").eq(lit(a.clone())));
            }
            df = df.filter(filter_expr)?;
        }

        let batches: Vec<RecordBatch> = df
            .collect()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to execute accelerations query: {}", e))?;

        let mut accel_records = Vec::new();
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                if let Ok(record) = self.row_to_accel_record(&batch, row_idx) {
                    accel_records.push(record);
                }
            }
        }

        Ok(accel_records)
    }

    fn row_to_series_info(&self, batch: &RecordBatch, row_idx: usize) -> Result<SeriesInfo> {
        let series_id_col = batch.column_by_name("series_id").unwrap();
        let series_id_array = series_id_col.as_any().downcast_ref::<Int64Array>().unwrap();
        let series_id = series_id_array.value(row_idx) as i32;

        let series_name_col = batch.column_by_name("series_name").unwrap();
        let series_name = Self::extract_string_from_array(series_name_col, row_idx)?;

        // Parse arguments struct
        let arguments_col = batch.column_by_name("arguments").unwrap();
        let arguments_struct = arguments_col
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let arguments = self.extract_string_map_from_struct(arguments_struct, row_idx)?;

        // Parse series_limit struct
        let series_limit_col = batch.column_by_name("series_limit").unwrap();
        let series_limit_struct = series_limit_col
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let series_limit = self.extract_complex_from_struct(series_limit_struct, row_idx)?;

        // Parse computed list
        let computed_col = batch.column_by_name("computed").unwrap();
        let computed_list = computed_col.as_any().downcast_ref::<ListArray>().unwrap();
        let computed = self.extract_computed_values_from_list(computed_list, row_idx)?;

        Ok(SeriesInfo {
            series_id,
            name: series_name,
            arguments,
            series_limit,
            computed,
        })
    }

    fn row_to_accel_record(&self, batch: &RecordBatch, row_idx: usize) -> Result<AccelRecord> {
        let accel_name_col = batch.column_by_name("accel_name").unwrap();
        let accel_name = Self::extract_string_from_array(accel_name_col, row_idx)?;

        let m_value_col = batch.column_by_name("m_value").unwrap();
        let m_value_array = m_value_col.as_any().downcast_ref::<Int64Array>().unwrap();
        let m_value = m_value_array.value(row_idx) as i32;

        // Parse additional_args (may be null)
        let additional_args =
            if let Some(additional_args_col) = batch.column_by_name("additional_args") {
                if let Some(additional_args_struct) =
                    additional_args_col.as_any().downcast_ref::<StructArray>()
                {
                    self.extract_string_map_from_struct(additional_args_struct, row_idx)?
                } else {
                    HashMap::new()
                }
            } else {
                HashMap::new()
            };

        // Parse computed list
        let computed_col = batch.column_by_name("computed").unwrap();
        let computed_list = computed_col.as_any().downcast_ref::<ListArray>().unwrap();
        let computed = self.extract_complex_list_from_list(computed_list, row_idx)?;

        let accel_info = AccelInfo {
            name: accel_name,
            m_value,
            additional_args,
        };

        Ok(AccelRecord {
            accel_info,
            computed,
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

    fn extract_string_map_from_struct(
        &self,
        struct_array: &StructArray,
        row_idx: usize,
    ) -> Result<HashMap<String, String>> {
        let mut map = HashMap::new();
        for (field_idx, field_name) in struct_array.column_names().iter().enumerate() {
            let field_array = struct_array.column(field_idx);
            let value = Self::extract_string_from_array(field_array, row_idx)?;
            map.insert(field_name.to_string(), value);
        }
        Ok(map)
    }

    fn extract_complex_from_struct(
        &self,
        struct_array: &StructArray,
        row_idx: usize,
    ) -> Result<ComplexNumber> {
        let real_col = struct_array.column_by_name("real").unwrap();
        let real_str = Self::extract_string_from_array(real_col, row_idx)?;

        let imag_col = struct_array.column_by_name("imag").unwrap();
        let imag_str = Self::extract_string_from_array(imag_col, row_idx)?;

        Ok(ComplexNumber {
            real: real_str.parse::<f64>().unwrap_or(0.0),
            imag: imag_str.parse::<f64>().unwrap_or(0.0),
        })
    }

    fn extract_computed_values_from_list(
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

    fn extract_complex_list_from_list(
        &self,
        list_array: &ListArray,
        row_idx: usize,
    ) -> Result<Vec<ComplexNumber>> {
        let mut complex_values = Vec::new();
        let value = list_array.value(row_idx);
        if let Some(struct_array) = value.as_any().downcast_ref::<StructArray>() {
            for i in 0..struct_array.len() {
                if let Ok(complex) = self.extract_complex_from_struct(struct_array, i) {
                    complex_values.push(complex);
                }
            }
        }
        Ok(complex_values)
    }

    fn extract_single_computed_value(
        &self,
        struct_array: &StructArray,
        row_idx: usize,
    ) -> Result<ComputedValue> {
        let mut n = 0;
        let mut value = ComplexNumber {
            real: 0.0,
            imag: 0.0,
        };

        for (field_idx, field_name) in struct_array.column_names().iter().enumerate() {
            let field_array = struct_array.column(field_idx);
            match *field_name {
                "n" => {
                    if let Some(int_array) = field_array.as_any().downcast_ref::<Int64Array>() {
                        n = int_array.value(row_idx) as i32;
                    }
                }
                "value" => {
                    if let Some(value_struct) = field_array.as_any().downcast_ref::<StructArray>() {
                        value = self.extract_complex_from_struct(value_struct, row_idx)?;
                    }
                }
                _ => {}
            }
        }

        Ok(ComputedValue { n, value })
    }
}
