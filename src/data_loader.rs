use crate::filters::Filters;
use anyhow::Result;
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::File;
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

// Entries

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
    file_path: PathBuf,
    pub metadata: Metadata,
}

impl DataLoader {
    pub fn new(path: PathBuf) -> Result<Self> {
        let metadata = Self::compute_metadata(&path)?;
        Ok(Self {
            file_path: path,
            metadata,
        })
    }

    fn compute_metadata(file_path: &PathBuf) -> Result<Metadata> {
        let file = File::open(file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let mut reader = builder.build()?;

        let mut precisions = HashSet::new();
        let mut series_names = HashSet::new();
        let mut accel_names = HashSet::new();
        let m_values = HashSet::new();
        let accel_param_info: HashMap<String, Vec<String>> = HashMap::new();
        let series_param_info: HashMap<String, Vec<String>> = HashMap::new();

        let mut processed_rows = 0;
        const MAX_SAMPLE_ROWS: usize = 10000;

        while let Some(batch) = reader.next() {
            let batch = batch?;
            if processed_rows >= MAX_SAMPLE_ROWS {
                break;
            }

            // Get precision column
            if let Some(precision_array) = batch.column_by_name("precision") {
                if let Some(string_array) = precision_array.as_any().downcast_ref::<StringArray>() {
                    for val in string_array.iter().take(MAX_SAMPLE_ROWS - processed_rows) {
                        if let Some(val) = val {
                            precisions.insert(val.to_string());
                        }
                    }
                }
            }

            // Get series_name column
            if let Some(series_array) = batch.column_by_name("series_name") {
                if let Some(string_array) = series_array.as_any().downcast_ref::<StringArray>() {
                    for val in string_array.iter().take(MAX_SAMPLE_ROWS - processed_rows) {
                        if let Some(val) = val {
                            series_names.insert(val.to_string());
                            processed_rows += 1;
                        }
                    }
                }
            }

            // Get accel_name column
            if let Some(accel_array) = batch.column_by_name("accel_name") {
                if let Some(string_array) = accel_array.as_any().downcast_ref::<StringArray>() {
                    for val in string_array.iter() {
                        if let Some(val) = val {
                            accel_names.insert(val.to_string());
                        }
                    }
                }
            }
        }

        Ok(Metadata {
            precisions: precisions.into_iter().collect(),
            series_names: series_names.into_iter().collect(),
            accel_names: accel_names.into_iter().collect(),
            m_values: m_values.into_iter().collect(),
            accel_param_info,
            series_param_info,
        })
    }

    pub fn filter_data(&self, filters: &Filters) -> Result<Vec<DataItem>> {
        let file = File::open(&self.file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let mut reader = builder.build()?;

        let mut items = Vec::new();

        while let Some(batch) = reader.next() {
            let batch = batch?;
            for row_idx in 0..batch.num_rows() {
                if let Ok(item) = self.row_to_data_item(&batch, row_idx) {
                    if self.matches_filters(&item, filters) {
                        items.push(item);
                    }
                }
            }
        }

        // Group by (series, accel, m_value, args) and select best item
        let grouped_items = self.group_and_select_best(items);

        Ok(grouped_items)
    }

    fn row_to_data_item(&self, batch: &RecordBatch, row_idx: usize) -> Result<DataItem> {
        // Get partition columns
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

        // Parse series info
        let series_col = batch.column_by_name("series").unwrap();
        let series_array = series_col.as_any().downcast_ref::<StringArray>().unwrap();
        let series_str = series_array.value(row_idx);
        let series_data: serde_json::Value = serde_json::from_str(series_str)
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

        // Parse accel info
        let accel_col = batch.column_by_name("accel").unwrap();
        let accel_array = accel_col.as_any().downcast_ref::<StringArray>().unwrap();
        let accel_str = accel_array.value(row_idx);
        let accel_data: serde_json::Value = serde_json::from_str(accel_str)
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

        // Parse computed values
        let computed = if let Some(computed_col) = batch.column_by_name("computed") {
            let computed_array = computed_col.as_any().downcast_ref::<StringArray>().unwrap();
            let computed_str = computed_array.value(row_idx);

            // Try to parse as JSON array
            if let Ok(computed_data) = serde_json::from_str::<serde_json::Value>(computed_str) {
                if let Some(arr) = computed_data.as_array() {
                    arr.iter()
                        .filter_map(|item| self.parse_computed_value(item))
                        .collect()
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        // Get optional stack_id and error
        let stack_id = batch
            .column_by_name("stack_id")
            .and_then(|col| col.as_any().downcast_ref::<Int32Array>())
            .and_then(|arr| arr.value(row_idx).try_into().ok());

        let error = batch
            .column_by_name("error")
            .and_then(|col| col.as_any().downcast_ref::<Float64Array>())
            .and_then(|arr| arr.value(row_idx).try_into().ok());

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

    fn matches_filters(&self, item: &DataItem, filters: &Filters) -> bool {
        // Check precision filter
        if !filters.precisions.is_empty() && !filters.precisions.contains(&item.precision) {
            return false;
        }

        // Check base series filter
        if !filters.base_series.is_empty() && !filters.base_series.contains(&item.series.name) {
            return false;
        }

        // Check base accel filter
        if !filters.base_accel.is_empty() && !filters.base_accel.contains(&item.accel.name) {
            return false;
        }

        // Check m_values filter
        if !filters.m_values.is_empty() && !filters.m_values.contains(&item.accel.m_value) {
            return false;
        }

        // Check accel params
        for (param_name, expected_values) in &filters.accel_params {
            if !expected_values.is_empty() {
                let actual_value: &str = item
                    .accel
                    .additional_args
                    .get(param_name)
                    .map(|s| s.as_str())
                    .unwrap_or("");
                if !expected_values.contains(actual_value) {
                    return false;
                }
            }
        }

        // Check series params
        for (param_name, expected_values) in &filters.series_params {
            if !expected_values.is_empty() {
                let actual_value: &str = item
                    .series
                    .arguments
                    .get(param_name)
                    .map(|s| s.as_str())
                    .unwrap_or("");
                if !expected_values.contains(actual_value) {
                    return false;
                }
            }
        }

        true
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
}
