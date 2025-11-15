use anyhow::Result;
use polars::lazy::frame::LazyFrame;
use polars::prelude::*;
use polars_rows_iter::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

// Core
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplexNumber {
    pub real: f64,
    pub imag: f64,
}

// impl ComplexNumber {
//     pub fn magnitude(&self) -> f64 {
//         (self.real * self.real + self.imag * self.imag).sqrt()
//     }
// }

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct Filters {
    pub precisions: HashSet<String>,
    pub base_series: HashSet<String>,
    pub base_accel: HashSet<String>,
    pub m_values: HashSet<i32>,
    pub accel_params: std::collections::HashMap<String, HashSet<String>>,
    pub series_params: std::collections::HashMap<String, HashSet<String>>,
}

// impl Filters {
//     pub fn is_empty(&self) -> bool {
//         self.precisions.is_empty()
//             && self.base_series.is_empty()
//             && self.base_accel.is_empty()
//             && self.m_values.is_empty()
//             && self.accel_params.is_empty()
//             && self.series_params.is_empty()
//     }

//     pub fn has_active_filters(&self) -> bool {
//         !self.is_empty()
//     }
// }

// Entries

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputedPoint {
    pub n: i32,
    pub accel_value: ComplexNumber,
    pub partial_sum: ComplexNumber,
    pub accel_value_deviation: ComplexNumber,
    pub partial_sum_deviation: ComplexNumber,
    pub series_value: ComplexNumber, // TODO: duplication
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

#[rustfmt::skip]
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum Precision {
    F32, F64, FLong, Arb,
    CF32, CF64, CFLong, CArb,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub precision: Precision,
    pub series: SeriesInfo,
    pub accel: AccelInfo,
    pub computed: Vec<ComputedPoint>,
    // pub stack_id: Option<i32>,
    pub error: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub series_names: Vec<String>,
    pub accel_names: Vec<String>,
    pub m_values: Vec<i32>,
    pub accel_param_info: HashMap<String, Vec<String>>,
    pub series_param_info: HashMap<String, Vec<String>>,
}

pub struct DataLoader {
    lf: LazyFrame,
    metadata: Metadata,
}

impl DataLoader {
    pub fn new(path: PathBuf) -> Result<Self> {
        let lf = LazyFrame::scan_parquet(PlPath::Local(path.into()), ScanArgsParquet::default())?;
        Ok(Self {
            metadata: Self::compute_metadata(lf.clone())?,
            lf,
        })
    }

    // LLM-written
    fn compute_metadata(lf: LazyFrame) -> Result<Metadata> {
        // Sample data for metadata extraction
        let sample_df = lf.limit(10000).collect()?;

        // Fail fast if required columns are missing
        let required_columns = ["precision", "series_name", "accel_name", "series", "accel"];
        for col in &required_columns {
            if sample_df.column(col).is_err() {
                return Err(anyhow::anyhow!(
                    "Required column '{}' not found in dataset",
                    col
                ));
            }
        }

        let mut series_names = HashSet::new();
        let mut accel_names = HashSet::new();
        let mut m_values = HashSet::new();
        let mut accel_param_info: HashMap<String, Vec<String>> = HashMap::new();
        let mut series_param_info: HashMap<String, Vec<String>> = HashMap::new();

        let series_name_col = sample_df.column("series_name")?;
        let series_name_str = series_name_col.str()?;
        for opt_val in series_name_str.into_iter() {
            if let Some(val) = opt_val {
                series_names.insert(val.to_string());
            }
        }

        let accel_name_col = sample_df.column("accel_name")?;
        let accel_name_str = accel_name_col.str()?;
        for opt_val in accel_name_str.into_iter() {
            if let Some(val) = opt_val {
                accel_names.insert(val.to_string());
            }
        }

        // Extract m_values and parameter info from nested data
        let accel_col = sample_df.column("accel")?;
        let accel_str = accel_col.str()?;
        for opt_val in accel_str.into_iter() {
            if let Some(val) = opt_val {
                if let Ok(accel_data) = serde_json::from_str::<serde_json::Value>(val) {
                    // Extract m_value
                    if let Some(m_val) = accel_data.get("m_value").and_then(|v| v.as_i64()) {
                        m_values.insert(m_val as i32);
                    }

                    // Extract accel parameters
                    if let Some(args) = accel_data
                        .get("additional_args")
                        .and_then(|v| v.as_object())
                    {
                        let params: Vec<String> = args.keys().cloned().collect();
                        let accel_name = accel_data
                            .get("name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown");
                        accel_param_info.insert(accel_name.to_string(), params);
                    }
                }
            }
        }

        // Extract series parameter info
        let series_col = sample_df.column("series")?;
        let series_str = series_col.str()?;
        for opt_val in series_str.into_iter() {
            if let Some(val) = opt_val {
                if let Ok(series_data) = serde_json::from_str::<serde_json::Value>(val) {
                    if let Some(args) = series_data.get("arguments").and_then(|v| v.as_object()) {
                        let params: Vec<String> = args.keys().cloned().collect();
                        let series_name = series_data
                            .get("name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown");
                        series_param_info.insert(series_name.to_string(), params);
                    }
                }
            }
        }

        Ok(Metadata {
            series_names: series_names.into_iter().collect(),
            accel_names: accel_names.into_iter().collect(),
            m_values: m_values.into_iter().collect(),
            accel_param_info,
            series_param_info,
        })
    }

    pub fn filter_data(&self, filters: &Filters) -> _ {
        // #[derive(FromDataFrameRow)]
        // struct RComputedPoint<'a> {
        //     pub n: i32,
        //     pub accel_value: &'a str,
        //     pub partial_sum: &'a str,
        //     pub accel_value_deviation: &'a str,
        //     pub partial_sum_deviation: &'a str,
        //     pub series_value: &'a str, // TODO: duplication
        // }

        // #[derive(FromDataFrameRow)]
        // pub struct RSeriesInfo {
        //     pub arguments: HashMap<String, String>,
        //     pub lim: Option<ComplexNumber>,
        // }

        // #[derive(FromDataFrameRow)]
        // pub struct AccelInfo {
        //     pub name: String,
        //     pub m_value: i32,
        //     pub additional_args: HashMap<String, String>,
        // }

        // #[rustfmt::skip]
        // #[derive(Debug, Clone, Copy, Serialize, Deserialize)]
        // enum Precision {
        //     F32, F64, FLong, Arb,
        //     CF32, CF64, CFLong, CArb,
        // }

        // #[derive(Debug, Clone, Serialize, Deserialize)]
        // pub struct Entry {
        //     pub precision: Precision,
        //     pub series: SeriesInfo,
        //     pub accel: AccelInfo,
        //     pub computed: Vec<ComputedPoint>,
        //     // pub stack_id: Option<i32>,
        //     pub error: Option<f64>,
        // }

        // #[derive(FromDataFrameRow)]
        // struct Row<'a> {
        //     accel_name: &'a str,
        //     series_name: &'a str,
        // }
        // // Build filter expression
        // let mut filtered_lf = self.lf.clone();

        // // Apply basic filters on partition columns using when/then/otherwise
        // if !filters.precisions.is_empty() {
        //     let precision_list: Vec<String> = filters.precisions.iter().cloned().collect();
        //     let mut filter_expr = lit(false);
        //     for precision in precision_list {
        //         filter_expr = filter_expr.or(col("precision").eq(lit(precision)));
        //     }
        //     filtered_lf = filtered_lf.filter(filter_expr);
        // }

        // if !filters.base_series.is_empty() {
        //     let series_list: Vec<String> = filters.base_series.iter().cloned().collect();
        //     let mut filter_expr = lit(false);
        //     for series in series_list {
        //         filter_expr = filter_expr.or(col("series_name").eq(lit(series)));
        //     }
        //     filtered_lf = filtered_lf.filter(filter_expr);
        // }

        // if !filters.base_accel.is_empty() {
        //     let accel_list: Vec<String> = filters.base_accel.iter().cloned().collect();
        //     let mut filter_expr = lit(false);
        //     for accel in accel_list {
        //         filter_expr = filter_expr.or(col("accel_name").eq(lit(accel)));
        //     }
        //     filtered_lf = filtered_lf.filter(filter_expr);
        // }

        // if !filters.base_series.is_empty() {
        //     let series_list: Vec<String> = filters.base_series.iter().cloned().collect();
        //     filtered_lf.filt
        //     filtered_lf = filtered_lf.filter(is_in(
        //         col("series_name"),
        //         lit(Series::new("series_name".into(), series_list)),
        //         true,
        //     ));
        // }

        // if !filters.base_accel.is_empty() {
        //     let accel_list: Vec<String> = filters.base_accel.iter().cloned().collect();
        //     filtered_lf = filtered_lf.filter(is_in(
        //         col("accel_name"),
        //         lit(Series::new("accel_name".into(), accel_list)),
        //         true,
        //     ));
        // }

        // // Collect filtered data
        // let df = filtered_lf.collect()?;

        // // Convert to DataItems
        // let mut items = Vec::new();
        // for row_idx in 0..df.height() {
        //     if let Ok(item) = self.row_to_data_item(&df, row_idx) {
        //         if self.matches_filters(&item, filters) {
        //             items.push(item);
        //         }
        //     }
        // }

        // // Group by (series, accel, m_value, args) and select best item
        // let grouped_items = self.group_and_select_best(items);

        // Ok(grouped_items)
    }

    fn row_to_data_item(&self, df: &DataFrame, row_idx: usize) -> Result<DataItem> {
        // Get partition columns
        let precision_val = df.column("precision")?.get(row_idx)?;
        let precision = precision_val.to_string();

        let series_name_val = df.column("series_name")?.get(row_idx)?;
        let series_name = series_name_val.to_string();

        let accel_name_val = df.column("accel_name")?.get(row_idx)?;
        let accel_name = accel_name_val.to_string();

        // Parse series info
        let series_val = df.column("series")?.get(row_idx)?;
        let series_str = series_val.to_string();
        let series_data: serde_json::Value = serde_json::from_str(&series_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse series JSON: {}", e))?;

        let series = SeriesInfo {
            name: series_name,
            arguments: series_data
                .get("arguments")
                .and_then(|v| v.as_object())
                .map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                .unwrap_or_default(),
            lim: series_data
                .get("lim")
                .and_then(|v| v.as_str())
                .and_then(|s| self.parse_complex_number(s).ok()),
        };

        // Parse accel info
        let accel_val = df.column("accel")?.get(row_idx)?;
        let accel_str = accel_val.to_string();
        let accel_data: serde_json::Value = serde_json::from_str(&accel_str)
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
                .map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                .unwrap_or_default(),
        };

        // Parse computed values
        let computed = if let Ok(computed_col) = df.column("computed") {
            let computed_val = computed_col.get(row_idx)?;
            let computed_str = computed_val.to_string();

            // Try to parse as JSON array
            if let Ok(computed_data) = serde_json::from_str::<serde_json::Value>(&computed_str) {
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
        let stack_id = df
            .column("stack_id")
            .ok()
            .and_then(|col| col.get(row_idx).ok())
            .and_then(|val| val.i32().ok())
            .flatten();

        let error = df
            .column("error")
            .ok()
            .and_then(|col| col.get(row_idx).ok())
            .and_then(|val| val.f64().ok())
            .flatten();

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

    fn matches_filters(&self, item: &DataItem, filters: &crate::filters::Filters) -> bool {
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
                let actual_value = item
                    .accel
                    .additional_args
                    .get(param_name)
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if !expected_values.contains(actual_value) {
                    return false;
                }
            }
        }

        // Check series params
        for (param_name, expected_values) in &filters.series_params {
            if !expected_values.is_empty() {
                let actual_value = item
                    .series
                    .arguments
                    .get(param_name)
                    .and_then(|v| v.as_str())
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
