use crate::filters::Filters;
use anyhow::{Context, Result, anyhow};
use datafusion::arrow::array::*;

use datafusion::arrow::datatypes::{
    Float16Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use datafusion::arrow::record_batch::RecordBatch;

use datafusion::logical_expr::{col, lit};
use datafusion::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::iter;

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct Filters {
    pub precisions: HashSet<String>,
    pub base_series: HashSet<String>,
    pub base_accel: HashSet<String>,
    pub m_values: HashSet<i32>,
    pub accel_params: std::collections::HashMap<String, HashSet<String>>,
    pub series_params: std::collections::HashMap<String, HashSet<String>>,
}

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
pub struct Point {
    pub n: i32,
    pub value: ComplexNumber,
}

// to_x
fn to_str<'a>(name: &str, v: &'a dyn Array) -> Result<Vec<Option<&'a str>>> {
    if let Some(v) = v.as_string_opt::<i32>() {
        Ok(v.iter().collect())
    } else if let Some(v) = v.as_string_opt::<i64>() {
        Ok(v.iter().collect())
    } else if let Some(v) = v.as_string_view_opt() {
        Ok(v.iter().collect())
    } else {
        Err(anyhow!(
            "Expected `{name}` to be string, found {}",
            v.data_type()
        ))
    }
}

fn to_i64<'a>(name: &str, v: &'a dyn Array) -> Result<Vec<Option<i64>>> {
    // wildly inefficient
    if let Some(v) = v.as_primitive_opt::<Int8Type>() {
        Ok(v.iter().map(|x| x.map(|x| x as i64)).collect())
    } else if let Some(v) = v.as_primitive_opt::<UInt8Type>() {
        Ok(v.iter().map(|x| x.map(|x| x as i64)).collect())
    } else if let Some(v) = v.as_primitive_opt::<Int16Type>() {
        Ok(v.iter().map(|x| x.map(|x| x as i64)).collect())
    } else if let Some(v) = v.as_primitive_opt::<UInt16Type>() {
        Ok(v.iter().map(|x| x.map(|x| x as i64)).collect())
    } else if let Some(v) = v.as_primitive_opt::<Int32Type>() {
        Ok(v.iter().map(|x| x.map(|x| x as i64)).collect())
    } else if let Some(v) = v.as_primitive_opt::<UInt32Type>() {
        Ok(v.iter().map(|x| x.map(|x| x as i64)).collect())
    } else if let Some(v) = v.as_primitive_opt::<Int64Type>() {
        Ok(v.iter().map(|x| x.map(|x| x as i64)).collect())
    } else if let Some(v) = v.as_primitive_opt::<UInt64Type>() {
        Ok(v.iter().map(|x| x.map(|x| x as i64)).collect())
    } else {
        Err(anyhow!(
            "Expected `{name}` to be int, found {}",
            v.data_type()
        ))
    }
}

// fn to_f64<'a>(name: &str, v: &'a dyn Array) -> Result<Vec<Option<f64>>> {
//     // wildly inefficient
//     if let Some(v) = v.as_primitive_opt::<Float16Type>() {
//         Ok(v.iter().map(|x| x.map(|x| f64::from(x))).collect())
//     } else if let Some(v) = v.as_primitive_opt::<Float32Type>() {
//         Ok(v.iter().map(|x| x.map(|x| x as f64)).collect())
//     } else if let Some(v) = v.as_primitive_opt::<Float64Type>() {
//         Ok(v.iter().map(|x| x.map(|x| x as f64)).collect())
//     } else {
//         Err(anyhow!(
//             "Expected `{name}` to be floats, found {}",
//             v.data_type()
//         ))
//     }
// }

fn to_struct_str<'a>(name: &str, v: &'a dyn Array) -> Result<Vec<HashMap<String, String>>> {
    if let Some(struct_array) = v.as_struct_opt() {
        let mut maps: Vec<HashMap<String, String>> =
            iter::repeat(HashMap::new()).take(v.len()).collect();
        for (field_name, field_array) in struct_array
            .column_names()
            .into_iter()
            .zip(struct_array.columns())
        {
            for (i, val) in (0..field_array.len()).zip(to_str(field_name, field_array)?) {
                if let Some(val) = val {
                    maps[i].insert(field_name.to_string(), val.to_string());
                }
            }
        }
        Ok(maps)
    } else {
        Err(anyhow!(
            "Expected `{name}` to be struct of strings, found {}",
            v.data_type()
        ))
    }
}

fn to_complex<'a>(name: &str, v: &'a dyn Array) -> Result<Vec<ComplexNumber>> {
    if let Some(v) = v.as_struct_opt() {
        if let (Some(real), Some(imag)) = (v.column_by_name("real"), v.column_by_name("imag")) {
            if let (Ok(real), Ok(imag)) = (to_str("", real), to_str("", imag)) {
                let mut res = Vec::new();
                for (real, imag) in real.into_iter().zip(imag) {
                    res.push(ComplexNumber {
                        real: real.map(|x| x.parse()).transpose()?.unwrap_or(0.0),
                        imag: imag.map(|x| x.parse()).transpose()?.unwrap_or(0.0),
                    })
                }
                return Ok(res);
            }
        }
    }
    Err(anyhow!(
        "Expected `{name}` to be {{ real: str, imag: str }}, found {}",
        v.data_type()
    ))
}

// fn to_list_complex(name: &str, v: &'a dyn Array) -> Result<Vec<ComplexNumber>> {
//     if let Ok(x) = v.as_list_opt() {

//     }
// }

fn to_list<R>(
    name: &str,
    v: &dyn Array,
    f: impl for<'b> Fn(&'b dyn Array) -> Result<R>,
) -> Result<Vec<Option<R>>> {
    if let Some(x) = v.as_list_opt::<i32>() {
        let mut res = Vec::new();
        for i in x.iter() {
            res.push(if let Some(i) = i { Some(f(&i)?) } else { None });
        }
        Ok(res)
    } else if let Some(x) = v.as_list_opt::<i64>() {
        let mut res = Vec::new();
        for i in x.iter() {
            res.push(if let Some(i) = i { Some(f(&i)?) } else { None });
        }
        Ok(res)
    } else if let Some(x) = v.as_list_view_opt::<i32>() {
        let mut res = Vec::new();
        for i in x.iter() {
            res.push(if let Some(i) = i { Some(f(&i)?) } else { None });
        }
        Ok(res)
    } else if let Some(x) = v.as_list_view_opt::<i64>() {
        let mut res = Vec::new();
        for i in x.iter() {
            res.push(if let Some(i) = i { Some(f(&i)?) } else { None });
        }
        Ok(res)
    } else {
        Err(anyhow!(
            "Expected `{name}` to be list, found {}",
            v.data_type()
        ))
    }
}

fn to_point<'a>(name: &str, v: &'a dyn Array) -> Result<Vec<Point>> {
    if let Some(v) = v.as_struct_opt() {
        if let (Some(n), Some(real), Some(imag)) = (
            v.column_by_name("n"),
            v.column_by_name("real"),
            v.column_by_name("imag"),
        ) {
            if let (Ok(n), Ok(real), Ok(imag)) = (to_i64("", n), to_str("", real), to_str("", imag))
            {
                let mut res = Vec::new();
                for ((n, real), imag) in n.into_iter().zip(real).zip(imag) {
                    res.push(Point {
                        n: n.context("n not provided")? as i32,
                        value: ComplexNumber {
                            real: real.map(|x| x.parse()).transpose()?.unwrap_or(0.0),
                            imag: imag.map(|x| x.parse()).transpose()?.unwrap_or(0.0),
                        },
                    })
                }
                return Ok(res);
            }
        }
    }
    Err(anyhow!(
        "Expected `{name}` to be {{ n: int, real: str, imag: str }}, found {}",
        v.data_type()
    ))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesRecord {
    pub series_id: i32,
    pub name: String,
    pub arguments: HashMap<String, String>,
    pub series_limit: ComplexNumber,
    pub computed: Vec<Point>,
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
    pub computed: Vec<Option<ComplexNumber>>,
}

pub type DataItem = (SeriesRecord, Vec<AccelRecord>);

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

    // Not null
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

        let mut res = Vec::new();
        for batch in batches {
            let col = batch.column_by_name(column).context("column not found")?;
            for i in to_str(column, col)? {
                res.push(
                    i.with_context(|| format!("Didn't expect null in {column}"))?
                        .to_string(),
                );
            }
        }
        Ok(res)
    }
}

impl DataLoader {
    pub async fn filter_data(
        &self,
        filters: &Filters,
    ) -> Result<Vec<(SeriesRecord, Vec<AccelRecord>)>> {
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

        let batches: Vec<RecordBatch> = df.collect().await?;

        let mut result = Vec::new();
        for batch in batches {
            let series_id = to_i64(
                "series_id",
                batch
                    .column_by_name("series_id")
                    .context("No series_id in series")?,
            )?;
            let series_name = to_str(
                "series_name",
                batch
                    .column_by_name("series_name")
                    .context("No series_name in series")?,
            )?;
            let arguments = to_struct_str(
                "arguments",
                batch
                    .column_by_name("arguments")
                    .context("No arguments in series")?,
            )?;

            let series_limit = to_complex(
                "series_limit",
                batch
                    .column_by_name("series_limit")
                    .context("No series_limit in series")?,
            )?;

            let computed = to_list(
                "computed",
                batch
                    .column_by_name("computed")
                    .context("No computed in series")?,
                |x| to_point("computed.[]", x),
            )?;

            for ((((series_id, series_name), arguments), series_limit), computed) in series_id
                .into_iter()
                .zip(series_name)
                .zip(arguments)
                .zip(series_limit)
                .zip(computed)
            {
                let accels = todo!();
                result.push((
                    SeriesRecord {
                        series_id: series_id.context("series_id is null")? as i32,
                        name: series_name.context("name is null")?.to_string(),
                        arguments,
                        series_limit,
                        computed: computed.context("computed is null")?,
                    },
                    accels,
                ));
            }
        }

        Ok(result)
    }
}

//     pub async fn filter_data(&self, filters: &Filters) -> Result<Vec<DataItem>> {
//     }

//     async fn load_accelerations_for_series(
//         &self,
//         series: &SeriesInfo,
//         filters: &Filters,
//     ) -> Result<Vec<AccelRecord>> {
//         let mut df = self.ctx.table("accelerations").await?;

//         // Filter by series_id
//         df = df.filter(col("series_id").eq(lit(series.series_id)))?;

//         // Apply accel filters
//         if !filters.base_accel.is_empty() {
//             let mut filter_expr =
//                 col("accel_name").eq(lit(filters.base_accel.iter().next().unwrap().clone()));
//             for a in filters.base_accel.iter().skip(1) {
//                 filter_expr = filter_expr.or(col("accel_name").eq(lit(a.clone())));
//             }
//             df = df.filter(filter_expr)?;
//         }

//         let batches: Vec<RecordBatch> = df
//             .collect()
//             .await
//             .map_err(|e| anyhow::anyhow!("Failed to execute accelerations query: {}", e))?;

//         let mut accel_records = Vec::new();
//         for batch in batches {
//             for row_idx in 0..batch.num_rows() {
//                 if let Ok(record) = self.row_to_accel_record(&batch, row_idx) {
//                     accel_records.push(record);
//                 }
//             }
//         }

//         Ok(accel_records)
//     }

//     fn row_to_series_info(&self, batch: &RecordBatch, row_idx: usize) -> Result<SeriesInfo> {
//         let series_id_col = batch.column_by_name("series_id").unwrap();
//         let series_id_array = series_id_col.as_any().downcast_ref::<Int64Array>().unwrap();
//         let series_id = series_id_array.value(row_idx) as i32;

//         let series_name_col = batch.column_by_name("series_name").unwrap();
//         let series_name = Self::extract_string_from_array(series_name_col, row_idx)?;

//         // Parse arguments struct
//         let arguments_col = batch.column_by_name("arguments").unwrap();
//         let arguments_struct = arguments_col
//             .as_any()
//             .downcast_ref::<StructArray>()
//             .unwrap();
//         let arguments = self.extract_string_map_from_struct(arguments_struct, row_idx)?;

//         // Parse series_limit struct
//         let series_limit_col = batch.column_by_name("series_limit").unwrap();
//         let series_limit_struct = series_limit_col
//             .as_any()
//             .downcast_ref::<StructArray>()
//             .unwrap();
//         let series_limit = self.extract_complex_from_struct(series_limit_struct, row_idx)?;

//         // Parse computed list
//         let computed_col = batch.column_by_name("computed").unwrap();
//         let computed_list = computed_col.as_any().downcast_ref::<ListArray>().unwrap();
//         let computed = self.extract_computed_values_from_list(computed_list, row_idx)?;

//         Ok(SeriesInfo {
//             series_id,
//             name: series_name,
//             arguments,
//             series_limit,
//             computed,
//         })
//     }

//     fn row_to_accel_record(&self, batch: &RecordBatch, row_idx: usize) -> Result<AccelRecord> {
//         let accel_name_col = batch.column_by_name("accel_name").unwrap();
//         let accel_name = Self::extract_string_from_array(accel_name_col, row_idx)?;

//         let m_value_col = batch.column_by_name("m_value").unwrap();
//         let m_value_array = m_value_col.as_any().downcast_ref::<Int64Array>().unwrap();
//         let m_value = m_value_array.value(row_idx) as i32;

//         // Parse additional_args (may be null)
//         let additional_args =
//             if let Some(additional_args_col) = batch.column_by_name("additional_args") {
//                 if let Some(additional_args_struct) =
//                     additional_args_col.as_any().downcast_ref::<StructArray>()
//                 {
//                     self.extract_string_map_from_struct(additional_args_struct, row_idx)?
//                 } else {
//                     HashMap::new()
//                 }
//             } else {
//                 HashMap::new()
//             };

//         // Parse computed list
//         let computed_col = batch.column_by_name("computed").unwrap();
//         let computed_list = computed_col.as_any().downcast_ref::<ListArray>().unwrap();
//         let computed = self.extract_complex_list_from_list(computed_list, row_idx)?;

//         let accel_info = AccelInfo {
//             name: accel_name,
//             m_value,
//             additional_args,
//         };

//         Ok(AccelRecord {
//             accel_info,
//             computed,
//         })
//     }

//     fn parse_complex_number(&self, value: &str) -> Result<ComplexNumber> {
//         let value = value.trim();

//         // Handle complex numbers in format "real + imag * i" or "real - imag * i"
//         let re = Regex::new(
//             r"^([+-]?\d*\.?\d+(?:[eE][+-]?\d+)?)\s*([+-])\s*(\d*\.?\d+(?:[eE][+-]?\d+)?)\s*\*\s*i$",
//         )?;

//         if let Some(caps) = re.captures(value) {
//             let real = caps[1].parse::<f64>()?;
//             let imag_sign = if &caps[2] == "-" { -1.0 } else { 1.0 };
//             let imag = caps[3].parse::<f64>()? * imag_sign;
//             return Ok(ComplexNumber { real, imag });
//         }

//         // Handle simple real numbers (including scientific notation)
//         if let Ok(real) = value.parse::<f64>() {
//             return Ok(ComplexNumber { real, imag: 0.0 });
//         }

//         // Try to extract number using regex for complex cases
//         let simple_re = Regex::new(r"([+-]?\d*\.?\d+(?:[eE][+-]?\d+)?)")?;
//         if let Some(caps) = simple_re.captures(value) {
//             let real = caps[1].parse::<f64>()?;
//             return Ok(ComplexNumber { real, imag: 0.0 });
//         }

//         Err(anyhow::anyhow!("Could not parse complex number: {}", value))
//     }

//     fn extract_string_map_from_struct(
//         &self,
//         struct_array: &StructArray,
//         row_idx: usize,
//     ) -> Result<HashMap<String, String>> {
//         let mut map = HashMap::new();
//         for (field_idx, field_name) in struct_array.column_names().iter().enumerate() {
//             let field_array = struct_array.column(field_idx);
//             let value = Self::extract_string_from_array(field_array, row_idx)?;
//             map.insert(field_name.to_string(), value);
//         }
//         Ok(map)
//     }

//     fn extract_complex_from_struct(
//         &self,
//         struct_array: &StructArray,
//         row_idx: usize,
//     ) -> Result<ComplexNumber> {
//         let real_col = struct_array.column_by_name("real").unwrap();
//         let real_str = Self::extract_string_from_array(real_col, row_idx)?;

//         let imag_col = struct_array.column_by_name("imag").unwrap();
//         let imag_str = Self::extract_string_from_array(imag_col, row_idx)?;

//         Ok(ComplexNumber {
//             real: real_str.parse::<f64>().unwrap_or(0.0),
//             imag: imag_str.parse::<f64>().unwrap_or(0.0),
//         })
//     }

//     fn extract_computed_values_from_list(
//         &self,
//         list_array: &ListArray,
//         row_idx: usize,
//     ) -> Result<Vec<ComputedValue>> {
//         let mut computed_values = Vec::new();
//         let value = list_array.value(row_idx);
//         if let Some(struct_array) = value.as_any().downcast_ref::<StructArray>() {
//             for i in 0..struct_array.len() {
//                 if let Ok(computed_value) = self.extract_single_computed_value(struct_array, i) {
//                     computed_values.push(computed_value);
//                 }
//             }
//         }
//         Ok(computed_values)
//     }

//     fn extract_complex_list_from_list(
//         &self,
//         list_array: &ListArray,
//         row_idx: usize,
//     ) -> Result<Vec<ComplexNumber>> {
//         let mut complex_values = Vec::new();
//         let value = list_array.value(row_idx);
//         if let Some(struct_array) = value.as_any().downcast_ref::<StructArray>() {
//             for i in 0..struct_array.len() {
//                 if let Ok(complex) = self.extract_complex_from_struct(struct_array, i) {
//                     complex_values.push(complex);
//                 }
//             }
//         }
//         Ok(complex_values)
//     }

//     fn extract_single_computed_value(
//         &self,
//         struct_array: &StructArray,
//         row_idx: usize,
//     ) -> Result<ComputedValue> {
//         let mut n = 0;
//         let mut value = ComplexNumber {
//             real: 0.0,
//             imag: 0.0,
//         };

//         for (field_idx, field_name) in struct_array.column_names().iter().enumerate() {
//             let field_array = struct_array.column(field_idx);
//             match *field_name {
//                 "n" => {
//                     if let Some(int_array) = field_array.as_any().downcast_ref::<Int64Array>() {
//                         n = int_array.value(row_idx) as i32;
//                     }
//                 }
//                 "value" => {
//                     if let Some(value_struct) = field_array.as_any().downcast_ref::<StructArray>() {
//                         value = self.extract_complex_from_struct(value_struct, row_idx)?;
//                     }
//                 }
//                 _ => {}
//             }
//         }

//         Ok(ComputedValue { n, value })
//     }
// }
