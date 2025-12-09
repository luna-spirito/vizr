use crate::symlog::Scientific;
use anyhow::{Context, Result, anyhow};
use datafusion::{
    arrow::{
        array::*,
        datatypes::{
            DataType, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type,
            UInt64Type,
        },
        record_batch::RecordBatch,
    },
    functions::core::expr_ext::FieldAccessor,
    logical_expr::{col, lit},
    prelude::*,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::iter;
#[cfg(feature = "perf_tracing")]
use std::{sync::Mutex, time::Instant};

// NOTE: Currently, it allows accepts entries with values _default/-1. Why? Because DataFusion doesn't like vision and doesn't like singular
// .eq queries over partitioned dataset. We don't know why, it works perfectly on windows.
// By the way, in_list also crashed on Windows.

// Global timing tracking (only enabled with perf_tracing feature)
#[cfg(feature = "perf_tracing")]
static TIMING_STATS: std::sync::LazyLock<Mutex<TimingStats>> =
    std::sync::LazyLock::new(|| Mutex::new(TimingStats::new()));

#[cfg(feature = "perf_tracing")]
#[derive(Default)]
struct TimingStats {
    series_query_time: std::time::Duration,
    series_processing_time: std::time::Duration,
    accelerations_query_time: std::time::Duration,
    accelerations_processing_time: std::time::Duration,
    accelerations_table_access_time: std::time::Duration,
    accelerations_filtering_time: std::time::Duration,
    accelerations_collect_time: std::time::Duration,
    metadata_query_time: std::time::Duration,
    metadata_processing_time: std::time::Duration,
    total_filter_data_time: std::time::Duration,
    series_count: usize,
    accel_count: usize,
}

#[cfg(feature = "perf_tracing")]
impl TimingStats {
    fn new() -> Self {
        Self::default()
    }

    fn reset(&mut self) {
        *self = Self::default();
    }

    fn print_summary(&self) {
        println!("=== Performance Summary ===");
        println!("Total filter_data time: {:?}", self.total_filter_data_time);
        println!("Series query time: {:?}", self.series_query_time);
        println!("Series processing time: {:?}", self.series_processing_time);
        println!(
            "Accelerations query time: {:?}",
            self.accelerations_query_time
        );
        println!(
            "  - Table access time: {:?}",
            self.accelerations_table_access_time
        );
        println!(
            "  - Filtering time: {:?}",
            self.accelerations_filtering_time
        );
        println!("  - Collect time: {:?}", self.accelerations_collect_time);
        println!(
            "Accelerations processing time: {:?}",
            self.accelerations_processing_time
        );
        println!("Metadata query time: {:?}", self.metadata_query_time);
        println!(
            "Metadata processing time: {:?}",
            self.metadata_processing_time
        );
        println!("Series count: {}", self.series_count);
        println!("Acceleration records processed: {}", self.accel_count);
        println!("============================");
    }
}

#[derive(Default, Clone)]
pub struct Filters {
    pub precisions: HashSet<String>,
    pub base_series: HashSet<String>,
    pub base_accel: HashSet<String>,
    pub m_values: HashSet<i32>,
    pub accel_params: HashMap<String, HashSet<String>>,
    pub series_params: HashMap<String, HashSet<String>>,
}

// Build DataFusion filter expressions for struct field parameters
fn filter_params(col_name: &str, filters: &HashMap<String, HashSet<String>>) -> Option<Expr> {
    let mut fin: Option<Expr> = None;

    for (arg, values) in filters {
        let mut curr: Option<Expr> = None;
        for value in values {
            let f = col(col_name).field(arg).eq(lit(value));
            curr = Some(match curr {
                None => f.or(col(col_name).field(arg).eq(lit("_default"))), // ugly fix
                Some(curr) => curr.or(f),
            });
        }
        if let Some(mut curr) = curr {
            curr = curr.or(col(col_name).field(arg).is_null());
            fin = Some(match fin {
                None => curr,
                Some(fin) => fin.and(curr),
            });
        }
    }

    fin
}

// Core
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct ComplexNumber {
    pub real: Scientific,
    pub imag: Scientific,
}

impl ComplexNumber {
    pub fn format(&self) -> String {
        let real_str = self.real.format();
        if self.imag.0.abs() > 0.0 {
            let imag_str = self.imag.format();
            format!("{real_str} + {imag_str}")
        } else {
            real_str
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesPoint {
    pub n: i32,
    pub value: ComplexNumber,
    pub deviation: Scientific,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorInfo {
    pub n: i32,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventInfo {
    pub n: i32,
    pub name: String,
    pub description: String,
}

fn parse_scientific(s: &str) -> Result<Scientific> {
    // Check for scientific notation (e or E)
    if let Some(e_pos) = s.find(['e', 'E']) {
        let mantissa_str = &s[..e_pos];
        let exponent_str = &s[e_pos + 1..];

        // Parse mantissa and exponent
        let mantissa: f64 = mantissa_str
            .parse()
            .with_context(|| format!("Failed to parse mantissa: {}", mantissa_str))?;
        let exponent: i32 = exponent_str
            .parse()
            .with_context(|| format!("Failed to parse exponent: {}", exponent_str))?;

        Ok(Scientific(mantissa, exponent))
    } else {
        // Regular number - parse and compute log10
        let value: f64 = s
            .parse()
            .with_context(|| format!("Failed to parse number: {}", s))?;

        Ok(Scientific(value, 0))
    }
}

// to_x
fn to_str<'a>(name: &str, v: &'a dyn Array) -> Result<Vec<Option<&'a str>>> {
    if let Some(v) = v.as_string_opt::<i32>() {
        Ok(v.iter().collect())
    } else if let Some(v) = v.as_string_opt::<i64>() {
        Ok(v.iter().collect())
    } else if let Some(v) = v.as_string_view_opt() {
        Ok(v.iter().collect())
    } else if let Some(v) = v.as_any().downcast_ref::<NullArray>() {
        Ok(iter::repeat_with(|| None).take(v.len()).collect())
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
    } else if let Some(v) = v.as_any().downcast_ref::<NullArray>() {
        Ok(iter::repeat_with(|| None).take(v.len()).collect())
    } else {
        Err(anyhow!(
            "Expected `{name}` to be int, found {}",
            v.data_type()
        ))
    }
}

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
    } else if let Some(v) = v.as_any().downcast_ref::<NullArray>() {
        Ok(iter::repeat_with(|| None).take(v.len()).collect())
    } else {
        Err(anyhow!(
            "Expected `{name}` to be list, found {}",
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

fn to_complex<'a>(name: &str, v: &'a dyn Array) -> Result<Vec<Option<ComplexNumber>>> {
    if let Some(v) = v.as_struct_opt() {
        if let (Some(real), Some(imag)) = (v.column_by_name("real"), v.column_by_name("imag")) {
            if let (Ok(real), Ok(imag)) = (to_str("", real), to_str("", imag)) {
                let mut res = Vec::new();
                for (i, (real, imag)) in real.into_iter().zip(imag).enumerate() {
                    res.push(if v.is_null(i) {
                        None
                    } else {
                        Some(ComplexNumber {
                            real: parse_scientific(real.context("real is null")?)?,
                            imag: imag
                                .map(|x| parse_scientific(x))
                                .transpose()?
                                .unwrap_or(Scientific(0.0, 0)),
                        })
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

fn to_series_point<'a>(name: &str, v: &'a dyn Array) -> Result<Vec<SeriesPoint>> {
    if let Some(v) = v.as_struct_opt() {
        if let (Some(n), Some(value), Some(deviation)) = (
            v.column_by_name("n"),
            v.column_by_name("value"),
            v.column_by_name("deviation"),
        ) {
            if let (Ok(n), Ok(value), Ok(deviation)) = (
                to_i64("", n),
                to_complex("", value),
                to_str("", deviation),
            ) {
                let mut res = Vec::new();
                for ((n, value), deviation) in n.into_iter().zip(value).zip(deviation) {
                    res.push(SeriesPoint {
                        n: n.context("n not provided")? as i32,
                        value: value.context("value not provided")?,
                        deviation: parse_scientific(deviation.context("deviation not provided")?)?,
                    })
                }
                return Ok(res);
            }
        }
    }
    Err(anyhow!(
        "Expected `{name}` to be {{ n: int, value: {{ real: str, imag: str }}, deviation: str }}, found {}",
        v.data_type()
    ))
}

fn to_error_info<'a>(name: &str, v: &'a dyn Array) -> Result<Vec<ErrorInfo>> {
    if let Some(v) = v.as_struct_opt() {
        if let (Some(n), Some(message)) = (v.column_by_name("n"), v.column_by_name("message")) {
            if let (Ok(n), Ok(message)) = (to_i64("", n), to_str("", message)) {
                let mut res = Vec::new();
                for (n, message) in n.into_iter().zip(message) {
                    res.push(ErrorInfo {
                        n: n.context("n not provided")? as i32,
                        message: message.context("message not provided")?.to_string(),
                    })
                }
                return Ok(res);
            }
        }
    }
    Err(anyhow!(
        "Expected `{name}` to be {{ n: int, message: str }}, found {}",
        v.data_type()
    ))
}

fn to_event_info<'a>(name: &str, v: &'a dyn Array) -> Result<Vec<EventInfo>> {
    if let Some(v) = v.as_struct_opt() {
        if let (Some(n), Some(name_field), Some(description)) = (
            v.column_by_name("n"),
            v.column_by_name("name"),
            v.column_by_name("description"),
        ) {
            if let (Ok(n), Ok(name_field), Ok(description)) = (
                to_i64("", n),
                to_str("", name_field),
                to_str("", description),
            ) {
                let mut res = Vec::new();
                for ((n, name_field), description) in n.into_iter().zip(name_field).zip(description)
                {
                    res.push(EventInfo {
                        n: n.context("n not provided")? as i32,
                        name: name_field.context("name not provided")?.to_string(),
                        description: description.context("description not provided")?.to_string(),
                    })
                }
                return Ok(res);
            }
        }
    }
    Err(anyhow!(
        "Expected `{name}` to be {{ n: int, name: str, description: str }}, found {}",
        v.data_type()
    ))
}

fn to_accel_point<'a>(name: &str, v: &'a dyn Array) -> Result<Vec<Option<AccelPoint>>> {
    if let Some(v) = v.as_struct_opt() {
        if let (Some(value), Some(deviation)) =
            (v.column_by_name("value"), v.column_by_name("deviation"))
        {
            if let (Ok(value), Ok(deviation)) = (to_complex("", value), to_str("", deviation)) {
                let mut res = Vec::new();
                for (i, (value, deviation)) in value.into_iter().zip(deviation).enumerate() {
                    res.push(if v.is_null(i) {
                        None
                    } else {
                        let deviation_str = deviation.context("no deviation in accel point")?;
                        let deviation = parse_scientific(deviation_str)?;
                        Some(AccelPoint {
                            value: value.context("no value in accel point")?,
                            deviation,
                        })
                    });
                }
                return Ok(res);
            }
        }
    }
    Err(anyhow!(
        "Expected `{name}` to be {{ value: {{ real: str, imag: str }}, deviation: str }}, found {}",
        v.data_type()
    ))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesRecord {
    pub precision: String,
    pub series_id: i32,
    pub name: String,
    pub arguments: HashMap<String, String>,
    pub series_limit: ComplexNumber,
    pub computed: Vec<SeriesPoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccelInfo {
    pub name: String,
    pub m_value: i32,
    pub additional_args: HashMap<String, String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct AccelPoint {
    pub value: ComplexNumber,
    pub deviation: Scientific,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccelRecord {
    pub accel_info: AccelInfo,
    pub computed: Vec<Option<AccelPoint>>,
    pub errors: Vec<ErrorInfo>,
    pub events: Vec<EventInfo>,
}

pub type SeriesData = (SeriesRecord, Vec<AccelRecord>);

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
        let series_options = ParquetReadOptions::default().table_partition_cols(vec![
            ("precision".to_string(), DataType::Utf8),
            ("series_name".to_string(), DataType::Utf8),
        ]);
        ctx.register_parquet("series", &format!("{}/series", path), series_options)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to register series parquet: {}", e))?;

        // Register accelerations table
        let accel_options = ParquetReadOptions::default()
            .table_partition_cols(vec![("series_id".to_string(), DataType::Int32)]);
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
        println!("collecting m_values");
        let m_values = Self::get_unique_m_values(ctx).await?;

        println!("collecting accel_param_info");
        let accel_param_info =
            Self::get_unique_param_info(ctx, "accelerations", "additional_args").await?;

        println!("collecting series_param_info");
        let series_param_info = Self::get_unique_param_info(ctx, "series", "arguments").await?;

        Ok(Metadata {
            precisions,
            series_names,
            accel_names,
            m_values,
            accel_param_info,
            series_param_info,
        })
    }

    // Extract unique parameter names and values from struct fields
    async fn get_unique_param_info(
        ctx: &SessionContext,
        table: &str,
        column: &str,
    ) -> Result<HashMap<String, Vec<String>>> {
        let df = ctx.table(table).await?;
        let df = df.select(vec![col(column)])?;
        let batches: Vec<RecordBatch> = df.collect().await.map_err(|e| {
            anyhow::anyhow!("Failed to get unique {} from {}: {}", column, table, e)
        })?;

        let mut param_info: HashMap<String, Vec<String>> = HashMap::new();

        for batch in batches {
            let col = batch.column_by_name(column).context("column not found")?;
            let param_maps = to_struct_str(column, col)?;

            for param_map in param_maps {
                for (key, value) in param_map {
                    param_info.entry(key).or_insert_with(Vec::new).push(value);
                }
            }
        }

        // Remove duplicates and sort each parameter's values
        for values in param_info.values_mut() {
            values.sort();
            values.dedup();
        }

        Ok(param_info)
    }

    // Not null
    async fn get_unique_strings(
        ctx: &SessionContext,
        table: &str,
        column: &str,
    ) -> Result<Vec<String>> {
        #[cfg(feature = "perf_tracing")]
        let query_start = Instant::now();
        let df = ctx.table(table).await?;
        let df = df.select(vec![col(column)])?.distinct()?;
        let batches: Vec<RecordBatch> = df.collect().await.map_err(|e| {
            anyhow::anyhow!("Failed to get unique {} from {}: {}", column, table, e)
        })?;
        #[cfg(feature = "perf_tracing")]
        let query_time = query_start.elapsed();

        #[cfg(feature = "perf_tracing")]
        let processing_start = Instant::now();
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
        #[cfg(feature = "perf_tracing")]
        let processing_time = processing_start.elapsed();

        // Update global stats
        #[cfg(feature = "perf_tracing")]
        if let Ok(mut stats) = TIMING_STATS.lock() {
            stats.metadata_query_time += query_time;
            stats.metadata_processing_time += processing_time;
        }

        res.sort();
        Ok(res)
    }

    // Not null
    async fn get_unique_m_values(ctx: &SessionContext) -> Result<Vec<i32>> {
        #[cfg(feature = "perf_tracing")]
        let query_start = Instant::now();
        let df = ctx.table("accelerations").await?;
        let df = df.select(vec![col("m_value")])?.distinct()?;
        let batches: Vec<RecordBatch> = df.collect().await.map_err(|e| {
            anyhow::anyhow!("Failed to get unique m_values from accelerations: {}", e)
        })?;
        #[cfg(feature = "perf_tracing")]
        let query_time = query_start.elapsed();

        #[cfg(feature = "perf_tracing")]
        let processing_start = Instant::now();
        let mut res = Vec::new();
        for batch in batches {
            let col = batch
                .column_by_name("m_value")
                .context("m_value column not found")?;
            for i in to_i64("m_value", col)? {
                res.push(i.with_context(|| "Didn't expect null in m_value")? as i32);
            }
        }
        #[cfg(feature = "perf_tracing")]
        let processing_time = processing_start.elapsed();

        // Update global stats
        #[cfg(feature = "perf_tracing")]
        if let Ok(mut stats) = TIMING_STATS.lock() {
            stats.metadata_query_time += query_time;
            stats.metadata_processing_time += processing_time;
        }

        Ok(res)
    }
}

// Filtering
impl DataLoader {
    async fn load_accelerations_for_multiple_series(
        &self,
        series_ids: &[i32],
        filters: &Filters,
    ) -> Result<HashMap<i32, Vec<AccelRecord>>> {
        #[cfg(feature = "perf_tracing")]
        let table_start = Instant::now();
        let mut df = self.ctx.table("accelerations").await?;
        #[cfg(feature = "perf_tracing")]
        let table_time = table_start.elapsed();

        // Filter by series_ids
        #[cfg(feature = "perf_tracing")]
        let filter_start = Instant::now();
        {
            let mut filter_expr = col("series_id").eq(lit(-1));
            for &series_id in series_ids.iter() {
                filter_expr = filter_expr.or(col("series_id").eq(lit(series_id)));
            }
            df = df.filter(filter_expr)?;
        }

        // Apply accel filters
        if !filters.base_accel.is_empty() {
            let mut filter_expr = col("accel_name").eq(lit("_default"));
            for a in filters.base_accel.iter() {
                filter_expr = filter_expr.or(col("accel_name").eq(lit(a.clone())));
            }
            df = df.filter(filter_expr)?;
        }

        if !filters.m_values.is_empty() {
            let mut filter_expr = col("m_value").eq(lit("_default"));
            for m in filters.m_values.iter() {
                filter_expr = filter_expr.or(col("m_value").eq(lit(*m)));
            }
            df = df.filter(filter_expr)?;
        }

        // Apply accel_params filters using SQL
        if let Some(param_filter) = filter_params("additional_args", &filters.accel_params) {
            df = df.filter(param_filter)?;
        }
        #[cfg(feature = "perf_tracing")]
        let filter_time = filter_start.elapsed();

        #[cfg(feature = "perf_tracing")]
        let collect_start = Instant::now();
        let batches: Vec<RecordBatch> = df
            .collect()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to execute accelerations query: {}", e))?;
        #[cfg(feature = "perf_tracing")]
        let collect_time = collect_start.elapsed();

        #[cfg(feature = "perf_tracing")]
        let processing_start = Instant::now();
        let mut result: HashMap<i32, Vec<AccelRecord>> = HashMap::new();

        for batch in batches {
            let series_id = to_i64(
                "series_id",
                batch
                    .column_by_name("series_id")
                    .context("No series_id in accelerations")?,
            )?;

            let accel_name = to_str(
                "accel_name",
                batch
                    .column_by_name("accel_name")
                    .context("No accel_name in accelerations")?,
            )?;

            let m_value = to_i64(
                "m_value",
                batch
                    .column_by_name("m_value")
                    .context("No m_value in accelerations")?,
            )?;

            let additional_args = if let Some(col) = batch.column_by_name("additional_args") {
                to_struct_str("additional_args", col)?
            } else {
                vec![HashMap::new(); batch.num_rows()]
            };

            let computed = to_list(
                "computed",
                batch
                    .column_by_name("computed")
                    .context("No computed in accelerations")?,
                |x| to_accel_point("computed.[]", x),
            )?;

            let errors = if let Some(col) = batch.column_by_name("errors") {
                to_list("errors", col, |x| to_error_info("errors.[]", x))?
                    .into_iter()
                    .map(|opt| opt.unwrap_or_default())
                    .collect()
            } else {
                vec![Vec::new(); batch.num_rows()]
            };

            let events = if let Some(col) = batch.column_by_name("events") {
                to_list("events", col, |x| to_event_info("events.[]", x))?
                    .into_iter()
                    .map(|opt| opt.unwrap_or_default())
                    .collect()
            } else {
                vec![Vec::new(); batch.num_rows()]
            };

            for (
                (((((series_id, accel_name), m_value), additional_args), computed), errors),
                events,
            ) in series_id
                .into_iter()
                .zip(accel_name)
                .zip(m_value)
                .zip(additional_args)
                .zip(computed)
                .zip(errors)
                .zip(events)
            {
                let series_id = series_id.context("series_id is null")? as i32;
                let accel_name = accel_name.context("accel_name is null")?.to_string();
                let m_value = m_value.context("m_value is null")? as i32;
                let additional_args = additional_args;

                let accel_record = AccelRecord {
                    accel_info: AccelInfo {
                        name: accel_name,
                        m_value,
                        additional_args,
                    },
                    computed: computed.context("computed is null")?,
                    errors,
                    events,
                };

                result.entry(series_id).or_default().push(accel_record);
            }
        }
        #[cfg(feature = "perf_tracing")]
        let processing_time = processing_start.elapsed();

        // Update global stats
        #[cfg(feature = "perf_tracing")]
        if let Ok(mut stats) = TIMING_STATS.lock() {
            stats.accelerations_query_time += table_time + filter_time + collect_time;
            stats.accelerations_table_access_time += table_time;
            stats.accelerations_filtering_time += filter_time;
            stats.accelerations_collect_time += collect_time;
            stats.accelerations_processing_time += processing_time;
            let total_accel_count: usize = result.values().map(|v| v.len()).sum();
            stats.accel_count += total_accel_count;
        }

        Ok(result)
    }

    pub async fn filter_data(
        &self,
        filters: &Filters,
    ) -> Result<Vec<(SeriesRecord, Vec<AccelRecord>)>> {
        // Reset global timing stats
        #[cfg(feature = "perf_tracing")]
        if let Ok(mut stats) = TIMING_STATS.lock() {
            stats.reset();
        }

        #[cfg(feature = "perf_tracing")]
        let total_start = Instant::now();
        let mut df = self.ctx.table("series").await?;

        // Apply series filters
        if !filters.precisions.is_empty() {
            let mut filter_expr = col("precision").eq(lit("_default"));
            for p in filters.precisions.iter() {
                filter_expr = filter_expr.or(col("precision").eq(lit(p.clone())));
            }
            df = df.filter(filter_expr)?;
        }

        if !filters.base_series.is_empty() {
            let mut filter_expr = col("series_name").eq(lit("_default"));
            for s in filters.base_series.iter() {
                filter_expr = filter_expr.or(col("series_name").eq(lit(s.clone())));
            }
            df = df.filter(filter_expr)?;
        }

        // Apply series_params filters using SQL
        if let Some(param_filter) = filter_params("arguments", &filters.series_params) {
            df = df.filter(param_filter)?;
        }

        #[cfg(feature = "perf_tracing")]
        let query_start = Instant::now();
        let batches: Vec<RecordBatch> = df.collect().await?;
        #[cfg(feature = "perf_tracing")]
        let query_time = query_start.elapsed();

        #[cfg(feature = "perf_tracing")]
        let processing_start = Instant::now();
        let mut series_records = Vec::new();
        let mut series_ids = Vec::new();

        // First, collect all series records and series_ids
        for batch in batches {
            let precision = to_str(
                "precision",
                batch
                    .column_by_name("precision")
                    .context("No precision in series")?,
            )?;
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
                |x| to_series_point("computed.[]", x),
            )?;

            for (((((precision, series_id), series_name), arguments), series_limit), computed) in
                precision
                    .into_iter()
                    .zip(series_id)
                    .zip(series_name)
                    .zip(arguments)
                    .zip(series_limit)
                    .zip(computed)
            {
                let precision = precision.context("precision is null")?.to_string();
                let series_id = series_id.context("series_id is null")? as i32;
                let series_name = series_name.context("name is null")?.to_string();
                let arguments = arguments;
                let computed = computed.context("computed is null")?;

                series_ids.push(series_id);
                series_records.push(SeriesRecord {
                    precision,
                    series_id,
                    name: series_name,
                    arguments,
                    series_limit: series_limit.unwrap_or_default(),
                    computed,
                });
            }
        }

        // Load all accelerations for all series in a single query
        let accelerations_map = if !series_ids.is_empty() {
            self.load_accelerations_for_multiple_series(&series_ids, filters)
                .await?
        } else {
            HashMap::new()
        };

        // Combine series records with their accelerations
        let mut result = Vec::new();
        for series_record in series_records {
            let accels = accelerations_map
                .get(&series_record.series_id)
                .cloned()
                .unwrap_or_default();
            result.push((series_record, accels));
        }

        #[cfg(feature = "perf_tracing")]
        let processing_time = processing_start.elapsed();
        #[cfg(feature = "perf_tracing")]
        let total_time = total_start.elapsed();

        // Update global stats and print summary
        #[cfg(feature = "perf_tracing")]
        if let Ok(mut stats) = TIMING_STATS.lock() {
            stats.series_query_time += query_time;
            stats.series_processing_time += processing_time;
            stats.series_count = result.len();
            stats.total_filter_data_time = total_time;
            stats.print_summary();
        }

        println!("filtering complete");
        Ok(result)
    }
}
