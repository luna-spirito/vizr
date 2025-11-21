use crate::data_loader::{
    AccelInfo, AccelRecord, ComplexNumber, DataLoader, Filters, SeriesData, SeriesRecord,
};
use crate::symlog::symlog_formatter;
use anyhow::Result;
use eframe::egui;

use egui::{Color32, Context, Stroke, Ui, ViewportCommand};
use egui_plot::{Line, MarkerShape, Plot, PlotPoint, Points};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, mpsc};
use std::{mem, slice};

// TODO: Current `symlog` flag implementation is absolutely awful. To be fixed.

type SeriesDataRef<'a> = (&'a SeriesRecord, Vec<&'a AccelRecord>);

fn filterable(entries: &[SeriesData]) -> Filters {
    let mut precisions = HashSet::new();
    let mut base_series = HashSet::new();
    let mut base_accel = HashSet::new();
    let mut m_values = HashSet::new();
    let mut accel_params = HashMap::new();
    let mut series_params = HashMap::new();

    for (series, accel_records) in entries.iter() {
        // Collect series-level fields
        precisions.insert(series.precision.clone());
        base_series.insert(series.name.clone());

        // Collect series parameters
        for (key, value) in &series.arguments {
            series_params
                .entry(key.clone())
                .or_insert_with(HashSet::new)
                .insert(value.clone());
        }

        // Collect acceleration-level fields
        for accel_record in accel_records.iter() {
            base_accel.insert(accel_record.accel_info.name.clone());
            m_values.insert(accel_record.accel_info.m_value);

            // Collect acceleration parameters
            for (key, value) in &accel_record.accel_info.additional_args {
                accel_params
                    .entry(key.clone())
                    .or_insert_with(HashSet::new)
                    .insert(value.clone());
            }
        }
    }

    // Remove fields that have only one unique value (not filterable)
    let mut result = Filters::default();

    if precisions.len() > 1 {
        result.precisions = precisions;
    }
    if base_series.len() > 1 {
        result.base_series = base_series;
    }
    if base_accel.len() > 1 {
        result.base_accel = base_accel;
    }
    if m_values.len() > 1 {
        result.m_values = m_values;
    }

    // Only keep parameters that have multiple values
    for (key, values) in series_params {
        if values.len() > 1 {
            result.series_params.insert(key, values);
        }
    }

    for (key, values) in accel_params {
        if values.len() > 1 {
            result.accel_params.insert(key, values);
        }
    }

    result
}

pub struct Vis {
    // Plot options
    show_partial_sums: bool,
    show_limits: bool,
    show_imaginary: bool,
    force_show_imaginary: bool,

    // Screenshot functionality
    pending_screenshots: HashMap<&'static str, egui::Rect>,

    // Plot hover state for scroll control
    plot_hovered: bool,
}

impl Vis {
    fn request_screenshot(&mut self, ctx: &Context, plot_id: &'static str, plot_rect: egui::Rect) {
        self.pending_screenshots.insert(plot_id, plot_rect);
        // Try without parameters first
        ctx.send_viewport_cmd(ViewportCommand::Screenshot(Default::default()));
    }

    fn handle_screenshot_events(&mut self, ctx: &Context) -> Result<()> {
        let mut screenshots_to_save = Vec::new();

        // Find screenshot events
        for event in &ctx.input(|i| i.events.clone()) {
            if let egui::Event::Screenshot { image, .. } = event {
                // Extract pending screenshots
                for (plot_id, rect) in self.pending_screenshots.drain() {
                    screenshots_to_save.push((plot_id, rect, image.clone()));
                }
            }
        }

        // Save screenshots
        for (plot_id, rect, image_data) in screenshots_to_save {
            self.save_cropped_image(ctx, &plot_id, rect, &image_data)?;
        }

        Ok(())
    }

    fn save_cropped_image(
        &self,
        ctx: &Context,
        plot_id: &str,
        rect: egui::Rect,
        image_data: &std::sync::Arc<egui::ColorImage>,
    ) -> Result<()> {
        let rect = egui::Rect {
            min: egui::Pos2 {
                x: rect.min.x - 50.0,
                y: rect.min.y - 20.0,
            },
            max: egui::Pos2 {
                x: rect.max.x + 50.0,
                y: rect.max.y + 20.0,
            },
        };
        // Convert egui ColorImage to image::DynamicImage
        let width = image_data.size[0] as u32;
        let height = image_data.size[1] as u32;

        // Convert RGBA to RGB
        let mut rgb_data = Vec::with_capacity((width * height * 3) as usize);
        for pixel in &image_data.pixels {
            rgb_data.push(pixel.r());
            rgb_data.push(pixel.g());
            rgb_data.push(pixel.b());
        }

        let img_buffer = image::RgbImage::from_raw(width, height, rgb_data)
            .ok_or_else(|| anyhow::anyhow!("Failed to create RGB buffer"))?;

        let dynamic_img = image::DynamicImage::ImageRgb8(img_buffer);

        // Convert rect coordinates to pixel coordinates
        let pixels_per_point = ctx.pixels_per_point();
        let x = (rect.min.x * pixels_per_point) as u32;
        let y = (rect.min.y * pixels_per_point) as u32;
        let w = ((rect.max.x - rect.min.x) * pixels_per_point) as u32;
        let h = ((rect.max.y - rect.min.y) * pixels_per_point) as u32;

        // Crop image
        let cropped_img = dynamic_img.crop_imm(x, y, w, h);

        // Generate filename with timestamp
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        let filename = format!("{}_{}.png", plot_id, timestamp);

        // Save cropped image
        cropped_img.save(&filename)?;
        println!("Screenshot saved: {}", filename);

        Ok(())
    }
}

fn format_series_name_with_args(series: &SeriesRecord) -> String {
    let mut name = series.precision.clone() + " " + &series.name;

    // Add series parameters
    if !series.arguments.is_empty() {
        let params: Vec<String> = series
            .arguments
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect();
        name.push_str(&format!(" ({})", params.join(", ")));
    }

    name
}

fn format_item_name(series: &SeriesRecord, accel: &AccelInfo) -> String {
    let mut name = format!("{} {} (m={}) ", series.precision, accel.name, accel.m_value);

    // Add accel parameters
    if !accel.additional_args.is_empty() {
        let params: Vec<String> = accel
            .additional_args
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect();
        name.push_str(&format!("({}) ", params.join(", ")));
    }

    name.push_str(&series.name);

    // Add series parameters
    if !series.arguments.is_empty() {
        let params: Vec<String> = series
            .arguments
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect();
        name.push_str(&format!(" ({})", params.join(", ")));
    }

    name
}

// Real & Imaginary & ZeroImaginary / Accel & Partial Sum & Limit
#[derive(Clone, Copy)]
enum LineReal {
    Real,
    Imag { zero: bool },
}
#[derive(Clone, Copy)]
enum LineKind {
    Accel,
    PartialSum,
    Limit,
}
const TOTAL_VIS: usize = 9;

fn vtoind(real: LineReal, kind: LineKind) -> usize {
    use LineKind::*;
    use LineReal::*;
    match (real, kind) {
        (Real, Accel) => 0,
        (Imag { zero: false }, Accel) => 1,
        (Imag { zero: true }, Accel) => 2,

        (Real, PartialSum) => 3,
        (Imag { zero: false }, PartialSum) => 4,
        (Imag { zero: true }, PartialSum) => 5,

        (Real, Limit) => 6,
        (Imag { zero: false }, Limit) => 7,
        (Imag { zero: true }, Limit) => 8,
    }
}

fn indtov(i: usize) -> Option<(LineReal, LineKind)> {
    use LineKind::*;
    use LineReal::*;
    Some(match i {
        0 => (Real, Accel),
        1 => (Imag { zero: false }, Accel),
        2 => (Imag { zero: true }, Accel),

        3 => (Real, PartialSum),
        4 => (Imag { zero: false }, PartialSum),
        5 => (Imag { zero: true }, PartialSum),

        6 => (Real, Limit),
        7 => (Imag { zero: false }, Limit),
        8 => (Imag { zero: true }, Limit),
        _ => return None,
    })
}

type CreateConvergencePlot = impl Fn(&mut Vis, &mut Ui);

#[define_opaque(CreateConvergencePlot)]
fn create_convergence_plot(data: &[SeriesDataRef]) -> CreateConvergencePlot {
    use LineKind::*;
    use LineReal::*;
    let mut lines: [Vec<(String, Vec<PlotPoint>)>; TOTAL_VIS] = [const { Vec::new() }; 9];
    let mut encountered_partial_sum_series = HashSet::new();
    let mut encountered_limit_series = HashSet::new();

    // Calculate X range for 1:1 aspect ratio with fixed Y bounds [-10, 10]
    let mut min_x = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    for (series, _) in data {
        if !series.computed.is_empty() {
            for point in &series.computed {
                min_x = min_x.min(point.n as f64);
                max_x = max_x.max(point.n as f64);
            }
        }
    }

    for (series, accel_records) in data {
        if series.computed.is_empty() {
            continue;
        }

        // Partial sums (one per series)
        if !encountered_partial_sum_series.contains(&series.name) {
            encountered_partial_sum_series.insert(series.name.clone());

            let partial_points = series
                .computed
                .iter()
                .map(|c| PlotPoint::new(c.n as f64, c.value.real.approx_f64()))
                .collect();

            lines[vtoind(Real, PartialSum)].push((
                format!("{} (частичные суммы)", format_series_name_with_args(series)),
                partial_points,
            ));

            // Imaginary partial sums
            let zero = series.computed.iter().all(|c| c.value.imag.0.abs() == 0.0);
            let imag_partial_points: Vec<PlotPoint> = series
                .computed
                .iter()
                .map(|c| PlotPoint::new(c.n as f64, c.value.imag.approx_f64()))
                .collect();

            lines[vtoind(Imag { zero }, PartialSum)].push((
                format!(
                    "{} (частичные суммы, мнимая часть)",
                    format_series_name_with_args(series)
                ),
                imag_partial_points,
            ));
        }

        // Limit line (one per series)
        if !encountered_limit_series.contains(&series.name) {
            let limit = &series.series_limit;
            let x_range: Vec<f64> = series.computed.iter().map(|c| c.n as f64).collect();
            if !x_range.is_empty() {
                let min_x = x_range.iter().fold(f64::INFINITY, |a, &b| a.min(b));
                let max_x = x_range.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));

                // Real limit line
                let real_y = limit.real.approx_f64();
                let limit_points =
                    vec![PlotPoint::new(min_x, real_y), PlotPoint::new(max_x, real_y)];
                lines[vtoind(Real, Limit)].push((
                    format!("{} (предел)", format_series_name_with_args(series)),
                    limit_points,
                ));

                let imag_y = limit.imag.approx_f64();
                let imag_points =
                    vec![PlotPoint::new(min_x, imag_y), PlotPoint::new(max_x, imag_y)];
                lines[vtoind(
                    Imag {
                        zero: limit.imag.0 == 0.0,
                    },
                    Limit,
                )]
                .push((
                    format!(
                        "{} (предел, мнимая часть)",
                        format_series_name_with_args(series)
                    ),
                    imag_points,
                ));

                encountered_limit_series.insert(series.name.clone());
            }
        }

        // Process each acceleration record
        for accel_record in accel_records {
            if accel_record.computed.is_empty() {
                continue;
            }

            let item_name = format_item_name(series, &accel_record.accel_info);

            // Main convergence line - zip series computed with accel computed
            let points = series
                .computed
                .iter()
                .zip(accel_record.computed.iter())
                .filter_map(|(c, accel)| {
                    accel.map(|ap| PlotPoint::new(c.n as f64, ap.value.real.approx_f64()))
                })
                .collect();

            lines[vtoind(Real, Accel)].push((item_name.clone(), points));

            let zero = accel_record
                .computed
                .iter()
                .all(|cn| cn.map_or(true, |x| x.value.imag.0 == 0.0));
            let imag_points = series
                .computed
                .iter()
                .zip(accel_record.computed.iter())
                .filter_map(|(c, accel)| {
                    accel.map(|ap| PlotPoint::new(c.n as f64, ap.value.imag.approx_f64()))
                })
                .collect();

            lines[vtoind(Imag { zero }, Accel)]
                .push((format!("{} (мнимая часть)", item_name), imag_points));
        }
    }

    move |viz, ui| {
        if lines.is_empty() {
            ui.label("Нет данных для отображения");
            return;
        }

        let mut plot = Plot::new("convergence")
            .allow_zoom(true)
            .allow_drag(true)
            .height(900.0)
            .x_axis_label("Итерация n")
            .y_axis_label("Значение")
            .legend(egui_plot::Legend::default());

        // Set fixed Y bounds [-10, 10] and calculate X bounds for 1:1 aspect ratio
        if min_x != f64::INFINITY && max_x != f64::NEG_INFINITY {
            // Y range is fixed at 20 units (from -10 to 10)
            let y_range = 20.0;
            let data_x_range = max_x - min_x;

            // Center X range around data, but ensure it's at least as wide as Y range for 1:1 aspect ratio
            let x_range = data_x_range.max(y_range);
            let x_center = (min_x + max_x) / 2.0;
            let x_min = x_center - x_range / 2.0;
            let x_max = x_center + x_range / 2.0;

            plot = plot
                .auto_bounds(egui::Vec2b::new(false, false)) // Disable auto bounds for both axes
                .include_x(x_min)
                .include_x(x_max)
                .include_y(-10.0)
                .include_y(10.0);
        }

        let plot = plot.show(ui, |plot_ui| {
            for (i, lines) in lines.iter().enumerate() {
                let (real, kind) = indtov(i).unwrap();
                let mut allowed = match real {
                    Real => true,
                    Imag { zero } => viz.show_imaginary && (viz.force_show_imaginary || !zero),
                };
                allowed &= match kind {
                    Accel => true,
                    PartialSum => viz.show_partial_sums,
                    Limit => viz.show_limits,
                };
                if allowed {
                    let color = match (real, kind) {
                        (Real, PartialSum) => Some(Color32::from_rgb(128, 128, 128)),
                        (Imag { zero: _ }, PartialSum) => {
                            Some(egui::Color32::from_rgb(255, 192, 203))
                        }
                        (Real, Limit) => Some(Color32::from_rgb(255, 0, 0)),
                        (Imag { zero: _ }, Limit) => Some(Color32::from_rgb(255, 100, 100)),
                        (Real, Accel) => None,
                        (Imag { zero: _ }, Accel) => Some(Color32::from_rgb(255, 165, 0)),
                    };
                    let stroke = match (real, kind) {
                        (Real, Limit) => Some(Stroke::new(3.0, egui::Color32::from_rgb(255, 0, 0))),
                        (Imag { zero: _ }, Limit) => {
                            Some(Stroke::new(2.0, egui::Color32::from_rgb(255, 100, 100)))
                        }
                        _ => None,
                    };
                    for (name, points) in lines {
                        let mut line = Line::new(points.as_slice()).name(name);
                        if let Some(color) = color {
                            line = line.color(color);
                        }
                        if let Some(stroke) = stroke {
                            line = line.stroke(stroke);
                        }
                        plot_ui.line(line);
                    }
                }
            }
        });
        viz.plot_hovered |= plot.response.hovered();
        ui.horizontal(|ui| {
            if ui.button("📸 Снимок экрана").clicked() {
                viz.request_screenshot(ui.ctx(), "convergence", plot.response.rect);
            }
        });
    }
}

type CreateErrorPlot = impl Fn(&mut Vis, &mut Ui);
#[define_opaque(CreateErrorPlot)]
fn create_error_plot(data: &[SeriesDataRef], symlog: bool) -> CreateErrorPlot {
    let mut lines = Vec::new();

    for (series, accel_records) in data.iter() {
        if series.computed.is_empty() {
            continue;
        }

        for accel_record in accel_records.iter() {
            if accel_record.computed.is_empty() {
                continue;
            }

            let item_name = format_item_name(series, &accel_record.accel_info);

            // Use Euclidean metric with machine epsilon for log scale, clamp to -1000
            let points: Vec<PlotPoint> = series
                .computed
                .iter()
                .zip(accel_record.computed.iter())
                .filter_map(|(c, accel)| {
                    let deviation = accel.as_ref()?.deviation;
                    Some(PlotPoint::new(
                        c.n as f64,
                        if symlog {
                            deviation.symlog()
                        } else {
                            deviation.approx_f64()
                        },
                    ))
                })
                .collect();

            lines.push((item_name, points));
        }
    }

    move |vis, ui| {
        if lines.is_empty() {
            ui.label("Нет данных для отображения");
            return;
        }

        let mut plot = Plot::new("error")
            .allow_zoom(true)
            .allow_drag(true)
            .height(900.0)
            .x_axis_label("Итерация n")
            .y_axis_label("Абсолютная ошибка")
            .legend(egui_plot::Legend::default());
        if symlog {
            plot = plot.y_axis_formatter(|mark, _| symlog_formatter(mark.value));
        }
        let plot = plot.show(ui, |plot_ui| {
            for (n, points) in &lines {
                plot_ui.line(Line::new(points.as_slice()).name(n));
            }
        });
        vis.plot_hovered |= plot.response.hovered();
        ui.horizontal(|ui| {
            if ui.button("📸 Снимок экрана").clicked() {
                vis.request_screenshot(ui.ctx(), "error", plot.response.rect);
            }
        });
    }
}

type CreatePerformancePlot = impl Fn(&mut Vis, &mut Ui);
#[define_opaque(CreatePerformancePlot)]
fn create_performance_plot(data: &[SeriesDataRef], symlog: bool) -> CreatePerformancePlot {
    let mut points = Vec::new();
    let mut min_x = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;

    for (series, accel_records) in data {
        if series.computed.is_empty() {
            continue;
        }

        for accel_record in accel_records {
            if accel_record.computed.is_empty() {
                continue;
            }

            let item_name = format_item_name(series, &accel_record.accel_info);

            // Find minimum error and corresponding iteration
            let mut min_error = f64::INFINITY;
            let mut min_error_iter = 0;

            for (c, accel) in series.computed.iter().zip(accel_record.computed.iter()) {
                if let Some(ap) = accel {
                    let error = if symlog {
                        ap.deviation.symlog()
                    } else {
                        ap.deviation.approx_f64()
                    };

                    if error < min_error {
                        min_error = error;
                        min_error_iter = c.n;
                    }
                }
            }

            if min_error < f64::INFINITY {
                min_x = min_x.min(min_error_iter as f64);
                max_x = max_x.max(min_error_iter as f64);
                points.push((item_name, PlotPoint::new(min_error_iter as f64, min_error)));
            }
        }
    }

    move |vis, ui| {
        if points.is_empty() {
            ui.label("Нет данных для отображения");
            return;
        }

        let mut plot = Plot::new("performance")
            .allow_zoom(true)
            .allow_drag(true)
            .height(900.0)
            .x_axis_label("Итерация достижения минимальной ошибки")
            .y_axis_label("Минимальная ошибка")
            .legend(egui_plot::Legend::default());
        if symlog {
            plot = plot.y_axis_formatter(|mark, _| symlog_formatter(mark.value));
        }
        let plot = plot.show(ui, |plot_ui| {
            for (name, points) in &points {
                plot_ui.points(
                    Points::new(slice::from_ref(points))
                        .name(name)
                        .shape(MarkerShape::Circle)
                        .radius(4.0),
                );
            }
        });
        vis.plot_hovered |= plot.response.hovered();
        ui.horizontal(|ui| {
            if ui.button("📸 Снимок экрана").clicked() {
                vis.request_screenshot(ui.ctx(), "performance", plot.response.rect);
            }
        });
    }
}

type CreateAccelRecordsTable = impl Fn(&mut Ui);
#[define_opaque(CreateAccelRecordsTable)]
fn create_accel_records_table(data: &[SeriesDataRef]) -> CreateAccelRecordsTable {
    type TableRow = (
        String,      // 0: Series ID
        String,      // 1: Название ряда
        String,      // 2: Precision
        String,      // 3: Предел ряда
        String,      // 4: Параметры ряда
        String,      // 5: Название ускорения
        String,      // 6: M
        String,      // 7: Параметры ускорения
        Vec<String>, // 8: S_n ряда values
        Vec<String>, // 9: S_n ускорения values
        Vec<String>, // 10: Отклонения values
        Vec<String>, // 11: Ошибки values
        Vec<String>, // 12: Событий values
    );
    let mut table_rows: Vec<TableRow> = Vec::new();
    for (series, accel_records) in data {
        for accel_record in accel_records {
            // Series parameters
            let series_params = if series.arguments.is_empty() {
                "(нет параметров)".to_string()
            } else {
                let params: Vec<String> = series
                    .arguments
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect();
                params.join(", ")
            };
            // Acceleration parameters
            let accel_params = if accel_record.accel_info.additional_args.is_empty() {
                "(нет параметров)".to_string()
            } else {
                let params: Vec<String> = accel_record
                    .accel_info
                    .additional_args
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect();
                params.join(", ")
            };
            // S_n ряда values
            let series_values: Vec<String> = series
                .computed
                .iter()
                .map(|c| format!("n={}: {}", c.n, c.value.format()))
                .collect();
            // S_n ускорения values
            let accel_values: Vec<String> = accel_record
                .computed
                .iter()
                .enumerate()
                .filter_map(|(i, j)| Some((i, j.as_ref()?)))
                .map(|(j, c)| format!("n={}: {}", j, c.value.format()))
                .collect();
            // Отклонения values
            let crude_deviation = |x: ComplexNumber| {
                ((x.real.approx_f64() - series.series_limit.real.approx_f64()).powi(2)
                    + (x.imag.approx_f64() - series.series_limit.imag.approx_f64()).powi(2))
                .sqrt()
            };
            let mut deviation_values = Vec::new();
            let mut sum_deviation = 0.0;
            let mut sum_series_deviation = 0.0;
            let mut len = 0;

            for (s, a) in series.computed.iter().zip(accel_record.computed.iter()) {
                if let Some(a) = a {
                    sum_series_deviation += crude_deviation(s.value);
                    sum_deviation += a.deviation.approx_f64();
                    len += 1;

                    deviation_values.push(format!(
                        "n={}: {} (vs {:.9})",
                        s.n,
                        a.deviation.format(),
                        crude_deviation(s.value)
                    ));
                }
            }

            // Add summary as first deviation value if we have data
            if len > 0 {
                let summary = format!(
                    "Среднее: {:.9} (vs {:.9})",
                    sum_deviation / len as f64,
                    sum_series_deviation / len as f64
                );
                deviation_values.insert(0, summary);
            }
            // Ошибки values
            let error_values: Vec<String> = accel_record
                .errors
                .iter()
                .map(|error| format!("n={}: {}", error.n, error.message))
                .collect();
            // Событий values
            let event_values: Vec<String> = accel_record
                .events
                .iter()
                .map(|event| format!("n={}: {} - {}", event.n, event.name, event.description))
                .collect();
            table_rows.push((
                series.series_id.to_string(),
                series.name.clone(),
                series.precision.clone(),
                series.series_limit.format(),
                series_params,
                accel_record.accel_info.name.clone(),
                accel_record.accel_info.m_value.to_string(),
                accel_params,
                series_values,
                accel_values,
                deviation_values,
                error_values,
                event_values,
            ));
        }
    }
    move |ui| {
        if table_rows.is_empty() {
            ui.label("Нет данных для отображения");
            return;
        }
        // Set spacing for spacious cells
        ui.spacing_mut().item_spacing = egui::vec2(20.0, 10.0);
        // Create grid
        egui::Grid::new("accel_table")
            .striped(true)
            .max_col_width(100.0)
            .show(ui, |ui| {
                // Header row
                ui.label(egui::RichText::new("Series ID").strong());
                ui.label(egui::RichText::new("Название ряда").strong());
                ui.label(egui::RichText::new("Precision").strong());
                ui.label(egui::RichText::new("Предел ряда").strong());
                ui.label(egui::RichText::new("Параметры ряда").strong());
                ui.label(egui::RichText::new("Название ускорения").strong());
                ui.label(egui::RichText::new("M").strong());
                ui.label(egui::RichText::new("Параметры ускорения").strong());
                ui.label(egui::RichText::new("S_n ряда").strong());
                ui.label(egui::RichText::new("S_n ускорения").strong());
                ui.label(egui::RichText::new("Отклонения").strong());
                ui.label(egui::RichText::new("Ошибки").strong());
                ui.label(egui::RichText::new("Событий").strong());
                ui.end_row();
                // Data rows
                for (i, row) in table_rows.iter().enumerate() {
                    ui.add(egui::Label::new(&row.0).wrap()); // Series ID
                    ui.add(egui::Label::new(&row.1).wrap()); // Название ряда
                    ui.add(egui::Label::new(&row.2).wrap()); // Precision
                    ui.add(egui::Label::new(&row.3).wrap()); // Предел ряда
                    ui.add(egui::Label::new(&row.4).wrap()); // Параметры ряда
                    ui.add(egui::Label::new(&row.5).wrap()); // Название ускорения
                    ui.add(egui::Label::new(&row.6).wrap()); // M
                    ui.add(egui::Label::new(&row.7).wrap()); // Параметры ускорения
                    // S_n ряда
                    if row.8.is_empty() {
                        ui.add(egui::Label::new("(нет точек)").wrap());
                    } else {
                        ui.collapsing(format!("#{i}: {} значений", row.8.len()), |ui| {
                            for value in &row.8 {
                                ui.label(value);
                            }
                        });
                    }
                    // S_n ускорения
                    if row.9.is_empty() {
                        ui.add(egui::Label::new("(нет точек)").wrap());
                    } else {
                        ui.collapsing(format!("#{i}: {} значений", row.9.len()), |ui| {
                            for value in &row.9 {
                                ui.label(value);
                            }
                        });
                    }
                    // Отклонения
                    if row.10.is_empty() {
                        ui.add(egui::Label::new("(нет данных)").wrap());
                    } else {
                        ui.collapsing(format!("#{i}: {} значений", row.10.len()), |ui| {
                            for value in &row.10 {
                                ui.label(value);
                            }
                        });
                    }
                    // Ошибки
                    if row.11.is_empty() {
                        ui.add(egui::Label::new("(нет ошибок)").wrap());
                    } else {
                        ui.collapsing(format!("#{i}: {} ошибок", row.11.len()), |ui| {
                            for value in &row.11 {
                                ui.label(value);
                            }
                        });
                    }
                    // Событий
                    if row.12.is_empty() {
                        ui.add(egui::Label::new("(нет событий)").wrap());
                    } else {
                        ui.collapsing(format!("#{i}: {} событий", row.12.len()), |ui| {
                            for value in &row.12 {
                                ui.label(value);
                            }
                        });
                    }
                    ui.end_row();
                }
            });
    }
}

// Генерируем UI для фильтров (полноширинный layout с переносом строк)
fn filter_section_horizontal(
    ui: &mut Ui,
    title: &str,
    items: &[String],
    selected: &mut HashSet<String>,
    show_all: &mut bool,
) {
    ui.horizontal(|ui| {
        ui.label(format!("{}:", title));
        if ui.button("All").clicked() {
            *show_all = true;
            selected.extend(items.iter().cloned());
        }
        if ui.button("None").clicked() {
            *show_all = false;
            selected.clear();
        }
    });

    // Use wrapping layout for checkboxes
    ui.horizontal_wrapped(|ui| {
        for item in items {
            let mut checked = selected.contains(item);
            if ui.checkbox(&mut checked, item).changed() {
                if checked {
                    selected.insert(item.clone());
                } else {
                    selected.remove(item);
                }
            }
        }
    });
    ui.add_space(5.0);
}

// For top-lvel filtering
fn param_filter_section(
    ui: &mut Ui,
    title: &str,
    param_info: &HashMap<String, Vec<String>>,
    selected_params: &mut HashMap<String, HashSet<String>>,
) {
    if param_info.is_empty() {
        return;
    }

    ui.heading(title);
    ui.add_space(2.0);

    for (param_name, values) in param_info {
        // Get current selection, but don't create empty entry automatically
        let param_selected = selected_params.get(param_name).cloned().unwrap_or_default();

        // Compact inline layout: parameter name, All/None buttons, and checkboxes all in one wrapped section
        ui.horizontal_wrapped(|ui| {
            ui.label(format!("{}:", param_name));

            let mut new_selection = param_selected.clone();

            if ui.button("All").clicked() {
                new_selection.extend(values.iter().cloned());
            }
            if ui.button("None").clicked() {
                new_selection.clear();
            }

            // Add checkboxes inline with the parameter name and buttons
            for value in values {
                let mut checked = new_selection.contains(value);
                if ui.checkbox(&mut checked, value).changed() {
                    if checked {
                        new_selection.insert(value.clone());
                    } else {
                        new_selection.remove(value);
                    }
                }
            }

            // Only store the selection if it's not empty, otherwise remove the entry
            if new_selection.is_empty() {
                selected_params.remove(param_name);
            } else {
                selected_params.insert(param_name.clone(), new_selection);
            }
        });
        ui.add_space(1.0);
    }
    ui.add_space(2.0);
}

pub struct FilteredData {
    selected_filters: Filters,
    create_convergence_plot: CreateConvergencePlot,
    create_error_plot: CreateErrorPlot,
    create_performance_plot: CreatePerformancePlot,
    create_accel_records_table: CreateAccelRecordsTable,
}

impl FilteredData {
    pub fn filter_data_items<'a>(
        data_items: &'a [(SeriesRecord, Vec<AccelRecord>)],
        filters: &Filters,
    ) -> Vec<(&'a SeriesRecord, Vec<&'a AccelRecord>)> {
        // Early return if no filters
        if filters.precisions.is_empty()
            && filters.base_series.is_empty()
            && filters.base_accel.is_empty()
            && filters.m_values.is_empty()
            && filters.series_params.is_empty()
            && filters.accel_params.is_empty()
        {
            return data_items
                .iter()
                .map(|(series, accel_records)| (series, accel_records.iter().collect()))
                .collect();
        }
        data_items
            .iter()
            .filter(|(series, accel_records)| {
                // Series-level filtering
                let precision_match =
                    filters.precisions.is_empty() || filters.precisions.contains(&series.precision);

                let series_match =
                    filters.base_series.is_empty() || filters.base_series.contains(&series.name);

                let series_params_match = filters.series_params.is_empty()
                    || filters
                        .series_params
                        .iter()
                        .all(|(param_name, allowed_values)| {
                            series
                                .arguments
                                .get(param_name)
                                .map(|value| allowed_values.contains(value))
                                .unwrap_or(false)
                        });
                if !precision_match || !series_match || !series_params_match {
                    return false;
                }
                // Check if any acceleration records match
                accel_records.iter().any(|accel_record| {
                    let accel_match = filters.base_accel.is_empty()
                        || filters.base_accel.contains(&accel_record.accel_info.name);

                    let m_value_match = filters.m_values.is_empty()
                        || filters.m_values.contains(&accel_record.accel_info.m_value);

                    let accel_params_match = filters.accel_params.is_empty()
                        || filters
                            .accel_params
                            .iter()
                            .all(|(param_name, allowed_values)| {
                                accel_record
                                    .accel_info
                                    .additional_args
                                    .get(param_name)
                                    .map(|value| allowed_values.contains(value))
                                    .unwrap_or(false)
                            });
                    accel_match && m_value_match && accel_params_match
                })
            })
            .map(|(series, accel_records)| {
                // Filter acceleration records for the final result
                let filtered_accel_records: Vec<&'a AccelRecord> = accel_records
                    .iter()
                    .filter(|accel_record| {
                        let accel_match = filters.base_accel.is_empty()
                            || filters.base_accel.contains(&accel_record.accel_info.name);

                        let m_value_match = filters.m_values.is_empty()
                            || filters.m_values.contains(&accel_record.accel_info.m_value);

                        let accel_params_match = filters.accel_params.is_empty()
                            || filters
                                .accel_params
                                .iter()
                                .all(|(param_name, allowed_values)| {
                                    accel_record
                                        .accel_info
                                        .additional_args
                                        .get(param_name)
                                        .map(|value| allowed_values.contains(value))
                                        .unwrap_or(false)
                                });
                        accel_match && m_value_match && accel_params_match
                    })
                    .collect();
                (series, filtered_accel_records)
            })
            .collect()
    }

    // Dynamic filtering UI function
    #[must_use]
    fn dynamic_ui_filter_section(
        ui: &mut Ui,
        available_filters: &Filters,
        selected_filters: &mut Filters,
    ) -> bool {
        if available_filters.precisions.is_empty()
            && available_filters.base_series.is_empty()
            && available_filters.base_accel.is_empty()
            && available_filters.m_values.is_empty()
            && available_filters.series_params.is_empty()
            && available_filters.accel_params.is_empty()
        {
            return false;
        }
        let mut updated = false;
        ui.heading("Быстрые фильтры");
        ui.add_space(5.0);
        ui.horizontal_wrapped(|ui| {
            // Precision checkboxes
            for precision in &available_filters.precisions {
                let mut checked = selected_filters.precisions.contains(precision);
                if ui
                    .checkbox(&mut checked, format!("prec={precision}"))
                    .changed()
                {
                    if checked {
                        selected_filters.precisions.insert(precision.clone());
                    } else {
                        selected_filters.precisions.remove(precision);
                    }
                    updated = true;
                }
            }
            // Series checkboxes
            for series in &available_filters.base_series {
                let mut checked = selected_filters.base_series.contains(series);
                if ui
                    .checkbox(&mut checked, format!("series={series}"))
                    .changed()
                {
                    if checked {
                        selected_filters.base_series.insert(series.clone());
                    } else {
                        selected_filters.base_series.remove(series);
                    }
                    updated = true;
                }
            }
            // Acceleration checkboxes
            for accel in &available_filters.base_accel {
                let mut checked = selected_filters.base_accel.contains(accel);
                if ui
                    .checkbox(&mut checked, format!("accel={accel}"))
                    .changed()
                {
                    if checked {
                        selected_filters.base_accel.insert(accel.clone());
                    } else {
                        selected_filters.base_accel.remove(accel);
                    }
                    updated = true;
                }
            }
            // M values checkboxes
            for m in &available_filters.m_values {
                let mut checked = selected_filters.m_values.contains(m);
                if ui.checkbox(&mut checked, format!("m={}", m)).changed() {
                    if checked {
                        selected_filters.m_values.insert(*m);
                    } else {
                        selected_filters.m_values.remove(m);
                    }
                    updated = true;
                }
            }
            // Series parameters checkboxes
            for (param_name, values) in &available_filters.series_params {
                for value in values {
                    let param_selected = selected_filters
                        .series_params
                        .get(param_name)
                        .map(|set| set.contains(value))
                        .unwrap_or(false);
                    let mut checked = param_selected;
                    if ui
                        .checkbox(&mut checked, format!("{param_name}={value}"))
                        .changed()
                    {
                        if checked {
                            selected_filters
                                .series_params
                                .entry(param_name.clone())
                                .or_insert_with(HashSet::new)
                                .insert(value.clone());
                        } else {
                            if let Some(set) = selected_filters.series_params.get_mut(param_name) {
                                set.remove(value);
                                if set.is_empty() {
                                    selected_filters.series_params.remove(param_name);
                                }
                            }
                        }
                        updated = true;
                    }
                }
            }
            // Acceleration parameters checkboxes
            for (param_name, values) in &available_filters.accel_params {
                for value in values {
                    let param_selected = selected_filters
                        .accel_params
                        .get(param_name)
                        .map(|set| set.contains(value))
                        .unwrap_or(false);
                    let mut checked = param_selected;
                    if ui
                        .checkbox(&mut checked, format!("{param_name}={value}"))
                        .changed()
                    {
                        if checked {
                            selected_filters
                                .accel_params
                                .entry(param_name.clone())
                                .or_insert_with(HashSet::new)
                                .insert(value.clone());
                        } else {
                            if let Some(set) = selected_filters.accel_params.get_mut(param_name) {
                                set.remove(value);
                                if set.is_empty() {
                                    selected_filters.accel_params.remove(param_name);
                                }
                            }
                        }
                        updated = true;
                    }
                }
            }
        });
        ui.add_space(5.0);
        return updated;
    }

    pub fn new(data: &[SeriesData], selected_filters: Filters, symlog: bool) -> Self {
        let filtered = Self::filter_data_items(data, &selected_filters);
        Self {
            selected_filters,
            create_convergence_plot: create_convergence_plot(&filtered),
            create_error_plot: create_error_plot(&filtered, symlog),
            create_performance_plot: create_performance_plot(&filtered, symlog),
            create_accel_records_table: create_accel_records_table(&filtered),
        }
    }

    fn upd(&mut self, data: &Vec<SeriesData>, symlog: bool) {
        *self = Self::new(data, mem::take(&mut self.selected_filters), symlog);
    }

    /// Renders filtering ui & updates itself
    pub fn ui_filter(
        &mut self,
        ui: &mut Ui,
        data: &Vec<SeriesData>,
        available_filters: &Filters,
        symlog: bool,
    ) {
        if Self::dynamic_ui_filter_section(ui, available_filters, &mut self.selected_filters) {
            self.upd(data, symlog);
        }
    }
}

pub struct Data {
    data: Vec<SeriesData>,
    available_filters: Filters,
    filtered: FilteredData,
}

impl Data {
    fn new(data: Vec<SeriesData>, symlog: bool) -> Self {
        Self {
            available_filters: filterable(&data),
            filtered: FilteredData::new(&data, Filters::default(), symlog),
            data,
        }
    }
}

pub struct DashboardApp {
    loader: Arc<DataLoader>,
    filters: Filters,
    data: Option<Data>,
    // Каналы для асинхронной загрузки данных
    data_sender: Option<mpsc::Sender<Result<Vec<SeriesData>>>>,
    data_receiver: Option<mpsc::Receiver<Result<Vec<SeriesData>>>>,
    loading: bool,
    viz: Vis,
    symlog: bool,
}

impl DashboardApp {
    pub fn new(loader: Arc<DataLoader>) -> Self {
        let (tx, rx) =
            std::sync::mpsc::channel::<std::result::Result<Vec<SeriesData>, anyhow::Error>>();
        Self {
            loader,
            filters: Filters::default(),
            data: None,
            data_sender: Some(tx),
            data_receiver: Some(rx),
            loading: false,
            viz: Vis {
                show_partial_sums: true,
                show_limits: true,
                show_imaginary: true,
                force_show_imaginary: false,
                pending_screenshots: HashMap::new(),
                plot_hovered: false,
            },
            symlog: true,
        }
    }

    fn update_data(&mut self) {
        if let (Some(sender), _) = (&self.data_sender, &self.data_receiver) {
            let filters = self.filters.clone();
            let loader = self.loader.clone();
            let tx = sender.clone();

            // Запускаем загрузку в отдельном потоке
            std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let result: std::result::Result<Vec<SeriesData>, anyhow::Error> =
                    rt.block_on(loader.filter_data(&filters));
                let _ = tx.send(result);
            });

            self.loading = true;
        }
    }

    fn check_for_data(&mut self) {
        if let Some(receiver) = &self.data_receiver {
            if let Ok(result) = receiver.try_recv() {
                match result {
                    Ok(data) => {
                        let len = data.len();
                        self.data = Some(Data::new(data, self.symlog));
                        println!("Loaded {} series after filtering", len);
                    }
                    Err(e) => {
                        eprintln!("Error filtering data: {}", e);
                        self.data = None;
                    }
                }
                self.loading = false;
            }
        }
    }
}

impl eframe::App for DashboardApp {
    fn update(&mut self, ctx: &Context, _frame: &mut eframe::Frame) {
        // Проверяем наличие новых данных от фоновых потоков
        self.check_for_data();

        // Handle screenshot events
        if let Err(e) = self.viz.handle_screenshot_events(ctx) {
            eprintln!("Screenshot error: {}", e);
        }

        // Единая прокручиваемая область для всего контента
        egui::CentralPanel::default().show(ctx, |ui| {
            // Configure scroll area based on plot hover state
            let mut scroll_area = egui::ScrollArea::vertical();
            if self.viz.plot_hovered {
                // Disable scrolling when any plot is hovered
                scroll_area = scroll_area.enable_scrolling(false);
                self.viz.plot_hovered = false;
            }

            scroll_area.show(ui, |ui| {
                // Фильтры
                ui.heading("Фильтры");
                ui.add_space(5.0);

                // Точность
                ui.push_id("precision_filters", |ui| {
                    let mut show_all =
                        self.filters.precisions.len() == self.loader.metadata.precisions.len();
                    filter_section_horizontal(
                        ui,
                        "Точность",
                        &self.loader.metadata.precisions,
                        &mut self.filters.precisions,
                        &mut show_all,
                    );
                });

                // Базовые ряды
                ui.push_id("series_filters", |ui| {
                    let mut show_all =
                        self.filters.base_series.len() == self.loader.metadata.series_names.len();
                    filter_section_horizontal(
                        ui,
                        "Базовые ряды",
                        &self.loader.metadata.series_names,
                        &mut self.filters.base_series,
                        &mut show_all,
                    );
                });

                // Параметры рядов (перемещено сюда)
                ui.push_id("series_params_filters", |ui| {
                    param_filter_section(
                        ui,
                        "Параметры рядов",
                        &self.loader.metadata.series_param_info,
                        &mut self.filters.series_params,
                    );
                });

                // Базовые методы ускорения
                ui.push_id("accel_filters", |ui| {
                    let mut show_all =
                        self.filters.base_accel.len() == self.loader.metadata.accel_names.len();
                    filter_section_horizontal(
                        ui,
                        "Базовые методы ускорения",
                        &self.loader.metadata.accel_names,
                        &mut self.filters.base_accel,
                        &mut show_all,
                    );
                });

                // m_values
                ui.push_id("m_values_filters", |ui| {
                    ui.horizontal(|ui| {
                        ui.label("Значения m:");
                        if ui.button("All").clicked() {
                            self.filters.m_values.extend(&self.loader.metadata.m_values);
                        }
                        if ui.button("None").clicked() {
                            self.filters.m_values.clear();
                        }
                    });

                    // Use wrapping layout for m_values checkboxes
                    ui.horizontal_wrapped(|ui| {
                        for m in &self.loader.metadata.m_values {
                            let mut checked = self.filters.m_values.contains(m);
                            if ui.checkbox(&mut checked, format!("m={}", m)).changed() {
                                if checked {
                                    self.filters.m_values.insert(*m);
                                } else {
                                    self.filters.m_values.remove(m);
                                }
                            }
                        }
                    });
                });

                // Параметры ускорения
                ui.push_id("accel_params_filters", |ui| {
                    param_filter_section(
                        ui,
                        "Параметры ускорения",
                        &self.loader.metadata.accel_param_info,
                        &mut self.filters.accel_params,
                    );
                });

                ui.separator();

                // Plot options
                ui.horizontal(|ui| {
                    ui.label("Опции графиков:");
                });
                ui.horizontal_wrapped(|ui| {
                    ui.label("Опции графиков:");
                    if ui.checkbox(&mut self.symlog, "Symlog").changed() {
                        if let Some(x) = &mut self.data {
                            x.filtered.upd(&x.data, self.symlog);
                        }
                    }
                    ui.checkbox(&mut self.viz.show_partial_sums, "Частичные суммы");
                    ui.checkbox(&mut self.viz.show_limits, "Пределы");
                    ui.checkbox(&mut self.viz.show_imaginary, "Мнимые части");
                    if self.viz.show_imaginary {
                        ui.checkbox(
                            &mut self.viz.force_show_imaginary,
                            "ВСЕГДА показывать мнимую часть",
                        );
                    }
                });

                ui.separator();

                // Кнопка Обновить и счетчик данных
                ui.horizontal(|ui| {
                    if self.loading {
                        ui.spinner();
                        ui.label("Загрузка...");
                    } else {
                        if ui.button("🔄 Обновить графики").clicked() {
                            self.update_data();
                        }
                    }
                    if let Some(data) = &self.data {
                        ui.label(format!("Загружено рядов: {}", data.data.len()));
                    }
                });

                ui.add_space(20.0);

                // Графики
                if let Some(data) = &mut self.data {
                    data.filtered
                        .ui_filter(ui, &data.data, &data.available_filters, self.symlog);

                    ui.separator();

                    // Convergence plot
                    ui.collapsing("Сходимость методов", |ui| {
                        let f = &data.filtered.create_convergence_plot;
                        f(&mut self.viz, ui);
                    });

                    // Error plot
                    ui.collapsing("Ошибка сходимости", |ui| {
                        let f = &data.filtered.create_error_plot;
                        f(&mut self.viz, ui);
                    });

                    // Performance plot
                    ui.collapsing("Производительность методов", |ui| {
                        let f = &data.filtered.create_performance_plot;
                        f(&mut self.viz, ui);
                    });

                    // AccelRecords table
                    ui.collapsing("Таблица ускорений", |ui| {
                        let f = &data.filtered.create_accel_records_table;
                        f(ui);
                    });
                } else if self.loading {
                    ui.centered_and_justified(|ui| {
                        ui.add_space(50.0);
                        ui.spinner();
                        ui.add_space(20.0);
                        ui.heading("Загрузка данных...");
                        ui.label("Пожалуйста, подождите пока фильтры применяются к данным");
                    });
                } else {
                    ui.centered_and_justified(|ui| {
                        ui.heading("Выберите фильтры и нажмите Обновить");
                    });
                }
            });
        });
    }
}
