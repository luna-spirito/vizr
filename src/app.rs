use crate::data_loader::{AccelRecord, DataItem, DataLoader, Filters, SeriesRecord};
use crate::symlog::symlog_formatter;
use anyhow::Result;
use eframe::egui;
use egui_plot::{Line, MarkerShape, Plot, PlotPoints, Points};
use std::collections::HashMap;
use std::sync::{Arc, mpsc};

// TODO: Current `symlog` flag implementation is absolutely awful. To be fixed.

type DataItemRef<'a> = (&'a SeriesRecord, Vec<&'a AccelRecord>);

fn filterable(entries: &[DataItem]) -> Filters {
    let mut precisions = std::collections::HashSet::new();
    let mut base_series = std::collections::HashSet::new();
    let mut base_accel = std::collections::HashSet::new();
    let mut m_values = std::collections::HashSet::new();
    let mut accel_params = std::collections::HashMap::new();
    let mut series_params = std::collections::HashMap::new();

    for (series, accel_records) in entries.iter() {
        // Collect series-level fields
        precisions.insert(series.precision.clone());
        base_series.insert(series.name.clone());

        // Collect series parameters
        for (key, value) in &series.arguments {
            series_params
                .entry(key.clone())
                .or_insert_with(std::collections::HashSet::new)
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
                    .or_insert_with(std::collections::HashSet::new)
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

pub struct Viz {
    // Plot options
    show_partial_sums: bool,
    show_limits: bool,
    show_imaginary: bool,

    // Screenshot functionality
    pending_screenshots: HashMap<&'static str, egui::Rect>,

    // Plot hover state for scroll control
    plot_hovered: bool,
}

impl Viz {
    fn format_series_name_with_args(&self, series: &crate::data_loader::SeriesRecord) -> String {
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

    fn format_item_name(
        &self,
        series: &crate::data_loader::SeriesRecord,
        accel: &crate::data_loader::AccelInfo,
    ) -> String {
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

    fn request_screenshot(
        &mut self,
        ctx: &egui::Context,
        plot_id: &'static str,
        plot_rect: egui::Rect,
    ) {
        self.pending_screenshots.insert(plot_id, plot_rect);
        // Try without parameters first
        ctx.send_viewport_cmd(egui::ViewportCommand::Screenshot);
    }

    fn handle_screenshot_events(&mut self, ctx: &egui::Context) -> Result<()> {
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
        ctx: &egui::Context,
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

    fn create_convergence_plot(&mut self, ui: &mut egui::Ui, data: &[DataItemRef]) {
        if data.is_empty() {
            ui.label("Нет данных для отображения");
            return;
        }

        let mut lines = Vec::new();
        let mut partial_sum_series = std::collections::HashSet::new();
        let mut limit_series = std::collections::HashSet::new();
        let mut limit_lines = Vec::new();

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
            if self.show_partial_sums && !partial_sum_series.contains(&series.name) {
                partial_sum_series.insert(series.name.clone());

                let has_complex = series.computed.iter().any(|c| c.value.imag.abs() > 1e-15);

                let partial_points: PlotPoints = series
                    .computed
                    .iter()
                    .map(|c| [c.n as f64, c.value.real])
                    .collect();

                lines.push(
                    Line::new(partial_points)
                        .name(format!(
                            "{} (частичные суммы)",
                            self.format_series_name_with_args(series)
                        ))
                        .color(egui::Color32::from_rgb(128, 128, 128)),
                );

                // Imaginary partial sums
                if has_complex && self.show_imaginary {
                    let imag_partial_points: PlotPoints = series
                        .computed
                        .iter()
                        .map(|c| [c.n as f64, c.value.imag])
                        .collect();

                    lines.push(
                        Line::new(imag_partial_points)
                            .name(format!(
                                "{} (частичные суммы, мнимая часть)",
                                self.format_series_name_with_args(series)
                            ))
                            .color(egui::Color32::from_rgb(255, 192, 203)),
                    );
                }
            }

            // Limit line (one per series)
            if self.show_limits && !limit_series.contains(&series.name) {
                let limit = &series.series_limit;
                let x_range: Vec<f64> = series.computed.iter().map(|c| c.n as f64).collect();
                if !x_range.is_empty() {
                    let min_x = x_range.iter().fold(f64::INFINITY, |a, &b| a.min(b));
                    let max_x = x_range.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
                    let limit_points =
                        PlotPoints::new(vec![[min_x, limit.real], [max_x, limit.real]]);
                    limit_series.insert(series.name.clone());
                    limit_lines.push((series.name.clone(), limit_points));
                }
            }

            // Process each acceleration record
            for accel_record in accel_records {
                if accel_record.computed.is_empty() {
                    continue;
                }

                let item_name = self.format_item_name(series, &accel_record.accel_info);
                let has_complex = accel_record
                    .computed
                    .iter()
                    .any(|cn| cn.map_or(false, |ap| ap.value.imag.abs() > 1e-15));

                // Main convergence line - zip series computed with accel computed
                let points: PlotPoints = series
                    .computed
                    .iter()
                    .zip(accel_record.computed.iter())
                    .filter_map(|(c, accel)| accel.map(|ap| [c.n as f64, ap.value.real]))
                    .collect();

                lines.push(Line::new(points).name(item_name.clone()));

                // Imaginary part if present and enabled
                if has_complex && self.show_imaginary {
                    let imag_points: PlotPoints = series
                        .computed
                        .iter()
                        .zip(accel_record.computed.iter())
                        .filter_map(|(c, accel)| accel.map(|ap| [c.n as f64, ap.value.imag]))
                        .collect();

                    lines.push(
                        Line::new(imag_points)
                            .name(format!("{} (мнимая часть)", item_name))
                            .color(egui::Color32::from_rgb(255, 165, 0)),
                    );
                }
            }
        }

        // Add limit lines
        for (series_name, points) in limit_lines {
            // Find the series record to get arguments
            if let Some((series, _)) = data.iter().find(|(s, _)| s.name == series_name) {
                lines.push(
                    Line::new(points)
                        .name(format!(
                            "{} (предел)",
                            self.format_series_name_with_args(series)
                        ))
                        .color(egui::Color32::from_rgb(255, 0, 0))
                        .stroke(egui::Stroke::new(3.0, egui::Color32::from_rgb(255, 0, 0))),
                );
            }
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
            for line in lines {
                plot_ui.line(line);
            }
        });
        self.plot_hovered |= plot.response.hovered();
        ui.horizontal(|ui| {
            if ui.button("📸 Снимок экрана").clicked() {
                self.request_screenshot(ui.ctx(), "convergence", plot.response.rect);
            }
        });
    }

    fn create_error_plot(&mut self, ui: &mut egui::Ui, data: &[DataItemRef], symlog: bool) {
        if data.is_empty() {
            ui.label("Нет данных для отображения");
            return;
        }

        let mut lines = Vec::new();

        for (series, accel_records) in data.iter() {
            if series.computed.is_empty() {
                continue;
            }

            for accel_record in accel_records.iter() {
                if accel_record.computed.is_empty() {
                    continue;
                }

                let item_name = self.format_item_name(series, &accel_record.accel_info);

                // Use Euclidean metric with machine epsilon for log scale, clamp to -1000
                let points: PlotPoints = series
                    .computed
                    .iter()
                    .zip(accel_record.computed.iter())
                    .filter_map(|(c, accel)| Some([c.n as f64, accel.as_ref()?.deviation]))
                    .collect();

                lines.push(Line::new(points).name(item_name));
            }
        }

        let mut plot = Plot::new("error")
            .allow_zoom(true)
            .allow_drag(true)
            .height(900.0)
            .x_axis_label("Итерация n")
            .y_axis_label("Абсолютная ошибка (log)")
            .legend(egui_plot::Legend::default());
        if symlog {
            plot = plot.y_axis_formatter(|mark, _, _| symlog_formatter(mark.value));
        }
        let plot = plot.show(ui, |plot_ui| {
            for line in lines {
                plot_ui.line(line);
            }
        });
        self.plot_hovered |= plot.response.hovered();
        ui.horizontal(|ui| {
            if ui.button("📸 Снимок экрана").clicked() {
                self.request_screenshot(ui.ctx(), "error", plot.response.rect);
            }
        });
    }

    fn create_performance_plot(&mut self, ui: &mut egui::Ui, data: &[DataItemRef], symlog: bool) {
        if data.is_empty() {
            ui.label("Нет данных для отображения");
            return;
        }

        let mut point_series = Vec::new();
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

                let item_name = self.format_item_name(series, &accel_record.accel_info);

                // Find minimum error and corresponding iteration
                let mut min_error = f64::INFINITY;
                let mut min_error_iter = 0;

                for (c, accel) in series.computed.iter().zip(accel_record.computed.iter()) {
                    if let Some(ap) = accel {
                        let error = ap.deviation;

                        if error < min_error {
                            min_error = error;
                            min_error_iter = c.n;
                        }
                    }
                }

                if min_error < f64::INFINITY {
                    let clamped_error = min_error.max(-1000.0); // Clamp to -1000
                    min_x = min_x.min(min_error_iter as f64);
                    max_x = max_x.max(min_error_iter as f64);
                    let point = PlotPoints::new(vec![[min_error_iter as f64, clamped_error]]);
                    point_series.push((item_name, point));
                }
            }
        }

        let mut plot = Plot::new("performance")
            .allow_zoom(true)
            .allow_drag(true)
            .height(900.0)
            .x_axis_label("Итерация достижения минимальной ошибки")
            .y_axis_label("Минимальная ошибка")
            .legend(egui_plot::Legend::default());
        if symlog {
            plot = plot.y_axis_formatter(|mark, _, _| symlog_formatter(mark.value));
        }
        let plot = plot.show(ui, |plot_ui| {
            for (name, points) in point_series {
                plot_ui.points(
                    Points::new(points)
                        .name(name)
                        .shape(MarkerShape::Circle)
                        .radius(4.0),
                );
            }
        });
        self.plot_hovered |= plot.response.hovered();
        ui.horizontal(|ui| {
            if ui.button("📸 Снимок экрана").clicked() {
                self.request_screenshot(ui.ctx(), "performance", plot.response.rect);
            }
        });
    }
}

pub struct DashboardApp {
    loader: Arc<DataLoader>,
    filters: Filters,
    data: Option<((Vec<DataItem>, bool), Filters, Filters)>,
    // Каналы для асинхронной загрузки данных
    data_sender: Option<mpsc::Sender<(Result<Vec<DataItem>>, bool)>>,
    data_receiver: Option<mpsc::Receiver<(Result<Vec<DataItem>>, bool)>>,
    symlog: bool,
    loading: bool,
    viz: Viz,
}

impl DashboardApp {
    pub fn new(loader: Arc<DataLoader>) -> Self {
        let (tx, rx) =
            std::sync::mpsc::channel::<(std::result::Result<Vec<DataItem>, anyhow::Error>, bool)>();
        Self {
            loader,
            filters: Filters::default(),
            data: None,
            data_sender: Some(tx),
            data_receiver: Some(rx),
            symlog: true,
            loading: false,
            viz: Viz {
                show_partial_sums: true,
                show_limits: true,
                show_imaginary: true,
                pending_screenshots: HashMap::new(),
                plot_hovered: false,
            },
        }
    }

    fn update_data(&mut self) {
        if let (Some(sender), _) = (&self.data_sender, &self.data_receiver) {
            let filters = self.filters.clone();
            let loader = self.loader.clone();
            let symlog = self.symlog;
            let tx = sender.clone();

            // Запускаем загрузку в отдельном потоке
            std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let result: std::result::Result<Vec<DataItem>, anyhow::Error> =
                    rt.block_on(loader.filter_data(&filters, symlog));
                let _ = tx.send((result, symlog));
            });

            self.loading = true;
        }
    }

    fn check_for_data(&mut self) {
        if let Some(receiver) = &self.data_receiver {
            if let Ok((result, symlog)) = receiver.try_recv() {
                match result {
                    Ok(data) => {
                        let len = data.len();
                        let dynamic_filters = filterable(&data);
                        self.data = Some(((data, symlog), dynamic_filters, Filters::default()));
                        println!("Loaded {} items after filtering", len);
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

// Генерируем UI для фильтров (полноширинный layout с переносом строк)
fn filter_section_horizontal(
    ui: &mut egui::Ui,
    title: &str,
    items: &[String],
    selected: &mut std::collections::HashSet<String>,
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

// Generate UI for parameter-based filtering
fn param_filter_section(
    ui: &mut egui::Ui,
    title: &str,
    param_info: &std::collections::HashMap<String, Vec<String>>,
    selected_params: &mut std::collections::HashMap<String, std::collections::HashSet<String>>,
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

// Dynamic filtering UI function
fn dynamic_ui_filter_section(
    ui: &mut egui::Ui,
    available_filters: &Filters,
    selected_filters: &mut Filters,
) {
    if available_filters.precisions.is_empty()
        && available_filters.base_series.is_empty()
        && available_filters.base_accel.is_empty()
        && available_filters.m_values.is_empty()
        && available_filters.series_params.is_empty()
        && available_filters.accel_params.is_empty()
    {
        return;
    }
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
                            .or_insert_with(std::collections::HashSet::new)
                            .insert(value.clone());
                    } else {
                        if let Some(set) = selected_filters.series_params.get_mut(param_name) {
                            set.remove(value);
                            if set.is_empty() {
                                selected_filters.series_params.remove(param_name);
                            }
                        }
                    }
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
                            .or_insert_with(std::collections::HashSet::new)
                            .insert(value.clone());
                    } else {
                        if let Some(set) = selected_filters.accel_params.get_mut(param_name) {
                            set.remove(value);
                            if set.is_empty() {
                                selected_filters.accel_params.remove(param_name);
                            }
                        }
                    }
                }
            }
        }
    });
    ui.add_space(5.0);
}

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

impl eframe::App for DashboardApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
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
                ui.checkbox(&mut self.symlog, "Symlog");

                ui.separator();

                // Plot options
                ui.horizontal(|ui| {
                    ui.label("Опции графиков:");
                });
                ui.horizontal_wrapped(|ui| {
                    ui.label("Опции графиков:");
                    ui.checkbox(&mut self.viz.show_partial_sums, "Частичные суммы");
                    ui.checkbox(&mut self.viz.show_limits, "Пределы");
                    ui.checkbox(&mut self.viz.show_imaginary, "Мнимые части");
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
                    if let Some((ref data, _, _)) = self.data {
                        ui.label(format!("Загружено рядов: {}", data.0.len()));
                    }
                });

                ui.add_space(20.0);

                // Графики
                if let Some(((data, symlog), available_dynamic_filters, selected_dynamic_filters)) =
                    &mut self.data
                {
                    // Dynamic filters section
                    dynamic_ui_filter_section(
                        ui,
                        available_dynamic_filters,
                        selected_dynamic_filters,
                    );
                    let data = filter_data_items(data, selected_dynamic_filters);

                    ui.separator();

                    // Convergence plot
                    ui.collapsing("Сходимость методов", |ui| {
                        self.viz.create_convergence_plot(ui, &data);
                    });

                    // Error plot
                    ui.collapsing("Ошибка сходимости", |ui| {
                        self.viz.create_error_plot(ui, &data, *symlog);
                    });

                    // Performance plot
                    ui.collapsing("Производительность методов", |ui| {
                        self.viz.create_performance_plot(ui, &data, *symlog);
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
