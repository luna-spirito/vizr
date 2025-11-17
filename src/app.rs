use crate::data_loader::{DataItem, DataLoader, Filters};
use anyhow::Result;
use eframe::egui;
use egui_plot::{Line, MarkerShape, Plot, PlotPoints, Points};
use std::sync::{Arc, mpsc};

pub struct DashboardApp {
    loader: Arc<DataLoader>,
    filters: Filters,
    data: Option<Vec<DataItem>>,
    // UI состояние
    show_precision: bool,
    show_series: bool,
    show_accel: bool,
    // Plot visibility toggles
    show_convergence: bool,
    show_error: bool,
    show_performance: bool,
    // Plot options
    show_partial_sums: bool,
    show_limits: bool,
    show_imaginary: bool,
    // Каналы для асинхронной загрузки данных
    data_sender: Option<mpsc::Sender<Result<Vec<DataItem>>>>,
    data_receiver: Option<mpsc::Receiver<Result<Vec<DataItem>>>>,
    loading: bool,
}

impl DashboardApp {
    pub fn new(loader: Arc<DataLoader>) -> Self {
        let (tx, rx) =
            std::sync::mpsc::channel::<std::result::Result<Vec<DataItem>, anyhow::Error>>();
        Self {
            loader,
            filters: Filters::default(),
            data: None,
            show_precision: true,
            show_series: true,
            show_accel: true,
            show_convergence: true,
            show_error: true,
            show_performance: true,
            show_partial_sums: true,
            show_limits: true,
            show_imaginary: true,
            data_sender: Some(tx),
            data_receiver: Some(rx),
            loading: false,
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
                let result: std::result::Result<Vec<DataItem>, anyhow::Error> =
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
                        self.data = Some(data);
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

    fn format_item_name(
        &self,
        series: &crate::data_loader::SeriesRecord,
        accel: &crate::data_loader::AccelInfo,
    ) -> String {
        let mut name = format!("{} (m={}) ", accel.name, accel.m_value);

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

    fn create_convergence_plot(&self, ui: &mut egui::Ui) {
        ui.heading("Сходимость методов");

        if let Some(ref data) = self.data {
            if data.is_empty() {
                ui.label("Нет данных для отображения");
                return;
            }

            let mut lines = Vec::new();
            let mut series_names = std::collections::HashSet::new();
            let mut limit_lines = Vec::new();

            for (series, accel_records) in data {
                if series.computed.is_empty() {
                    continue;
                }

                // Partial sums (one per series)
                if self.show_partial_sums && !series_names.contains(&series.name) {
                    series_names.insert(series.name.clone());

                    let has_complex = series.computed.iter().any(|c| c.value.imag.abs() > 1e-15);

                    let partial_points: PlotPoints = series
                        .computed
                        .iter()
                        .map(|c| [c.n as f64, c.value.real])
                        .collect();

                    lines.push(
                        Line::new(partial_points)
                            .name(format!("{} (частичные суммы)", series.name))
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
                                .name(format!("{} (частичные суммы, мнимая часть)", series.name))
                                .color(egui::Color32::from_rgb(255, 192, 203)),
                        );
                    }
                }

                // Limit line (one per series)
                if self.show_limits && !series_names.contains(&series.name) {
                    let limit = &series.series_limit;
                    let x_range: Vec<f64> = series.computed.iter().map(|c| c.n as f64).collect();
                    if !x_range.is_empty() {
                        let min_x = x_range.iter().fold(f64::INFINITY, |a, &b| a.min(b));
                        let max_x = x_range.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
                        let limit_points =
                            PlotPoints::new(vec![[min_x, limit.real], [max_x, limit.real]]);
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
                        .any(|cn| cn.map_or(false, |c| c.imag.abs() > 1e-15));

                    // Main convergence line - zip series computed with accel computed
                    let points: PlotPoints = series
                        .computed
                        .iter()
                        .zip(accel_record.computed.iter())
                        .filter_map(|(c, accel)| accel.map(|a| [c.n as f64, a.real]))
                        .collect();

                    lines.push(Line::new(points).name(item_name.clone()));

                    // Imaginary part if present and enabled
                    if has_complex && self.show_imaginary {
                        let imag_points: PlotPoints = series
                            .computed
                            .iter()
                            .zip(accel_record.computed.iter())
                            .filter_map(|(c, accel)| accel.map(|a| [c.n as f64, a.imag]))
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
                lines.push(
                    Line::new(points)
                        .name(format!("{} (предел)", series_name))
                        .color(egui::Color32::from_rgb(255, 0, 0))
                        .stroke(egui::Stroke::new(3.0, egui::Color32::from_rgb(255, 0, 0))),
                );
            }

            Plot::new("convergence")
                .allow_zoom(true)
                .allow_drag(true)
                .height(600.0)
                .x_axis_label("Итерация n")
                .y_axis_label("Значение")
                .show(ui, |plot_ui| {
                    for line in lines {
                        plot_ui.line(line);
                    }
                });
        } else {
            ui.label("Нет данных для отображения");
        }
    }

    fn create_error_plot(&self, ui: &mut egui::Ui) {
        ui.heading("Ошибка сходимости");

        if let Some(ref data) = self.data {
            if data.is_empty() {
                ui.label("Нет данных для отображения");
                return;
            }

            let mut lines = Vec::new();

            for (series, accel_records) in data {
                if series.computed.is_empty() {
                    continue;
                }

                for accel_record in accel_records {
                    if accel_record.computed.is_empty() {
                        continue;
                    }

                    let item_name = self.format_item_name(series, &accel_record.accel_info);

                    // Calculate error as difference between accel value and series limit
                    let points: PlotPoints = series
                        .computed
                        .iter()
                        .zip(accel_record.computed.iter())
                        .filter_map(|(c, accel)| {
                            accel.map(|a| {
                                let error = (a.real - series.series_limit.real).abs()
                                    + (a.imag - series.series_limit.imag).abs();
                                [c.n as f64, error.ln()] // Log scale
                            })
                        })
                        .collect();

                    lines.push(Line::new(points).name(item_name));
                }
            }

            Plot::new("error")
                .allow_zoom(true)
                .allow_drag(true)
                .height(400.0)
                .x_axis_label("Итерация n")
                .y_axis_label("Абсолютная ошибка (log)")
                .show(ui, |plot_ui| {
                    for line in lines {
                        plot_ui.line(line);
                    }
                });
        } else {
            ui.label("Нет данных для отображения");
        }
    }

    fn create_performance_plot(&self, ui: &mut egui::Ui) {
        ui.heading("Производительность методов");

        if let Some(ref data) = self.data {
            if data.is_empty() {
                ui.label("Нет данных для отображения");
                return;
            }

            let mut point_series = Vec::new();

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
                        if let Some(a) = accel {
                            let error = (a.real - series.series_limit.real).abs()
                                + (a.imag - series.series_limit.imag).abs();

                            if error < min_error {
                                min_error = error;
                                min_error_iter = c.n;
                            }
                        }
                    }

                    if min_error < f64::INFINITY {
                        let point = PlotPoints::new(vec![[min_error_iter as f64, min_error.ln()]]);
                        point_series.push((item_name, point));
                    }
                }
            }

            Plot::new("performance")
                .allow_zoom(true)
                .allow_drag(true)
                .height(400.0)
                .x_axis_label("Итерация достижения минимальной ошибки")
                .y_axis_label("Минимальная ошибка (log)")
                .show(ui, |plot_ui| {
                    for (name, points) in point_series {
                        plot_ui.points(
                            Points::new(points)
                                .name(name)
                                .shape(MarkerShape::Circle)
                                .radius(4.0),
                        );
                    }
                });
        } else {
            ui.label("Нет данных для отображения");
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

impl eframe::App for DashboardApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Проверяем наличие новых данных от фоновых потоков
        self.check_for_data();

        // Верхняя панель с фильтрами
        egui::TopBottomPanel::top("filters").show(ctx, |ui| {
            ui.heading("Фильтры");
            ui.add_space(5.0);

            // Точность
            ui.push_id("precision_filters", |ui| {
                filter_section_horizontal(
                    ui,
                    "Точность",
                    &self.loader.metadata.precisions,
                    &mut self.filters.precisions,
                    &mut self.show_precision,
                );
            });

            // Базовые ряды
            ui.push_id("series_filters", |ui| {
                filter_section_horizontal(
                    ui,
                    "Базовые ряды",
                    &self.loader.metadata.series_names,
                    &mut self.filters.base_series,
                    &mut self.show_series,
                );
            });

            // Базовые методы ускорения
            ui.push_id("accel_filters", |ui| {
                filter_section_horizontal(
                    ui,
                    "Базовые методы ускорения",
                    &self.loader.metadata.accel_names,
                    &mut self.filters.base_accel,
                    &mut self.show_accel,
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

            ui.separator();

            // Plot options
            ui.horizontal(|ui| {
                ui.label("Опции графиков:");
            });
            ui.horizontal_wrapped(|ui| {
                ui.checkbox(&mut self.show_convergence, "Сходимость");
                ui.checkbox(&mut self.show_error, "Ошибка");
                ui.checkbox(&mut self.show_performance, "Производительность");
                ui.separator();
                ui.checkbox(&mut self.show_partial_sums, "Частичные суммы");
                ui.checkbox(&mut self.show_limits, "Пределы");
                ui.checkbox(&mut self.show_imaginary, "Мнимые части");
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
                if let Some(ref data) = self.data {
                    ui.label(format!("Загружено записей: {}", data.len()));
                }
            });
        });

        // Центральная область с графиками
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.push_id("main_plots_scroll", |ui| {
                egui::ScrollArea::vertical().show(ui, |ui| {
                    if self.data.is_some() {
                        // Convergence plot
                        if self.show_convergence {
                            ui.push_id("convergence_plot_wrapper", |ui| {
                                self.create_convergence_plot(ui);
                            });
                            ui.separator();
                        }

                        // Error plot
                        if self.show_error {
                            ui.push_id("error_plot_wrapper", |ui| {
                                self.create_error_plot(ui);
                            });
                            ui.separator();
                        }

                        // Performance plot
                        if self.show_performance {
                            ui.push_id("performance_plot_wrapper", |ui| {
                                self.create_performance_plot(ui);
                            });
                            ui.separator();
                        }
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
        });
    }
}
