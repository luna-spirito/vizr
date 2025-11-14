use eframe::egui;
use egui_plot::{Line, Plot, PlotPoints};
use crate::data_loader::{DataLoader, Metadata};
use crate::filters::Filters;

pub struct DashboardApp {
    loader: DataLoader,
    filters: Filters,
    data: Option<polars::prelude::DataFrame>,
    metadata: Metadata,
    // UI состояние
    show_precision: bool,
    show_series: bool,
    show_accel: bool,
}

impl DashboardApp {
    pub fn new(loader: DataLoader, metadata: Metadata) -> Self {
        Self {
            loader,
            filters: Filters::default(),
            data: None,
            metadata,
            show_precision: true,
            show_series: true,
            show_accel: true,
        }
    }

    fn update_data(&mut self) {
        if let Ok(df) = self.loader.filter_data(&self.filters) {
            self.data = Some(df);
        }
    }
}

// Генерируем UI для фильтров
fn filter_section(
    ui: &mut egui::Ui,
    title: &str,
    items: &[String],
    selected: &mut std::collections::HashSet<String>,
    show_all: &mut bool,
) {
    ui.horizontal(|ui| {
        ui.heading(title);
        if ui.button("All").clicked() {
            *show_all = true;
            selected.extend(items.iter().cloned());
        }
        if ui.button("None").clicked() {
            *show_all = false;
            selected.clear();
        }
    });

    ui.group(|ui| {
        ui.style_mut().wrap = Some(true);
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
    ui.add_space(10.0);
}

impl eframe::App for DashboardApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Левое меню с фильтрами (как в вашем HTML)
        egui::SidePanel::left("filters").show(ctx, |ui| {
            ui.heading("Фильтры");

            // Точность
            filter_section(
                ui,
                "Точность",
                &self.metadata.precisions,
                &mut self.filters.precisions,
                &mut self.show_precision,
            );

            // Базовые ряды
            filter_section(
                ui,
                "Базовые ряды",
                &self.metadata.series_names,
                &mut self.filters.base_series,
                &mut self.show_series,
            );

            // Базовые методы ускорения
            filter_section(
                ui,
                "Базовые методы ускорения",
                &self.metadata.accel_names,
                &mut self.filters.base_accel,
                &mut self.show_accel,
            );

            // m_values
            ui.horizontal(|ui| {
                ui.heading("Значения m");
                if ui.button("All").clicked() {
                    self.filters.m_values.extend(&self.metadata.m_values);
                }
                if ui.button("None").clicked() {
                    self.filters.m_values.clear();
                }
            });
            ui.group(|ui| {
                for m in &self.metadata.m_values {
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

            // Кнопка Обновить
            if ui.button("🔄 Обновить графики").clicked() {
                self.update_data();
            }
        });

        // Центральная область с графиками
        egui::CentralPanel::default().show(ctx, |ui| {
            if let Some(ref df) = self.data {
                ui.heading("Сходимость методов");
                
                // Создаём PlotPoints из данных
                if let Ok(_computed_col) = df.column("computed") {
                    if let Ok(_accel_col) = df.column("accel") {
                        let plot_points: PlotPoints = (0..df.height())
                            .map(|i| {
                                // Упрощенный пример - нужно адаптировать под реальные данные
                                let n = i as f64;
                                let value = 1.0; // Заглушка, нужно получить реальные данные
                                [n, value]
                            })
                            .collect();

                        Plot::new("convergence")
                            .allow_zoom(true)
                            .allow_drag(true)
                            .height(300.0)
                            .show(ui, |plot_ui| {
                                plot_ui.line(Line::new(plot_points));
                            });

                        // Второй график ошибки
                        ui.separator();
                        ui.heading("Ошибка сходимости");
                        
                        let error_points: PlotPoints = (0..df.height())
                            .map(|i| {
                                let n = i as f64;
                                let error = (i as f64).ln(); // Заглушка
                                [n, error]
                            })
                            .collect();

                        Plot::new("error")
                            .allow_zoom(true)
                            .height(300.0)
                            .show(ui, |plot_ui| {
                                plot_ui.line(Line::new(error_points).color(egui::Color32::RED));
                            });
                    }
                }
            } else {
                ui.centered_and_justified(|ui| {
                    ui.heading("Выберите фильтры и нажмите Обновить");
                });
            }
        });
    }
}