mod app;
mod data_loader;

use clap::Parser;
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "vizr")]
#[command(about = "A high-performance parquet data visualizer")]
struct Args {
    /// Path to the directory containing parquet files
    data_dir: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = Args::parse();

    println!("Loading parquet data from: {}", args.data_dir);
    let loader = data_loader::DataLoader::new(&args.data_dir).await?;
    println!(
        "Found {} precisions, {} series, {} accelerators",
        loader.metadata.precisions.len(),
        loader.metadata.series_names.len(),
        loader.metadata.accel_names.len()
    );

    // Запускаем GUI
    let options = eframe::NativeOptions::default();

    eframe::run_native(
        "Vizr - Parquet Data Visualizer",
        options,
        Box::new(|_cc| Box::new(app::DashboardApp::new(Arc::new(loader))) as Box<dyn eframe::App>),
    )
    .map_err(|e| anyhow::anyhow!("GUI error: {}", e))?;
    Ok(())
}
