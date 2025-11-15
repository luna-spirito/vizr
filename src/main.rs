mod app;
mod data_loader;
mod filters;

use clap::Parser;
use std::path::PathBuf;

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
    let metadata = loader.metadata.clone();
    println!(
        "Found {} precisions, {} series, {} accelerators",
        metadata.precisions.len(),
        metadata.series_names.len(),
        metadata.accel_names.len()
    );

    // Запускаем GUI
    let options = eframe::NativeOptions::default();

    eframe::run_native(
        "Vizr - Parquet Data Visualizer",
        options,
        Box::new(|_cc| Box::new(app::DashboardApp::new(loader, metadata)) as Box<dyn eframe::App>),
    )
    .map_err(|e| anyhow::anyhow!("GUI error: {}", e))?;
    Ok(())
}
