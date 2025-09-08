mod db;
mod ipfs;
mod model;
mod service;
mod settings;
mod substrate;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Parser, Debug)]
#[command(
    name = "miner-ipfs-service",
    version,
    about = "Sync IPFS pins from a Substrate miner profile"
)]
struct Cli {
    /// Path to config file (TOML/JSON). If not set, tries ./config.toml then env vars.
    #[arg(short, long)]
    config: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run the long-lived service
    Run,
    /// Perform a one-off reconcile cycle and exit
    Reconcile,
    /// Trigger IPFS garbage collection
    Gc,
    /// Print current DB state
    ShowState,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,miner_ipfs_service=debug"));
    fmt().with_env_filter(env_filter).json().init();

    let cli = Cli::parse();
    let cfg = settings::load(cli.config.as_deref()).await?;
    tracing::info!(?cfg, "effective_config");

    let pool = db::init(&cfg.db.path).await?;

    match cli.command {
        Commands::Run => service::run(cfg, pool).await?,
        Commands::Reconcile => service::reconcile_once(&cfg, &pool).await?,
        Commands::Gc => {
            let ipfs = ipfs::Client::new(cfg.ipfs.api_url.clone());
            ipfs.gc().await?;
            tracing::info!("gc_done");
        }
        Commands::ShowState => db::show_state(&pool).await?,
    }

    Ok(())
}
