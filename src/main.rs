mod db;
mod disk;
mod ipfs;
mod model;
mod monitoring;
mod notifier;
mod service;
mod settings;
mod substrate;

use crate::{
    db::CidPool,
    service::{NotifState, PinSet, ProgressReceiver},
};
use anyhow::Result;
use clap::{Parser, Subcommand};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::Mutex;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Parser, Debug)]
#[command(
    name = "miner-ipfs-service",
    version,
    about = "Sync IPFS pins from a Substrate miner profile"
)]
struct Cli {
    #[arg(short, long)]
    config: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Run,
    Reconcile,
    Gc,
    ShowState,
}

#[tokio::main]
async fn main() -> Result<()> {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,miner_ipfs_service=debug"));
    fmt().with_env_filter(env_filter).json().init();

    let cli = Cli::parse();
    let cfg = settings::load(cli.config.as_deref()).await?;
    tracing::info!(?cfg, "effective_config");

    let pool_location = cfg.db.path.clone();

    let pool = Arc::new(CidPool::init(&pool_location)?);

    let notifier = Arc::new(notifier::build_notifier_from_config(&cfg).await?);

    tracing::info!("Commence {:#?}", cli.command);

    match cli.command {
        Commands::Run => service::run(cfg, pool, notifier).await?,
        Commands::Reconcile => {
            let mut notif_state = Arc::new(Mutex::new(NotifState::default()));
            let active_pins: Arc<Mutex<HashMap<String, ProgressReceiver>>> =
                Arc::new(Mutex::new(HashMap::new()));
            let pending_pins: PinSet = Arc::new(Mutex::new(HashSet::new()));
            let _stalled_pins: PinSet = Arc::new(Mutex::new(HashSet::new()));
            let concurrency = Arc::new(tokio::sync::Semaphore::new(8)); // start with 8

            service::reconcile_once(
                &cfg,
                &pool,
                &notifier,
                &mut notif_state,
                active_pins.clone(),
                pending_pins.clone(),
                concurrency.clone(),
            )
            .await?

            // do poll progress until its done
        }
        Commands::Gc => {
            let ipfs = ipfs::Client::new(cfg.ipfs.api_url.clone());
            ipfs.gc().await?;
            tracing::info!("gc_done");
        }
        Commands::ShowState => pool.show_state()?,
    }

    Ok(())
}
