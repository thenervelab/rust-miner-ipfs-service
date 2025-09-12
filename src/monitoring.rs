use anyhow::Result;
use axum::{Json, Router, routing::get};
use serde::Serialize;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::{net::TcpListener, sync::Notify};

use crate::{
    disk::disk_usage, ipfs::Client as IpfsClient, notifier::MultiNotifier, substrate::Chain,
};

#[derive(Serialize)]
struct HealthStatus {
    ipfs: String,
    blockchain: String,
    disk: String,
    pinned_content: String,
    profile: String,
    notifications: Vec<String>,
}

pub async fn run_health_server(
    ipfs: IpfsClient,
    chain: Chain,
    notifier: Arc<MultiNotifier>,
    bind_addr: &str,
    shutdown: Arc<Notify>,
) -> Result<()> {
    let app = Router::new().route(
        "/status",
        get({
            let ipfs = ipfs.clone();
            let chain = chain.clone();
            let notifier = notifier.clone();

            move || async move {
                // --- IPFS check ---
                let ipfs_status = match ipfs.check_health().await {
                    Ok(_) => "OK".to_string(),
                    Err(e) => format!("Error: {e:?}"),
                };

                // --- Blockchain check ---
                let blockchain_status = match chain.client().blocks().at_latest().await {
                    Ok(_) => "OK".to_string(),
                    Err(e) => format!("Error: {e:?}"),
                };

                // --- Disk usage check ---
                let disk_status = match disk_usage() {
                    Ok((disks, _)) if !disks.is_empty() => "OK".to_string(),
                    Ok(_) => "Error: no disks found".to_string(),
                    Err(e) => format!("Error: {e:?}"),
                };

                // --- Pin verification ---
                let pin_status = match ipfs.pin_verify().await {
                    Ok(pins) if pins.is_empty() => "OK".to_string(),
                    Ok(pins) => format!("Error: {} pins failing", pins.len()),
                    Err(e) => format!("Error: {e:?}"),
                };

                // --- Profile fetch ---
                let profile_status = match chain
                    .fetch_profile_cid(None, Some("IpfsPallet"), Some("MinerProfile"), None)
                    .await
                {
                    Ok(Some(_)) => "OK".to_string(),
                    Ok(None) => "Error: profile not found".to_string(),
                    Err(e) => format!("Error: {e:?}"),
                };

                // --- Notifications check ---
                let notif_status = notifier
                    .health_check()
                    .into_iter()
                    .map(|res| match res {
                        Ok((name, true)) => format!("{}: OK", name),
                        Ok((name, false)) => format!("{}: Error: notifier not configured", name),
                        Err(e) => format!("Error: {e:?}"),
                    })
                    .collect();

                Json(HealthStatus {
                    ipfs: ipfs_status,
                    blockchain: blockchain_status,
                    disk: disk_status,
                    pinned_content: pin_status,
                    profile: profile_status,
                    notifications: notif_status,
                })
            }
        }),
    );

    let addr: SocketAddr = bind_addr.parse()?;

    let listener = TcpListener::bind(addr).await?;

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.notified().await;
        })
        .await?;

    Ok(())
}
