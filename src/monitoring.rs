use anyhow::Result;
use axum::{Json, Router, routing::get};
use serde::Serialize;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::{net::TcpListener, sync::Notify};

use crate::{
    disk::disk_usage, ipfs::Client as IpfsClient, model::PinState, notifier::MultiNotifier,
    substrate::Chain,
};

#[derive(Serialize)]
struct HealthStatus {
    ipfs: String,
    blockchain: String,
    disk: String,
    profile: String,
    notifications: Vec<String>,
    pin_status: String,
    pinned_content: Vec<PinState>,
}

pub async fn run_health_server(
    ipfs: Arc<IpfsClient>,
    chain: Chain,
    notifier: Arc<MultiNotifier>,
    bind_addr: &str,
    shutdown: Arc<Notify>,
    substrate_pallet: Option<String>,
    storage_item: Option<String>,
    miner_account_hex: Option<String>,
    raw_storage_key_hex: Option<String>,
) -> Result<()> {
    tracing::info!("Starting monitoring server on {}", bind_addr);

    let app = Router::new().route(
        "/status",
        get({
            let ipfs = ipfs.clone();
            let mut chain = chain.clone();
            let notifier = notifier.clone();
            // clone config values for the closure
            let pallet = substrate_pallet.clone();
            let item = storage_item.clone();
            let miner_hex = miner_account_hex.clone();
            let raw_key = raw_storage_key_hex.clone();

            move || async move {
                // --- IPFS check ---
                let ipfs_status = match ipfs.check_health().await {
                    Ok(_) => "OK".to_string(),
                    Err(e) => format!("Error: {e:?}"),
                };

                // --- Blockchain check ---
                let blockchain_status = match chain.client().blocks_at_latest().await {
                    Ok(_) => "OK".to_string(),
                    Err(e) => format!("Error: {e:?}"),
                };

                // --- Disk usage check ---
                let disk_status = match disk_usage() {
                    (disks, _) if !disks.is_empty() => "OK".to_string(),
                    _ => "Error: no disks found".to_string(),
                };

                // --- Pin verification ---
                let (pin_status, pinned_content) = match ipfs.pin_verify().await {
                    Ok(pins) => {
                        let mut status: String = "Status: ".to_string();
                        let mut all_pin_ok = true;
                        for pin_state in &pins {
                            if !pin_state.ok {
                                let e = match &pin_state.err {
                                    Some(e) => e,
                                    _ => &"unknown".to_string(),
                                };
                                status.push_str(&format!("CID: {} Error: {} ", pin_state.cid, e));
                                all_pin_ok = false;
                            }
                        }

                        if all_pin_ok {
                            status.push_str("OK");
                        }

                        (status, pins)
                    }
                    Err(e) => (format!("Internal error: {e:?}"), vec![]),
                };

                // --- Profile fetch ---
                let profile_status = match chain
                    .fetch_profile_cid(
                        raw_key.as_deref(),   // raw storage key hex (Option<&str>)
                        pallet.as_deref(),    // pallet name
                        item.as_deref(),      // storage item name
                        miner_hex.as_deref(), // miner account hex (Option<&str>)
                    )
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
                        Ok((name, false)) => {
                            format!("{}: Error: notifier not configured", name)
                        }
                        Err(e) => format!("Error: {e:?}"),
                    })
                    .collect();

                Json(HealthStatus {
                    ipfs: ipfs_status,
                    blockchain: blockchain_status,
                    disk: disk_status,
                    profile: profile_status,
                    notifications: notif_status,
                    pin_status: pin_status,
                    pinned_content: pinned_content,
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
