use anyhow::Result;
use axum::{Json, Router, routing::get};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};
use tokio::{net::TcpListener, sync::Notify};

use crate::{
    disk::disk_usage, ipfs::IpfsClient, model::PinState, notifier::MultiNotifier, substrate::Chain,
};

#[derive(Serialize, Deserialize)]
struct DiskInfo {
    available_percent: f64,
    total_gb: f64,
}

#[derive(Serialize, Deserialize)]
pub struct HealthStatus {
    ipfs: String,
    blockchain: String,
    disk: String,
    disk_info: Vec<DiskInfo>,
    profile: String,
    notifications: Vec<String>,
    pin_status: String,
    pinned_content: Vec<PinState>,
}

pub async fn status_handler<C>(
    ipfs: &Arc<C>,
    chain: &mut Chain,
    notifier: &Arc<MultiNotifier>,
    substrate_pallet: Option<String>,
    storage_item: Option<String>,
    miner_account_hex: Option<String>,
    raw_storage_key_hex: Option<String>,
) -> Json<HealthStatus>
where
    C: IpfsClient + 'static,
{
    // --- IPFS check ---
    let ipfs_status = match ipfs.check_health().await {
        Ok(_) => "OK".to_string(),
        Err(e) => format!("Error: {}", e),
    };

    // --- Blockchain check ---
    let blockchain_status = match chain.client().blocks_at_latest().await {
        Ok(_) => "OK".to_string(),
        Err(e) => format!("Error: {}", e),
    };

    // --- Disk usage check ---
    let (disks, _) = disk_usage();
    let (disk_status, disk_info) = if !disks.is_empty() {
        let infos = disks
            .iter()
            .map(|(avail, total)| DiskInfo {
                available_percent: *avail as f64 / *total as f64 * 100.0,
                total_gb: *total as f64 / (1024.0 * 1024.0 * 1024.0),
            })
            .collect();
        ("OK".to_string(), infos)
    } else {
        ("Error: no disks found".to_string(), vec![])
    };

    // --- Pin verification ---
    let (pin_status, pinned_content) = match ipfs.pin_verify().await {
        Ok(pins) => {
            let mut status: String = "Status: ".to_string();
            let mut all_pin_ok = true;
            for pin_state in &pins {
                if !pin_state.ok {
                    let e = pin_state.err.as_deref().unwrap_or("unknown");
                    status.push_str(&format!("CID: {} Error: {} ", pin_state.cid, e));
                    all_pin_ok = false;
                }
            }

            if all_pin_ok {
                status.push_str("OK");
            }

            (status, pins)
        }
        Err(e) => (format!("Internal error: {}", e), vec![]),
    };

    // --- Profile fetch ---
    let profile_status = match chain
        .fetch_profile_cid(
            raw_storage_key_hex.as_deref(),
            substrate_pallet.as_deref(),
            storage_item.as_deref(),
            miner_account_hex.as_deref(),
        )
        .await
    {
        Ok(Some(_)) => "OK".to_string(),
        Ok(None) => "Error: profile not found".to_string(),
        Err(e) => format!("Error: {}", e),
    };

    // --- Notifications check ---
    let notif_status = notifier
        .health_check()
        .into_iter()
        .map(|res| match res {
            Ok((name, true)) => format!("{}: OK", name),
            Ok((name, false)) => format!("{}: Error: notifier not configured", name),
            Err(e) => format!("Error: {}", e),
        })
        .collect();

    Json(HealthStatus {
        ipfs: ipfs_status,
        blockchain: blockchain_status,
        disk: disk_status,
        disk_info,
        profile: profile_status,
        notifications: notif_status,
        pin_status,
        pinned_content,
    })
}

/// HTTP server that just wraps the handler
pub async fn run_health_server<C>(
    ipfs: Arc<C>,
    chain: Chain,
    notifier: Arc<MultiNotifier>,
    bind_addr: &str,
    shutdown: Arc<Notify>,
    substrate_pallet: Option<String>,
    storage_item: Option<String>,
    miner_account_hex: Option<String>,
    raw_storage_key_hex: Option<String>,
) -> Result<()>
where
    C: IpfsClient + 'static,
{
    tracing::info!("Starting monitoring server on {}", bind_addr);

    let app = Router::new().route(
        "/status",
        get({
            let ipfs = ipfs.clone();
            let chain = chain.clone();
            let notifier = notifier.clone();
            let pallet = substrate_pallet.clone();
            let item = storage_item.clone();
            let miner_hex = miner_account_hex.clone();
            let raw_key = raw_storage_key_hex.clone();

            move || {
                let ipfs = ipfs.clone();
                let mut chain = chain.clone();
                let notifier = notifier.clone();
                let pallet = pallet.clone();
                let item = item.clone();
                let miner_hex = miner_hex.clone();
                let raw_key = raw_key.clone();

                async move {
                    status_handler(
                        &ipfs, &mut chain, &notifier, pallet, item, miner_hex, raw_key,
                    )
                    .await
                }
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

//          //          //          //          //          //          //          //          //          //          //          //

//                      //                      //                      //                                  //                      //

//                      //                      //          //          //          //                      //                      //

//                      //                      //                                  //                      //                      //

//                      //                      //          //          //          //                      //                      //

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::tests::DummyIpfs;

    #[tokio::test]
    async fn status_ok_with_dummy() {
        let ipfs = Arc::new(DummyIpfs::default());
        let notifier = Arc::new(MultiNotifier::new());
        let mut chain = Chain::dummy(true, Some(Ok(Some("profile".into()))));

        let response = status_handler(&ipfs, &mut chain, &notifier, None, None, None, None).await;

        let body = serde_json::to_string(&response.0).unwrap();
        assert!(body.contains("\"ipfs\""));
        assert!(body.contains("\"disk_info\""));
    }

    #[tokio::test]
    async fn status_ipfs_down() {
        use crate::service::tests::DummyIpfs;

        // Simulate IPFS failing health check
        let mut bad_ipfs = DummyIpfs::default();
        bad_ipfs.health_ok = false;
        let ipfs = Arc::new(bad_ipfs);

        let notifier = Arc::new(MultiNotifier::new());
        let mut chain = Chain::dummy(true, Some(Ok(Some("profile".into()))));

        let response = status_handler(&ipfs, &mut chain, &notifier, None, None, None, None).await;

        let body = serde_json::to_string(&response.0).unwrap();
        assert!(body.contains("Error: ipfs down"));
    }

    #[tokio::test]
    async fn status_blockchain_down() {
        let ipfs = Arc::new(DummyIpfs::default());
        let notifier = Arc::new(MultiNotifier::new());
        let mut chain = Chain::dummy(false, Some(Ok(Some("profile".into())))); // blocks_at_latest fails

        let response = status_handler(&ipfs, &mut chain, &notifier, None, None, None, None).await;
        let body = serde_json::to_string(&response.0).unwrap();
        assert!(body.contains("Error: chain down"));
    }

    #[tokio::test]
    async fn status_pin_verify_error() {
        let mut dummy = DummyIpfs::default();
        dummy.pin_verify_result = Err(anyhow::anyhow!("pin verify failed"));
        let ipfs = Arc::new(dummy);
        let notifier = Arc::new(MultiNotifier::new());
        let mut chain = Chain::dummy(true, Some(Ok(Some("profile".into()))));

        let response = status_handler(&ipfs, &mut chain, &notifier, None, None, None, None).await;
        let body = serde_json::to_string(&response.0).unwrap();
        assert!(body.contains("Internal error:"));
    }

    #[tokio::test]
    async fn status_pin_with_error() {
        let mut dummy = DummyIpfs::default();
        dummy.pin_verify_result = Ok(vec![PinState {
            cid: "cid1".into(),
            ok: false,
            err: Some("disk full".into()),
            pin_status: None,
        }]);
        let ipfs = Arc::new(dummy);
        let notifier = Arc::new(MultiNotifier::new());
        let mut chain = Chain::dummy(true, Some(Ok(Some("profile".into()))));

        let response = status_handler(&ipfs, &mut chain, &notifier, None, None, None, None).await;
        let body = serde_json::to_string(&response.0).unwrap();
        assert!(body.contains("disk full"));
    }

    #[tokio::test]
    async fn status_profile_not_found() {
        let ipfs = Arc::new(DummyIpfs::default());
        let notifier = Arc::new(MultiNotifier::new());
        // Chain returns Ok(None) → should map to "Error: profile not found"
        let mut chain = Chain::dummy(true, Some(Ok(None)));

        let response = status_handler(
            &ipfs,
            &mut chain,
            &notifier,
            Some("dummy_pallet".into()),
            Some("dummy_item".into()),
            Some("deadbeef".into()),
            None,
        )
        .await;
        let body = serde_json::to_string(&response.0).unwrap();

        // Deserialize into struct
        let status: HealthStatus = serde_json::from_str(&body).unwrap();
        assert_eq!(status.profile, "Error: profile not found");
    }

    #[tokio::test]
    async fn status_profile_error() {
        let ipfs = Arc::new(DummyIpfs::default());
        let notifier = Arc::new(MultiNotifier::new());
        // Chain returns Err → should map to "Error: profile error"
        let mut chain = Chain::dummy(true, Some(Err(anyhow::anyhow!("profile error"))));

        let response = status_handler(
            &ipfs,
            &mut chain,
            &notifier,
            Some("dummy_pallet".into()),
            Some("dummy_item".into()),
            Some("deadbeef".into()),
            None,
        )
        .await;
        let body = serde_json::to_string(&response.0).unwrap();

        // Deserialize into struct
        let status: HealthStatus = serde_json::from_str(&body).unwrap();
        assert!(status.profile.contains("profile error"));
    }

    #[tokio::test]
    async fn status_disk_usage_ok() {
        let ipfs = Arc::new(DummyIpfs::default());
        let notifier = Arc::new(MultiNotifier::new());
        let mut chain = Chain::dummy(true, Some(Ok(Some("profile".into()))));

        let response = status_handler(&ipfs, &mut chain, &notifier, None, None, None, None).await;
        let body = serde_json::to_string(&response.0).unwrap();

        // Explicitly assert the OK disk branch
        assert!(body.contains("\"disk\":\"OK\""));
        assert!(body.contains("\"disk_info\":["));
    }

    #[tokio::test]
    async fn run_server_integration() {
        use reqwest::Client;
        use tokio::time::{Duration, sleep};

        let ipfs = Arc::new(DummyIpfs::default());
        let notifier = Arc::new(MultiNotifier::new());
        let chain = Chain::dummy(true, Some(Ok(Some("profile".into()))));
        let shutdown = Arc::new(Notify::new());

        // Bind to ephemeral port
        let addr = "127.0.0.1:0";
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let shutdown_clone = shutdown.clone();

        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/status",
                    get({
                        let ipfs = ipfs.clone();
                        let mut chain = chain.clone();
                        let notifier = notifier.clone();
                        move || async move {
                            status_handler(&ipfs, &mut chain, &notifier, None, None, None, None)
                                .await
                        }
                    }),
                ),
            )
            .with_graceful_shutdown(async move {
                shutdown_clone.notified().await;
            })
            .await
            .unwrap();
        });

        // Give server time to start
        sleep(Duration::from_millis(100)).await;

        let client = Client::new();
        let resp = client
            .get(&format!("http://127.0.0.1:{}/status", port))
            .send()
            .await
            .unwrap();
        let text = resp.text().await.unwrap();

        assert!(text.contains("\"ipfs\""));

        // trigger shutdown
        shutdown.notify_one();
    }
}
