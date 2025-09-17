use std::{
    collections::HashMap,
    sync::{Arc, mpsc},
    time::Duration,
};

use anyhow::{Context, Result};
use sqlx::SqlitePool;

use tokio::{
    sync::{Mutex, Notify},
    time,
    time::sleep,
};

use crate::{
    db,
    disk::disk_usage,
    ipfs::Client as Ipfs,
    model::{FileInfo, PinState},
    notifier::MultiNotifier,
    settings::Settings,
    substrate::Chain,
};

#[derive(Default)]
pub struct NotifState {
    last_status: HashMap<String, bool>, // true = OK, false = error
}

pub enum PinProgress {
    /// Raw line from IPFS (fallback)
    Raw(String),
    /// Parsed percentage complete (if available)
    Percent(u64),
    /// Final state
    Done,
    Error(String),
}

pub type ProgressSender = mpsc::Sender<PinProgress>;
pub type ProgressReceiver = mpsc::Receiver<PinProgress>;

impl NotifState {
    async fn notify_change(
        &mut self,
        notifier: &MultiNotifier,
        key: String,
        is_ok: bool,
        ok_msg: &str,
        err_msg: &str,
    ) {
        let last = self.last_status.get(&key).copied();

        match (last, is_ok) {
            (Some(true), false) => {
                notifier
                    .notify_all(&format!("{key} failure"), err_msg)
                    .await;
            }
            (Some(false), true) => {
                notifier
                    .notify_all(&format!("{key} recovered"), ok_msg)
                    .await;
            }
            (None, false) => {
                notifier
                    .notify_all(&format!("{key} failure"), err_msg)
                    .await;
            }
            _ => {
                // Still OK or still failing → no spam
            }
        }

        self.last_status.insert(key, is_ok);
    }
}

pub async fn run(cfg: Settings, pool: SqlitePool, notifier: Arc<MultiNotifier>) -> Result<()> {
    let shutdown = Arc::new(Notify::new());
    {
        let s = shutdown.clone();
        ctrlc::set_handler(move || {
            s.notify_waiters();
        })
        .expect("ctrlc");
    }

    let mut notif_state = NotifState::default();

    tracing::info!("Connecting to IPFS node");

    let ipfs = Ipfs::new(cfg.ipfs.api_url.clone());

    tracing::info!("Connecting to Substrate node");

    let mut chain_uninited: Option<Chain> = None;

    let active_pins: Arc<Mutex<HashMap<String, ProgressReceiver>>> =
        Arc::new(Mutex::new(HashMap::new()));

    tokio::select! {
        _ = shutdown.notified() => {
            tracing::info!("Profile update shutting down during startup");
            return Ok(())
        }
        _ = async {
            while !chain_uninited.is_some() {
                match Chain::connect(&cfg.substrate.ws_url).await {
                    Ok(c) => {
                        tracing::info!("Connected to Substrate node");
                        chain_uninited = Some(c);
                        notif_state.notify_change(
                            &notifier,
                            "substrate_connect".to_string(),
                            true,
                            "Blockchain RPC node connected",
                            "unused",
                        ).await;
                        break;
                    },
                    Err(e) => {
                        tracing::info!("Connection attempt to Substrate node failed, error: {}", e);
                        notif_state.notify_change(
                            &notifier,
                            "substrate_connect".to_string(),
                            false,
                            "Blockchain RPC node connected",
                            &format!("Blockchain RPC node unreachable, error: {}", e),
                        ).await;

                        // Back off ~100ms before next retry (max 10/sec)
                        sleep(Duration::from_millis(100)).await;
                    },
                };
            };
        } => {}
    }

    let mut chain = chain_uninited.unwrap();

    if let Some(port) = cfg.monitoring.port {
        let addr = format!("0.0.0.0:{}", port);
        let ipfs_clone = ipfs.clone();
        let chain_clone = chain.clone();
        let notifier_arc = notifier.clone();
        let shutdown_clone = shutdown.clone();

        let pallet = cfg.substrate.pallet.clone();
        let storage_item = cfg.substrate.storage_item.clone();
        let miner_profile_id = cfg.substrate.miner_profile_id.clone();
        let raw_storage_key_hex = cfg.substrate.raw_storage_key_hex.clone();

        tokio::spawn(async move {
            if let Err(e) = crate::monitoring::run_health_server(
                ipfs_clone,
                chain_clone,
                notifier_arc,
                &addr,
                shutdown_clone,
                pallet,
                storage_item,
                miner_profile_id,
                raw_storage_key_hex,
            )
            .await
            {
                tracing::error!("Monitoring server failed: {:?}", e);
            }
        });
    }

    tracing::info!("Commencing node operation");

    let mut poll = time::interval(Duration::from_secs(cfg.service.poll_interval_secs));
    poll.tick().await;

    let mut reconcile = time::interval(Duration::from_secs(cfg.service.reconcile_interval_secs));
    reconcile.tick().await;

    let mut gc = time::interval(Duration::from_secs(cfg.service.ipfs_gc_interval_secs));
    gc.tick().await;

    let mut health_check =
        time::interval(Duration::from_secs(cfg.service.conn_check_interval_secs));
    health_check.tick().await;

    tokio::select! {
        _ = shutdown.notified() => {
            tracing::info!("Profile update shutting down during startup");
            return Ok(())
        }
        _ = async {
            if let Err(e) = update_profile_cid(&cfg, &pool, &mut chain).await {
                tracing::warn!(error=?e, "update_profile_cid_failed");
                notif_state.notify_change(
                    &notifier,
                    "profile_update".to_string(),
                    false,
                    "Profile update is working again",
                    &format!("Profile update failed: {}", e),
                ).await;
            } else {
                notif_state.notify_change(
                    &notifier,
                    "profile_update".to_string(),
                    true,
                    "Profile update is working again",
                    "unused",
                ).await;
            }
        } => {}
    }

    loop {
        tokio::select! {
            _ = shutdown.notified() => {
                tracing::info!("shutdown");
                break;
            }
            _ = health_check.tick() => {
                tokio::select! {
                    _ = shutdown.notified() => {
                        tracing::info!("Health check shutting down during tick");
                        break;
                    }
                    _ = async {
                        if let Err(e) = ipfs.check_health().await {
                            tracing::error!("IPFS health check failed: {:?}", e);
                            notif_state.notify_change(
                                &notifier,
                                "ipfs".to_string(),
                                false,
                                "IPFS node is healthy again",
                                &format!("IPFS node connectivity check failed: {}", e),
                            ).await;
                        } else {
                            notif_state.notify_change(
                                &notifier,
                                "ipfs".to_string(),
                                true,
                                "IPFS node is healthy again",
                                "unused",
                            ).await;
                        }

                        if let Err(e) = chain.check_health().await {
                            tracing::error!("Substrate health check failed: {:?}", e);
                            notif_state.notify_change(
                                &notifier,
                                "substrate".to_string(),
                                false,
                                "Blockchain RPC node is healthy again",
                                &format!("Blockchain RPC node connectivity check failed: {}", e),
                            ).await;
                        } else {
                            notif_state.notify_change(
                                &notifier,
                                "substrate".to_string(),
                                true,
                                "Blockchain RPC node is healthy again",
                                "unused",
                            ).await;
                        }
                    } => {}
                }
            }
            _ = poll.tick() => {
                tokio::select! {
                    _ = shutdown.notified() => {
                        tracing::info!("Profile update shutting down during tick");
                        break;
                    }
                    _ = async {
                        if let Err(e) = update_profile_cid(&cfg, &pool, &mut chain).await {
                            tracing::warn!(error=?e, "update_profile_cid_failed");
                            notif_state.notify_change(
                                &notifier,
                                "profile_update".to_string(),
                                false,
                                "Profile update is working again",
                                &format!("Profile update failed: {}", e),
                            ).await;
                        } else {
                            notif_state.notify_change(
                                &notifier,
                                "profile_update".to_string(),
                                true,
                                "Profile update is working again",
                                "unused",
                            ).await;
                        }
                    } => {}
                }
            }
            _ = reconcile.tick() => {

                tokio::select! {
                    _ = shutdown.notified() => {
                        tracing::info!("Reconcile shutting down during tick");
                        break;
                    }
                    _ = async {
                        if let Err(e) = reconcile_once(&cfg, &pool, &notifier, &mut notif_state, active_pins.clone()).await {
                            tracing::error!(error=?e, "reconcile_failed");
                            notif_state.notify_change(
                                &notifier,
                                "reconcile".to_string(),
                                false,
                                "Reconcile is working again",
                                &format!("Profile reconcile failed: {}", e),
                            ).await;
                        } else {
                            notif_state.notify_change(
                                &notifier,
                                "reconcile".to_string(),
                                true,
                                "Reconcile is working again",
                                "unused",
                            ).await;
                        }

                        if let Err(_e) = update_progress_cid(&pool,&notifier, &mut notif_state, active_pins.clone()).await {
                            //tracing::error!(error=?e, "reconcile_failed");
                            // notif_state.notify_change(
                            //     &notifier,
                            //     "reconcile".to_string(),
                            //     false,
                            //     "Reconcile is working again",
                            //     &format!("Profile reconcile failed: {}", e),
                            // ).await;
                        } else {
                            // notif_state.notify_change(
                            //     &notifier,
                            //     "reconcile".to_string(),
                            //     true,
                            //     "Reconcile is working again",
                            //     "unused",
                            // ).await;
                        }
                   } => {}
                }
            }
            _ = gc.tick() => {
                tokio::select! {
                    _ = shutdown.notified() => {
                        tracing::info!("Ipfs gc shutting down during tick");
                        break;
                    }
                    _ = async {
                        if let Err(e) = ipfs.gc().await {
                            tracing::error!(error=?e, "gc_failed");
                            notif_state.notify_change(
                                &notifier,
                                "ipfs_gc".to_string(),
                                false,
                                "IPFS GC is working again",
                                &format!("IPFS GC failed: {}", e),
                            ).await;
                        } else {
                            notif_state.notify_change(
                                &notifier,
                                "ipfs_gc".to_string(),
                                true,
                                "IPFS GC is working again",
                                "unused",
                            ).await;
                        }
                   } => {}
                }

            }
        }
    }

    Ok(())
}

pub async fn update_profile_cid(
    cfg: &Settings,
    pool: &SqlitePool,
    chain: &mut Chain,
) -> Result<()> {
    tracing::info!("Profile CID update commenced");

    let cid_opt = chain
        .fetch_profile_cid(
            cfg.substrate.raw_storage_key_hex.as_deref(),
            cfg.substrate.pallet.as_deref(),
            cfg.substrate.storage_item.as_deref(),
            cfg.substrate.miner_profile_id.as_deref(),
        )
        .await?;

    let old = db::get_profile(pool).await?;
    if cid_opt != old {
        tracing::info!(old=?old, new=?cid_opt, "profile_cid_changed");
        db::set_profile(pool, cid_opt.as_deref()).await?;
    }
    Ok(())
}

pub async fn reconcile_once(
    cfg: &Settings,
    pool: &SqlitePool,
    notifier: &MultiNotifier,
    notif_state: &mut NotifState,
    active_pins: Arc<Mutex<HashMap<String, ProgressReceiver>>>,
) -> Result<()> {
    let ipfs = Ipfs::new(cfg.ipfs.api_url.clone());

    tracing::info!("Reconcile commenced");

    let cid_opt = db::get_profile(pool).await?;
    let Some(profile_cid) = cid_opt else {
        tracing::warn!("no_profile_cid_set_yet");
        return Ok(());
    };

    let profile: Vec<FileInfo> = ipfs
        .cat_json::<Vec<FileInfo>>(&profile_cid)
        .await
        .context("fetch_profile")?;

    tracing::info!(pins = profile.len(), "profile_loaded");

    db::mark_all_unseen(pool).await?;

    let desired: Vec<String> = profile.iter().map(|p| p.cid.trim().to_string()).collect();

    for cid in desired.iter() {
        let cid = cid.clone();
        let ipfs = ipfs.clone();

        spawn_pin_task(ipfs, cid.clone(), &active_pins).await;
        db::mark_seen(&pool, &cid).await?;
    }

    let to_unpin = db::to_unpin(pool).await?;
    for cid in to_unpin {
        let ipfs = ipfs.clone();
        let pool = pool.clone();
        tokio::spawn(async move {
            let res: Result<()> = async {
                ipfs.pin_rm(&cid).await.context("pin_rm")?;
                db::delete_cid(&pool, &cid).await?;
                Ok(())
            }
            .await;

            if let Err(e) = &res {
                let _ = db::record_failure(&pool, Some(&cid), "unpin", &format!("{:?}", e)).await;
            }

            if let Err(e) = res {
                tracing::error!(?e, cid, "unpin_failed");
            }
        });
    }

    let pin_state_errors: Result<Vec<PinState>> =
        async { ipfs.pin_verify().await.context("pin_rm") }.await;

    match pin_state_errors {
        Ok(list) => {
            for p in list {
                let e = match &p.err {
                    Some(e) => {
                        tracing::error!("Problem with pinned CID: {}, Error: {}", p.cid, e);
                        e
                    }
                    _ => &"unknown".to_string(),
                };

                let cidm = format!("pinned_cid_err_{}", p.cid);
                let okm = format!("Problem with pinned CID: {} error: {}", p.cid, e);
                let errm = format!("Pinned CID OK: {}", p.cid);

                if p.err.is_some() {
                    notif_state
                        .notify_change(&notifier, cidm, false, &okm, &errm)
                        .await;
                } else {
                    notif_state
                        .notify_change(&notifier, cidm, true, &okm, &errm)
                        .await;
                };
            }
        }
        _ => {}
    };

    let (disks, _program_location_disk_usage) = match disk_usage() {
        Ok((v, f)) => (v, f),
        _ => (vec![], 404.0),
    };

    for i in 0..disks.len() {
        let available = disks[i].0 as f64 / disks[i].1 as f64 * 100.0;

        let gb: f64 = disks[i].1 as f64 / (1024.0 * 1024.0 * 1024.0);

        let diskm = format!("disk_space_disk_{}", i);
        let okm = format!(
            "Disk {} available space left: {:.2}% of {:.2} GB",
            i, available, gb
        );
        let errm = format!(
            "Disk {} available space left: {:.2}% of {:.2} GB",
            i, available, gb
        );

        if available < 50.0 {
            tracing::warn!("Disk {} available space: {}%", i, available);

            notif_state
                .notify_change(&notifier, diskm, false, &okm, &errm)
                .await;
        } else {
            tracing::info!("Disk {} available space: {}%", i, available);
            notif_state
                .notify_change(&notifier, diskm, true, &okm, &errm)
                .await;
        };
    }

    tracing::info!("Reconcile finished");
    Ok(())
}

async fn spawn_pin_task(
    ipfs: Ipfs,
    cid: String,
    active_pins: &Arc<Mutex<HashMap<String, ProgressReceiver>>>,
) {
    let mut active = active_pins.lock().await;

    // If already running, don’t start again
    if active.contains_key(&cid) {
        tracing::debug!("Pin task already running for CID {}", cid);
        return;
    }

    let (tx, rx) = mpsc::channel();

    // Store the receiver in the map
    active.insert(cid.clone(), rx);

    // Spawn the actual pinning worker
    tokio::spawn({
        tracing::info!("Starting new pin task for CID: {}", cid);
        let ipfs = ipfs.clone();
        async move {
            let _res = ipfs.pin_add_with_progress(&cid, tx).await;
        }
    });
}

pub async fn update_progress_cid(
    pool: &SqlitePool,
    _notifier: &MultiNotifier,
    _notif_state: &mut NotifState,
    active_pins: Arc<Mutex<HashMap<String, ProgressReceiver>>>,
) -> Result<()> {
    let mut active = active_pins.lock().await;
    let mut finished = Vec::new();
    let mut errored = Vec::new();

    tracing::info!("{} Active pins", active.len());

    for (cid, rx) in active.iter_mut() {
        let mut latest: Option<PinProgress> = None;

        while let Ok(progress) = rx.try_recv() {
            latest = Some(progress);
        }

        if let Some(p) = latest {
            match p {
                PinProgress::Percent(v) => {
                    tracing::info!("CID {} progress: {} blocks", &cid[0..16], v);
                }
                PinProgress::Done => {
                    tracing::info!("CID {} pin complete", &cid[0..16]);
                    finished.push(cid.clone());
                }
                PinProgress::Error(e) => {
                    tracing::error!("CID {} pin error: {}", &cid[0..16], e);
                    let _ = db::record_failure(&pool, Some(&cid), "pin", &format!("{:?}", e)).await;
                    errored.push(cid.clone());
                }
                PinProgress::Raw(line) => {
                    tracing::debug!("CID {} raw: {}", &cid[0..16], line);
                }
            }
        }
    }

    for cid in errored {
        active.remove(&cid);
    }

    for cid in finished {
        db::record_pin(&pool, &cid, true).await?;
    }

    Ok(())
}
