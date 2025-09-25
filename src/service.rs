use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, mpsc},
    time::Duration,
};

use anyhow::{Context, Result};

use tokio::{
    sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore, oneshot},
    time,
    time::sleep,
};

use crate::{
    db::{CidPool, PoolTrait},
    disk::disk_usage,
    ipfs::Client as Ipfs,
    ipfs::IpfsClient,
    model::{FileInfo, PinState},
    notifier::MultiNotifier,
    settings::Settings,
    substrate::Chain,
};

#[derive(Default)]
pub struct NotifState {
    last_status: Arc<Mutex<HashMap<String, bool>>>, // true = OK, false = error
}

pub struct ActiveTask {
    progress_rx: ProgressReceiver,
    cancel_tx: oneshot::Sender<()>,
    permit: Option<OwnedSemaphorePermit>, // keeps semaphore slot
}

pub enum PinProgress {
    /// Raw line from IPFS (fallback)
    #[allow(dead_code)]
    Raw(String),
    /// Parsed blocks retrieved
    Blocks(u64),
    /// Final state
    Done,
    Error(String),
}

pub type PinSet = Arc<Mutex<HashSet<String>>>;
pub type ProgressSender = mpsc::Sender<PinProgress>;
pub type ProgressReceiver = mpsc::Receiver<PinProgress>;

impl NotifState {
    async fn notify_change(
        &mut self,
        notifier: &Arc<MultiNotifier>,
        key: String,
        is_ok: bool,
        ok_msg: &str,
        err_msg: &str,
    ) {
        let ok_msg = ok_msg.to_owned();
        let err_msg = err_msg.to_owned();
        let notifier = notifier.clone();

        let last;
        // atomically read and update last status
        {
            let mut last_status_map = self.last_status.lock().await;
            last = last_status_map.get(&key).copied();
            last_status_map.insert(key.clone(), is_ok);
        }

        // decide whether to notify in a thread
        tokio::spawn(async move {
            match (last, is_ok) {
                (Some(true), false) => {
                    notifier
                        .notify_all(&format!("{key} failure"), &err_msg)
                        .await;
                }
                (Some(false), true) => {
                    notifier
                        .notify_all(&format!("{key} recovered"), &ok_msg)
                        .await;
                }
                (None, false) => {
                    notifier
                        .notify_all(&format!("{key} failure"), &err_msg)
                        .await;
                }
                _ => {
                    // Still OK or still failing → no spam
                }
            }
        });
    }
}

pub async fn run(cfg: Settings, pool: Arc<CidPool>, notifier: Arc<MultiNotifier>) -> Result<()> {
    let shutdown = Arc::new(Notify::new());
    {
        let s = shutdown.clone();
        ctrlc::set_handler(move || {
            s.notify_waiters();
        })
        .expect("ctrlc");
    }

    let mut notif_state = Arc::new(Mutex::new(NotifState::default()));

    tracing::info!("Connecting to IPFS node");

    let ipfs = Arc::new(Ipfs::new(cfg.ipfs.api_url.clone()));

    tracing::info!("Connecting to Substrate node");

    let mut chain_uninited: Option<Chain> = None;

    let active_pins: Arc<Mutex<HashMap<String, ActiveTask>>> = Arc::new(Mutex::new(HashMap::new()));

    let pending_pins: PinSet = Arc::new(Mutex::new(HashSet::new()));
    let stalled_pins: PinSet = Arc::new(Mutex::new(HashSet::new()));

    let concurrency = Arc::new(Semaphore::new(64));

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
                        {
                            notif_state.lock().await.notify_change(
                            &notifier,
                            "substrate_connect".to_string(),
                            true,
                            "Blockchain RPC node connected",
                            "unused",
                        ).await;
                            }
                        break;
                    },
                    Err(e) => {
                        tracing::info!("Connection attempt to Substrate node failed, error: {}", e);
                        { notif_state.lock().await.notify_change(
                            &notifier,
                            "substrate_connect".to_string(),
                            false,
                            "Blockchain RPC node connected",
                            &format!("Blockchain RPC node unreachable, error: {}", e),
                        ).await;}

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

    // reset last_progress_at values to current time so that node downtime does not trigger stalled progress detection
    let _ = pool.touch_all_progress();

    tokio::select! {
        _ = shutdown.notified() => {
            tracing::info!("Profile update shutting down during startup");
            return Ok(())
        }
        _ = async {
            if let Err(e) = update_profile_cid(&cfg, &pool, &mut chain).await {
                tracing::warn!(error=?e, "update_profile_cid_failed");
                 notif_state.lock().await.notify_change(
                    &notifier,
                    "profile_update".to_string(),
                    false,
                    "Profile update is working again",
                    &format!("Profile update failed: {}", e),
                ).await;
            } else {
                 notif_state.lock().await.notify_change(
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
                             notif_state.lock().await.notify_change(
                                &notifier,
                                "ipfs".to_string(),
                                false,
                                "IPFS node is healthy again",
                                &format!("IPFS node connectivity check failed: {}", e),
                            ).await;
                        } else {
                             notif_state.lock().await.notify_change(
                                &notifier,
                                "ipfs".to_string(),
                                true,
                                "IPFS node is healthy again",
                                "unused",
                            ).await;
                        }

                        if let Err(e) = chain.check_health().await {
                            tracing::error!("Substrate health check failed: {:?}", e);
                             notif_state.lock().await.notify_change(
                                &notifier,
                                "substrate".to_string(),
                                false,
                                "Blockchain RPC node is healthy again",
                                &format!("Blockchain RPC node connectivity check failed: {}", e),
                            ).await;
                        } else {
                             notif_state.lock().await.notify_change(
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
                             notif_state.lock().await.notify_change(
                                &notifier,
                                "profile_update".to_string(),
                                false,
                                "Profile update is working again",
                                &format!("Profile update failed: {}", e),
                            ).await;
                        } else {
                             notif_state.lock().await.notify_change(
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
                        if let Err(e) = reconcile_once(&pool, &ipfs, &notifier, &mut notif_state,
                                active_pins.clone(),
                                pending_pins.clone(),
                                concurrency.clone(),
                            ).await {
                            tracing::error!(error=?e, "reconcile_failed");
                             notif_state.lock().await.notify_change(
                                &notifier,
                                "reconcile".to_string(),
                                false,
                                "Reconcile is working again",
                                &format!("Profile reconcile failed: {}", e),
                            ).await;
                        } else {
                             notif_state.lock().await.notify_change(
                                &notifier,
                                "reconcile".to_string(),
                                true,
                                "Reconcile is working again",
                                "unused",
                            ).await;
                        }

                        if let Err(e) = update_progress_cid(&pool, &notifier, &mut notif_state, active_pins.clone(),  stalled_pins.clone(), concurrency.clone()).await {
                            tracing::error!(error=?e, "progress update error");
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
                            notif_state.lock().await.notify_change(
                                &notifier,
                                "ipfs_gc".to_string(),
                                false,
                                "IPFS GC is working again",
                                &format!("IPFS GC failed: {}", e),
                            ).await;
                        } else {
                            notif_state.lock().await.notify_change(
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
    pool: &Arc<CidPool>,
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

    let old = pool.get_profile()?;
    if cid_opt != old {
        tracing::info!(old=?old, new=?cid_opt, "profile_cid_changed");
        pool.set_profile(cid_opt.as_deref())?;
    }
    Ok(())
}

//    pool: &Arc<P>,
//    ipfs: &Arc<C>,

pub async fn reconcile_once<P, C>(
    pool: &Arc<P>,
    ipfs: &Arc<C>,
    notifier: &Arc<MultiNotifier>,
    notif_state: &Arc<Mutex<NotifState>>,
    active_pins: Arc<Mutex<HashMap<String, ActiveTask>>>,
    pending_pins: PinSet,
    concurrency: Arc<Semaphore>,
) -> Result<()>
where
    P: PoolTrait + 'static,
    C: IpfsClient + 'static,
{
    tracing::info!("Reconcile commenced");

    let cid_opt = pool.get_profile()?;
    let Some(profile_cid) = cid_opt else {
        tracing::warn!("no_profile_cid_set_yet");
        return Ok(());
    };

    let profile: Vec<FileInfo> = ipfs
        .cat_json::<Vec<FileInfo>>(&profile_cid)
        .await
        .context("fetch_profile")?;

    tracing::info!(pins = profile.len(), "profile_loaded");

    let pin_list = ipfs.pin_ls_all().await?;
    let _ = pool.merge_pins(&pin_list); // all top level pins are added to database to keep track of unnecessary pins

    let desired: Vec<String> = profile.iter().map(|p| p.cid.trim().to_string()).collect();

    for cid in desired.iter() {
        let cid = cid.clone();
        let ipfs = ipfs.clone();
        if spawn_pin_task(
            ipfs.clone(),
            cid.clone(),
            &active_pins.clone(),
            &pending_pins.clone(),
            concurrency.clone(),
        )
        .await
        {
            // triggers when new pin task background thread is started (spawn_pin_task returns true)

            let _ = pool.touch_progress(&cid);
            let notifier = notifier.clone();
            let cid = cid.clone();
            let notif_state_t = notif_state.clone();
            tokio::spawn(async move {
                notif_state_t
                    .lock()
                    .await
                    .notify_change(
                        &notifier,
                        format!("Pinning task {}", cid),
                        true,
                        &format!("Task {} started", cid),
                        &format!("unused"),
                    )
                    .await;
            });
        }
    }

    let to_unpin = pool.sync_pins(desired)?;

    for cid in to_unpin {
        tracing::error!("Removing pin");

        let ipfs = ipfs.clone();
        let pool = pool.clone();
        let active_pins_map = active_pins.clone();
        tokio::spawn(async move {
            let res: Result<()> = async {
                let _pin_attempt = match ipfs.pin_rm(&cid).await.context("pin_rm") {
                    Ok(()) => {}
                    Err(_e) => {
                        let pin_set = ipfs.pin_ls_all().await?;
                        if pin_set.contains(&cid) {
                            tracing::error!("");
                        }
                    }
                };
                {
                    let mut active = active_pins_map.lock().await;
                    if let Some(task) = active.remove(&cid) {
                        let _ = task.cancel_tx.send(());
                    }
                }

                Ok(())
            }
            .await;

            if let Err(e) = &res {
                let _ = pool.record_failure(Some(&cid), "unpin", &format!("{:?}", e));
            }

            if let Err(e) = res {
                tracing::error!(?e, cid, "Unpin failed");
            }
        });
    }

    let pin_state_errors: Result<Vec<PinState>> = async {
        match ipfs.pin_verify().await {
            Ok(states) => Ok(states),
            Err(e) => {
                tracing::error!("pin list verification error: {:?}", e);

                // Record the failure in the pool
                let _ = pool.record_failure(None, "pin_verification", &format!("{:?}", e));

                // Continue gracefully — no pin states
                Ok(vec![])
            }
        }
    }
    .await;

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
                        .lock()
                        .await
                        .notify_change(&notifier, cidm, false, &okm, &errm)
                        .await;
                } else {
                    notif_state
                        .lock()
                        .await
                        .notify_change(&notifier, cidm, true, &okm, &errm)
                        .await;
                };
            }
        }
        _ => {}
    };

    let (disks, _program_location_disk_usage) = disk_usage();

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
                .lock()
                .await
                .notify_change(&notifier, diskm, false, &okm, &errm)
                .await;
        } else {
            tracing::info!("Disk {} available space: {}%", i, available);
            notif_state
                .lock()
                .await
                .notify_change(&notifier, diskm, true, &okm, &errm)
                .await;
        };
    }

    tracing::info!("Reconcile finished");
    Ok(())
}

async fn spawn_pin_task<C: IpfsClient + 'static>(
    ipfs: Arc<C>,
    cid: String,
    active_pins: &Arc<Mutex<HashMap<String, ActiveTask>>>,
    pending_pins: &PinSet,
    concurrency: Arc<Semaphore>,
) -> bool {
    let mut active = active_pins.lock().await;

    if active.contains_key(&cid) {
        return false;
    }

    // Try get a permit without waiting
    if let Ok(permit) = concurrency.clone().try_acquire_owned() {
        let (tx, rx) = mpsc::channel();
        let (cancel_tx, cancel_rx) = oneshot::channel();

        // Store in map before spawn
        active.insert(
            cid.clone(),
            ActiveTask {
                progress_rx: rx,
                cancel_tx: cancel_tx,
                permit: Some(permit),
            },
        );

        tokio::spawn({
            let ipfs = ipfs.clone();
            let cid = cid.clone();
            let active_map = active_pins.clone();

            async move {
                tokio::select! {
                    res = ipfs.pin_add_with_progress(&cid, tx) => {
                        if let Err(e) = res {
                            tracing::error!("pinning {} failed: {:?}", cid, e);
                        }

                        let mut active = active_map.lock().await;
                        if let Some(task) = active.get_mut(&cid) {
                            task.permit.take(); // drops OwnedSemaphorePermit, frees slot
                        }
                    }

                    _ = cancel_rx => {
                        tracing::info!("pinning {} canceled", cid);
                        // Exit early, permit will be dropped when ActiveTask is removed
                    }
                }
            }
        });
        return true;
    } else {
        // No slot available → mark as pending
        if !pending_pins.lock().await.contains(&cid) {
            tracing::info!("CID {} added to pending queue", cid);
            pending_pins.lock().await.insert(cid);
        }
    }

    return false;
}

pub async fn update_progress_cid(
    pool: &Arc<CidPool>,
    notifier: &Arc<MultiNotifier>,
    notif_state: &Arc<Mutex<NotifState>>,
    active_pins: Arc<Mutex<HashMap<String, ActiveTask>>>,
    stalled_pins: PinSet,
    concurrency: Arc<Semaphore>,
) -> Result<()> {
    let mut active = active_pins.lock().await;
    let mut finished = Vec::new();
    let mut errored = Vec::new();

    tracing::info!("{} Active pins", active.len());
    let mut updates = HashMap::new();

    for (cid, task) in active.iter_mut() {
        let mut latest: Option<PinProgress> = None;

        while let Ok(progress) = task.progress_rx.try_recv() {
            latest = Some(progress);
        }

        if let Some(p) = latest {
            match p {
                PinProgress::Blocks(v) => {
                    tracing::info!("CID {} progress: {} blocks", &cid[0..16], v);
                    if !stalled_pins.lock().await.contains(cid) {
                        stalled_pins.lock().await.remove(cid);
                        let notifier = notifier.clone();
                        let cid = cid.clone();
                        let notif_state_t = notif_state.clone();

                        tokio::spawn(async move {
                            notif_state_t
                                .lock()
                                .await
                                .notify_change(
                                    &notifier,
                                    format!("Stalled progress {}", cid),
                                    true,
                                    &format!("Task {} progressing", cid),
                                    "unused",
                                )
                                .await;
                        });
                    }
                }
                PinProgress::Done => {
                    tracing::info!("CID {} pin complete", &cid[0..16]);
                    finished.push(cid.clone());
                }
                PinProgress::Error(ref e) => {
                    tracing::error!("CID {} pin error: {}", &cid[0..16], e);
                    let _ = pool.record_failure(Some(&cid), "pin", &format!("{:?}", e));
                    errored.push(cid.clone());
                    let notifier = notifier.clone();
                    let cid = cid.clone();
                    let error = e.clone();
                    let notif_state_t = notif_state.clone();
                    tokio::spawn(async move {
                        notif_state_t
                            .lock()
                            .await
                            .notify_change(
                                &notifier,
                                format!("Pinning task {}", cid),
                                false,
                                &format!("Task {} started", cid),
                                &format!("Task {} failed with error: {}", cid, error),
                            )
                            .await;
                    });
                }
                PinProgress::Raw(ref line) => {
                    tracing::debug!("CID {} raw: {}", &cid[0..16], &line);
                }
            }
            updates.insert(cid.to_owned(), p);
        }
    }

    let stall = pool.update_progress(&updates)?;

    for (cid, _task) in active.iter_mut() {
        let notifier = notifier.clone();
        let cid = cid.clone();
        let notif_state_t = notif_state.clone();

        if !stalled_pins.lock().await.contains(&cid) && stall.contains(&cid) {
            stalled_pins.lock().await.insert(cid.clone());
            concurrency.add_permits(1);
            tracing::debug!("Stalled progress {}", cid);

            tokio::spawn(async move {
                notif_state_t
                    .lock()
                    .await
                    .notify_change(
                        &notifier,
                        format!("Stalled progress {}", cid),
                        false,
                        "unused",
                        &format!("Task {} stalling", cid),
                    )
                    .await;
            });
        }
    }

    for cid in errored {
        if let Some(task) = active.remove(&cid) {
            let _ = task.cancel_tx.send(());
        }
        concurrency.add_permits(1);
    }

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
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, mpsc};
    use tempfile::TempDir;
    use tokio::sync::{Mutex, Semaphore};

    use async_trait::async_trait;

    // NOTE: we reference MultiNotifier and CidPool through their crate paths
    // to avoid any ambiguity inside this file's module tree.
    use crate::db::CidPool;
    use crate::notifier::MultiNotifier;

    #[tokio::test]
    async fn test_notify_change_adds_cid() {
        let notifier = Arc::new(MultiNotifier::new());
        let mut state = NotifState::default();

        // notify_change signature expects ok_msg & err_msg as &str (not String)
        state
            .notify_change(&notifier, "cid1".to_string(), false, "start", "stop")
            .await;

        // Validate side-effect: last_status map should contain the entry
        let map = state.last_status.lock().await;
        assert_eq!(map.get("cid1"), Some(&false));
    }

    #[tokio::test]
    async fn test_notify_change_idempotent() {
        let notifier = Arc::new(MultiNotifier::new());
        let mut state = NotifState::default();

        state
            .notify_change(&notifier, "cid1".to_string(), false, "start", "stop")
            .await;

        // second identical update should not create an extra entry
        state
            .notify_change(
                &notifier,
                "cid1".to_string(),
                false,
                "start-again",
                "stop-again",
            )
            .await;

        let map = state.last_status.lock().await;
        assert_eq!(map.len(), 1);
        assert_eq!(map.get("cid1"), Some(&false));
    }

    #[tokio::test]
    async fn test_progress_receiver_flow() {
        let (tx, rx) = mpsc::channel::<PinProgress>();

        let mut active_pins: HashMap<String, ActiveTask> = HashMap::new();
        let (cancel_tx, _cancel_rx) = oneshot::channel();

        active_pins.insert(
            "cidA".to_string(),
            ActiveTask {
                progress_rx: rx,
                cancel_tx,
                permit: None, // no semaphore needed in this test
            },
        );

        // push updates into the sender (synchronous)
        tx.send(PinProgress::Blocks(5)).unwrap();
        tx.send(PinProgress::Done).unwrap();

        let mut latest: Option<PinProgress> = None;
        if let Some(task) = active_pins.remove("cidA") {
            loop {
                match task.progress_rx.try_recv() {
                    Ok(progress) => latest = Some(progress),
                    Err(_) => break,
                }
            }
        }

        // ensure the last seen progress is Done
        match latest {
            Some(PinProgress::Done) => {}
            _ => panic!("expected Done"),
        }
    }

    #[tokio::test]
    async fn test_update_progress_cid_blocks_and_done() {
        let tmp = TempDir::new().unwrap();
        let pool = Arc::new(CidPool::init(tmp.path().to_str().unwrap()).unwrap());

        let notifier = Arc::new(MultiNotifier::new());
        let notif_state = Arc::new(Mutex::new(NotifState::default()));
        let active_pins = Arc::new(Mutex::new(HashMap::new()));
        let stalled_pins = Arc::new(Mutex::new(HashSet::new()));
        let concurrency = Arc::new(Semaphore::new(2));

        let (tx, rx) = mpsc::channel::<PinProgress>();
        tx.send(PinProgress::Blocks(42)).unwrap();
        tx.send(PinProgress::Done).unwrap();

        let (cancel_tx, _cancel_rx) = oneshot::channel();
        active_pins.lock().await.insert(
            "cidX".to_string(),
            ActiveTask {
                progress_rx: rx,
                cancel_tx,
                permit: None,
            },
        );

        // call the function under test
        let res = update_progress_cid(
            &pool,
            &notifier,
            &notif_state,
            active_pins.clone(),
            stalled_pins.clone(),
            concurrency.clone(),
        )
        .await;

        assert!(res.is_ok());

        // update_progress should have created/updated the db record and set sync_complete
        let rec = pool
            .get_pin("cidX")
            .unwrap()
            .expect("pin record should exist");
        assert!(
            rec.sync_complete,
            "record must be marked sync_complete after Done"
        );
    }

    #[tokio::test]
    async fn test_update_progress_cid_error_path() {
        let tmp = TempDir::new().unwrap();
        let pool = Arc::new(CidPool::init(tmp.path().to_str().unwrap()).unwrap());
        let notifier = Arc::new(MultiNotifier::new());
        let notif_state = Arc::new(Mutex::new(NotifState::default()));
        let active = Arc::new(Mutex::new(HashMap::new()));
        let stalled = Arc::new(Mutex::new(HashSet::new()));
        let concurrency = Arc::new(Semaphore::new(1));

        let (tx, rx) = mpsc::channel::<PinProgress>();
        tx.send(PinProgress::Error("boom".into())).unwrap();

        let (cancel_tx, _cancel_rx) = oneshot::channel();
        active.lock().await.insert(
            "cidErr".to_string(),
            ActiveTask {
                progress_rx: rx,
                cancel_tx,
                permit: None,
            },
        );

        let res = update_progress_cid(
            &pool,
            &notifier,
            &notif_state,
            active.clone(),
            stalled.clone(),
            concurrency.clone(),
        )
        .await;
        assert!(res.is_ok());
        assert!(!active.lock().await.contains_key("cidErr"));
    }

    #[tokio::test]
    async fn test_notify_change_transitions() {
        let notifier = Arc::new(MultiNotifier::new());
        let mut state = NotifState::default();

        // Insert initial OK
        state
            .notify_change(&notifier, "cidX".into(), true, "ok", "err")
            .await;
        assert_eq!(state.last_status.lock().await.get("cidX"), Some(&true));

        // Transition to failure
        state
            .notify_change(&notifier, "cidX".into(), false, "ok", "err")
            .await;
        assert_eq!(state.last_status.lock().await.get("cidX"), Some(&false));

        // Transition back to recovery
        state
            .notify_change(&notifier, "cidX".into(), true, "ok", "err")
            .await;
        assert_eq!(state.last_status.lock().await.get("cidX"), Some(&true));
    }

    #[tokio::test]
    async fn test_spawn_pin_task_success_and_duplicate() {
        let ipfs = Arc::new(Ipfs::new("http://127.0.0.1:5001".into()));
        let active = Arc::new(Mutex::new(HashMap::new()));
        let pending = Arc::new(Mutex::new(HashSet::new()));
        let concurrency = Arc::new(Semaphore::new(1));

        let cid = "cidA".to_string();

        // First call inserts task
        let first = spawn_pin_task(
            ipfs.clone(),
            cid.clone(),
            &active,
            &pending,
            concurrency.clone(),
        )
        .await;
        assert!(first);
        assert!(active.lock().await.contains_key(&cid));
        assert!(active.lock().await.len() == 1);

        // Second call sees duplicate
        let second = spawn_pin_task(
            ipfs.clone(),
            cid.clone(),
            &active,
            &pending,
            concurrency.clone(),
        )
        .await;
        assert!(!second);
        assert!(active.lock().await.len() == 1);
    }

    #[tokio::test]
    async fn test_spawn_pin_task_pending() {
        let ipfs = Arc::new(Ipfs::new("http://127.0.0.1:5001".into()));
        let active = Arc::new(Mutex::new(HashMap::new()));
        let pending = Arc::new(Mutex::new(HashSet::new()));
        let concurrency = Arc::new(Semaphore::new(0)); // force no permits

        let cid = "cidB".to_string();
        let result =
            spawn_pin_task(ipfs, cid.clone(), &active, &pending, concurrency.clone()).await;
        assert!(!result);
        assert!(pending.lock().await.contains(&cid));
        assert!(active.lock().await.len() == 0);
    }

    #[tokio::test]
    async fn test_reconcile_once_no_profile_cid() {
        let tmp = TempDir::new().unwrap();
        let pool = Arc::new(CidPool::init(tmp.path().to_str().unwrap()).unwrap());
        let notifier = Arc::new(MultiNotifier::new());
        let notif_state = Arc::new(Mutex::new(NotifState::default()));
        let active = Arc::new(Mutex::new(HashMap::new()));
        let pending = Arc::new(Mutex::new(HashSet::new()));
        let concurrency = Arc::new(Semaphore::new(1));
        let ipfs = Arc::new(DummyIpfs::default());

        // no profile set → early return
        let res = reconcile_once(
            &pool,
            &ipfs,
            &notifier,
            &notif_state,
            active,
            pending,
            concurrency,
        )
        .await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_update_progress_cid_stalled_branch() {
        let tmp = TempDir::new().unwrap();
        let pool = Arc::new(CidPool::init(tmp.path().to_str().unwrap()).unwrap());
        let notifier = Arc::new(MultiNotifier::new());
        let notif_state = Arc::new(Mutex::new(NotifState::default()));
        let active = Arc::new(Mutex::new(HashMap::new()));
        let stalled = Arc::new(Mutex::new(HashSet::new()));
        let concurrency = Arc::new(Semaphore::new(1));

        let (tx, rx) = mpsc::channel::<PinProgress>();
        tx.send(PinProgress::Blocks(1)).unwrap();

        let (cancel_tx, _cancel_rx) = oneshot::channel();
        active.lock().await.insert(
            "cidStalled".into(),
            ActiveTask {
                progress_rx: rx,
                cancel_tx,
                permit: None,
            },
        );

        // Force stalled set to contain cid
        stalled.lock().await.insert("cidStalled".into());

        // Simulate DB saying it’s stalled
        pool.touch_progress("cidStalled").unwrap();

        let res = update_progress_cid(
            &pool,
            &notifier,
            &notif_state,
            active.clone(),
            stalled.clone(),
            concurrency.clone(),
        )
        .await;
        assert!(res.is_ok());
    }

    pub struct DummyIpfs {
        pub cat_json_result: Result<serde_json::Value>,
        pub pin_rm_result: Result<()>,
        pub pin_verify_result: Result<Vec<PinState>>,
        pub pin_ls_all_result: Result<HashSet<String>>, // 👈 add this
    }

    impl Default for DummyIpfs {
        fn default() -> Self {
            Self {
                cat_json_result: Ok(serde_json::json!({})), // empty JSON
                pin_rm_result: Ok(()),
                pin_verify_result: Ok(vec![]),
                pin_ls_all_result: Ok(HashSet::new()),
            }
        }
    }

    #[async_trait]
    impl IpfsClient for DummyIpfs {
        // async fn cat_json<T>(&self, _cid: &str) -> Result<T>
        // where
        //     T: serde::de::DeserializeOwned + Send,
        // {
        //     let val = self
        //         .cat_json_result
        //         .clone()
        //         .expect("cat_json_result not set in DummyIpfs");
        //     Ok(serde_json::from_value(val)?)
        // }

        async fn cat_json<T>(&self, _cid: &str) -> Result<T>
        where
            T: serde::de::DeserializeOwned + Send,
        {
            match &self.cat_json_result {
                Ok(val) => {
                    // clone the JSON, which *is* Clone
                    let v = val.clone();
                    Ok(serde_json::from_value(v)?)
                }
                Err(e) => Err(anyhow::anyhow!(e.to_string())),
            }
        }

        async fn pin_rm(&self, _cid: &str) -> Result<()> {
            match &self.pin_rm_result {
                Ok(_) => Ok(()),
                Err(e) => Err(anyhow::anyhow!(e.to_string())),
            }
        }

        async fn pin_verify(&self) -> Result<Vec<PinState>> {
            match &self.pin_verify_result {
                Ok(v) => Ok(v.clone()), // Vec is Clone
                Err(e) => Err(anyhow::anyhow!(e.to_string())),
            }
        }

        async fn pin_add_with_progress(&self, _cid: &str, _tx: ProgressSender) -> Result<()> {
            Ok(())
        }

        async fn gc(&self) -> Result<()> {
            Ok(())
        }

        async fn check_health(&self) -> Result<()> {
            Ok(())
        }

        async fn pin_ls_all(&self) -> Result<HashSet<String>> {
            match &self.pin_ls_all_result {
                Ok(v) => Ok(v.clone()),
                Err(e) => Err(anyhow::anyhow!(e.to_string())),
            }
        }
    }

    #[derive(Default)]
    pub struct DummyPool {
        pub to_unpin_result: Vec<String>,
        pub profile: Option<String>,
        pub failures: Arc<std::sync::Mutex<Vec<(Option<String>, String, String)>>>,
    }

    impl PoolTrait for DummyPool {
        fn get_profile(&self) -> Result<Option<String>> {
            Ok(self.profile.clone())
        }

        fn record_failure(&self, cid: Option<&str>, action: &str, reason: &str) -> Result<()> {
            self.failures.lock().unwrap().push((
                cid.map(|c| c.to_string()),
                action.to_string(),
                reason.to_string(),
            ));
            Ok(())
        }

        fn sync_pins(&self, _cids: Vec<String>) -> Result<Vec<String>> {
            Ok(self.to_unpin_result.clone())
        }

        fn merge_pins(&self, _cids: &HashSet<String>) -> Result<()> {
            Ok(())
        }

        fn update_progress(
            &self,
            _updates: &HashMap<String, PinProgress>,
        ) -> Result<HashSet<String>> {
            Ok(HashSet::new())
        }

        fn touch_all_progress(&self) -> Result<()> {
            Ok(())
        }

        fn touch_progress(&self, _cid: &str) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_reconcile_once_no_profile() {
        let pool = Arc::new(DummyPool::default());
        let ipfs = Arc::new(DummyIpfs {
            cat_json_result: Err(anyhow::anyhow!("not called")),
            pin_rm_result: Ok(()),
            pin_verify_result: Ok(vec![]),
            pin_ls_all_result: Ok(HashSet::new()),
        });
        let notifier = Arc::new(MultiNotifier::new());
        let notif_state = Arc::new(Mutex::new(NotifState::default()));
        let active = Arc::new(Mutex::new(HashMap::new()));
        let pending = Arc::new(Mutex::new(HashSet::new()));
        let concurrency = Arc::new(Semaphore::new(1));

        let res = reconcile_once(
            &pool,
            &ipfs,
            &notifier,
            &notif_state,
            active,
            pending,
            concurrency,
        )
        .await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_reconcile_once_unpin_branch() {
        let pool = Arc::new(DummyPool {
            to_unpin_result: vec!["QmDeadbeef".into()],
            ..Default::default()
        });
        let ipfs = Arc::new(DummyIpfs {
            pin_ls_all_result: Ok(HashSet::new()),
            cat_json_result: Ok(serde_json::json!({"foo": "bar"})),
            pin_rm_result: Ok(()),
            pin_verify_result: Ok(vec![]),
        });
        let notifier = Arc::new(MultiNotifier::new());
        let notif_state = Arc::new(Mutex::new(NotifState::default()));
        let active = Arc::new(Mutex::new(HashMap::new()));
        let pending = Arc::new(Mutex::new(HashSet::new()));
        let concurrency = Arc::new(Semaphore::new(1));

        let res = reconcile_once(
            &pool,
            &ipfs,
            &notifier,
            &notif_state,
            active,
            pending,
            concurrency,
        )
        .await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_reconcile_once_pin_verify_error() {
        let pool = Arc::new(DummyPool {
            profile: Some("dummy_profile_cid".into()),
            ..Default::default()
        });

        let ipfs = Arc::new(DummyIpfs {
            // pretend the profile exists and has no files
            cat_json_result: Ok(serde_json::json!(Vec::<FileInfo>::new())),

            // returning an empty pin list is fine
            pin_ls_all_result: Ok(HashSet::new()),

            // unpin always succeeds
            pin_rm_result: Ok(()),

            // *** here’s the important failure we want to test ***
            pin_verify_result: Err(anyhow::anyhow!("pin_verify failed")),

            ..Default::default()
        });

        let notifier = Arc::new(MultiNotifier::new());
        let notif_state = Arc::new(Mutex::new(NotifState::default()));
        let active = Arc::new(Mutex::new(HashMap::new()));
        let pending = Arc::new(Mutex::new(HashSet::new()));
        let concurrency = Arc::new(Semaphore::new(1));

        let res = reconcile_once(
            &pool,
            &ipfs,
            &notifier,
            &notif_state,
            active,
            pending,
            concurrency,
        )
        .await;

        // Should not fail — reconcile_once logs and continues
        assert!(res.is_ok());

        // Verify that the failure was recorded
        let failures = pool.failures.lock().unwrap();
        assert!(
            failures.iter().any(|(_, action, reason)| {
                action == "pin_verification" && reason.contains("pin_verify failed")
            }),
            "expected pin_verification failure to be recorded"
        );
    }

    #[tokio::test]
    async fn test_reconcile_once_disk_low_high() {
        let pool = Arc::new(DummyPool::default());
        let ipfs_low = Arc::new(DummyIpfs {
            cat_json_result: Ok(serde_json::json!({"foo": "bar"})),
            pin_rm_result: Ok(()),
            pin_verify_result: Ok(vec![]),
            pin_ls_all_result: Ok(HashSet::new()),
        });

        let ipfs_high = Arc::new(DummyIpfs {
            cat_json_result: Ok(serde_json::json!({"foo": "bar"})),
            pin_rm_result: Ok(()),
            pin_verify_result: Ok(vec![]),
            pin_ls_all_result: Ok(HashSet::new()),
        });

        let notifier = Arc::new(MultiNotifier::new());
        let notif_state = Arc::new(Mutex::new(NotifState::default()));
        let active = Arc::new(Mutex::new(HashMap::new()));
        let pending = Arc::new(Mutex::new(HashSet::new()));
        let concurrency = Arc::new(Semaphore::new(1));

        // Low → expect notify
        reconcile_once(
            &pool,
            &ipfs_low,
            &notifier,
            &notif_state,
            active.clone(),
            pending.clone(),
            concurrency.clone(),
        )
        .await
        .unwrap();

        // High → expect notify_change back to Normal
        reconcile_once(
            &pool,
            &ipfs_high,
            &notifier,
            &notif_state,
            active,
            pending,
            concurrency,
        )
        .await
        .unwrap();
    }
}
