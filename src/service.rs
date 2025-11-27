use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, mpsc},
    time::Duration,
};

use anyhow::{Context, Result};

use tokio::{
    sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore, oneshot},
    time,
    time::MissedTickBehavior,
    time::sleep,
};

use crate::{
    db::{CidPool, PoolTrait},
    disk::disk_usage_with_disks,
    ipfs::Client as Ipfs,
    ipfs::IpfsClient,
    notifier::MultiNotifier,
    settings::Settings,
    substrate::Chain,
};

use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use sysinfo::Disks;

// Re-export so tests can use `crate::service::FileInfo`
pub use crate::model::FileInfo;

#[derive(Default)]
pub struct NotifState {
    last_status: Arc<Mutex<HashMap<String, bool>>>, // true = OK, false = error
}

pub struct ActiveTask {
    pub(crate) progress_rx: ProgressReceiver,
    pub(crate) cancel_tx: oneshot::Sender<()>,
    // Keeps a semaphore slot (base or extra) while the task is running
    pub(crate) permit: Option<OwnedSemaphorePermit>,
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
    pub async fn notify_change(
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

    if let Some(port) = cfg.monitoring.metrics_port.or(Some(9464)) {
        PrometheusBuilder::new()
            .with_http_listener(([0, 0, 0, 0], port))
            .install()
            .context("install prometheus exporter")?;
        tracing::info!("Prometheus metrics listening on 0.0.0.0:{port} (path: /metrics)");
    }

    describe_counter!("errors_total", Unit::Count, "Error events.");
    describe_counter!(
        "reconciles_total",
        Unit::Count,
        "Total reconcile loops run."
    );

    describe_gauge!("active_pins", Unit::Count, "Pin tasks currently running.");
    describe_gauge!("stalled_pins", Unit::Count, "Pins considered stalled.");
    describe_gauge!(
        "ipfs_disk_available_percent",
        Unit::Percent,
        "Available disk % for IPFS."
    );
    describe_histogram!(
        "reconcile_duration_seconds",
        Unit::Seconds,
        "Reconcile loop duration."
    );

    let notif_state = Arc::new(Mutex::new(NotifState::default()));

    tracing::info!("Connecting to IPFS node");

    let ipfs = Arc::new(Ipfs::new(cfg.ipfs.api_url.clone()));

    let mut bootstrap_done = false;

    match ipfs.check_health().await {
        Ok(()) => {
            tracing::info!("Connecting to IPFS bootstrap nodes");
            for addr in &cfg.ipfs.bootstrap {
                match async_std::future::timeout(
                    Duration::from_secs(10),
                    IpfsClient::connect_bootstrap(&*ipfs, addr),
                )
                .await
                {
                    Ok(Ok(())) => tracing::info!("Connected IPFS node to bootstrap peer: {addr}"),
                    Ok(Err(e)) => {
                        tracing::warn!("Failed to connect IPFS node to bootstrap {addr}: {e}")
                    }
                    Err(_) => tracing::warn!("Timed out connecting IPFS node to bootstrap {addr}"),
                }
            }

            bootstrap_done = true;
        }
        _ => {}
    }

    tracing::info!("Connecting to Substrate node");

    let mut chain_uninited: Option<Chain> = None;

    let active_pins: Arc<Mutex<HashMap<String, ActiveTask>>> = Arc::new(Mutex::new(HashMap::new()));

    let pending_pins: PinSet = Arc::new(Mutex::new(HashSet::new()));
    let stalled_pins: PinSet = Arc::new(Mutex::new(HashSet::new()));

    // Poisoning skiplist: CIDs that recently errored or stalled
    let skip_pins: PinSet = Arc::new(Mutex::new(HashSet::new()));

    // Base concurrency + adaptive extra concurrency
    let base_concurrency = Arc::new(Semaphore::new(cfg.service.initial_pin_concurrency));
    let mut extra_concurrency = Arc::new(Semaphore::new(0));

    let disks = Arc::new(std::sync::Mutex::new(Disks::new_with_refreshed_list()));
    let disk_fn = {
        let disks = Arc::clone(&disks);
        move || {
            let mut guard = disks.lock().expect("lock Disks");
            disk_usage_with_disks(&mut *guard)
        }
    };

    tokio::select! {
        _ = shutdown.notified() => {
            tracing::info!("Profile update shutting down during startup");
            return Ok(())
        }
        _ = async {
            while chain_uninited.is_none() {
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
                        {
                            notif_state.lock().await.notify_change(
                                &notifier,
                                "substrate_connect".to_string(),
                                false,
                                "Blockchain RPC node connected",
                                &format!("Blockchain RPC node unreachable, error: {}", e),
                            ).await;
                        }

                        // Back off ~100ms before next retry (max 10/sec)
                        sleep(Duration::from_millis(100)).await;
                    },
                };
            }
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
            let cfg = crate::monitoring::HealthServerConfig {
                ipfs: ipfs_clone,
                chain: chain_clone,
                notifier: notifier_arc,
                addr: addr.parse().expect("invalid bind addr"),
                shutdown: shutdown_clone,
                substrate_pallet: pallet,
                storage_item,
                miner_account_hex: miner_profile_id,
                raw_storage_key_hex,
                disks: disks.clone(),
            };

            if let Err(e) = crate::monitoring::run_health_server(cfg).await {
                tracing::error!("Monitoring server failed: {:?}", e);
            }
        });
    }

    tracing::info!("Commencing node operation");

    let mut poll = time::interval(Duration::from_secs(cfg.service.poll_interval_secs));
    poll.tick().await;

    let mut reconcile = time::interval(Duration::from_secs(cfg.service.reconcile_interval_secs));
    reconcile.set_missed_tick_behavior(MissedTickBehavior::Skip);
    reconcile.tick().await;

    let mut health_check =
        time::interval(Duration::from_secs(cfg.service.health_check_interval_secs));
    health_check.set_missed_tick_behavior(MissedTickBehavior::Skip);
    health_check.tick().await;

    // reset last_progress_at values to current time so that node downtime does not trigger stalled progress detection
    let _ = pool.touch_all_progress();

    // Initial profile CID & background profile fetch
    tokio::select! {
        _ = shutdown.notified() => {
            tracing::info!("Profile update shutting down during startup");
            return Ok(())
        }
        _ = async {
            if let Err(e) = update_profile_cid(&cfg, &pool, &mut chain, &ipfs).await {
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
                            metrics::counter!("errors_total", "component" => "ipfs", "kind" => "health").increment(1);
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

                            if !bootstrap_done {
                                tracing::info!("Connecting to IPFS bootstrap nodes");
                                for addr in &cfg.ipfs.bootstrap {
                                    match async_std::future::timeout(
                                        Duration::from_secs(10),
                                        IpfsClient::connect_bootstrap(&*ipfs, addr),
                                    )
                                    .await
                                    {
                                        Ok(Ok(())) => tracing::info!("Connected IPFS node to bootstrap peer: {addr}"),
                                        Ok(Err(e)) => tracing::warn!("Failed to connect IPFS node to bootstrap {addr}: {e}"),
                                        Err(_) => tracing::warn!("Timed out connecting IPFS node to bootstrap {addr}"),
                                    }
                                }

                                bootstrap_done = true;
                            }
                        }

                        if let Err(e) = chain.check_health().await {
                            metrics::counter!("errors_total", "component" => "substrate", "kind" => "health").increment(1);
                            tracing::error!("Substrate health check failed: {:?}", e);
                            notif_state.lock().await.notify_change(
                                &notifier,
                                "substrate".to_string(),
                                false,
                                "Blockchain RPC node is healthy again",
                                &format!("Blockchain RPC node connectivity check failed: {}", e),
                            ).await;
                        } else {
                            metrics::counter!("health_checks_total", "component" => "ipfs", "result" => "ok").increment(1);
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
                        if let Err(e) = update_profile_cid(&cfg, &pool, &mut chain, &ipfs).await {
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
                        let mut init = 0;
                        loop {
                            if init > 0 {
                                sleep(Duration::from_millis(600)).await;
                            }
                            init += 1;
                            tracing::info!("ENTER THE MATRIX");
                            let started = std::time::Instant::now();
                            if let Err(e) = reconcile_once(
                                &pool,
                                &ipfs,
                                &notifier,
                                &notif_state,
                                &active_pins,
                                &pending_pins,
                                &skip_pins,
                                &base_concurrency,
                                &extra_concurrency,
                                &disk_fn,
                            ).await {
                                metrics::counter!("errors_total", "component" => "reconcile", "kind" => "reconcile_once").increment(1);
                                tracing::error!(error=?e, "reconcile_failed");
                                notif_state.lock().await.notify_change(
                                    &notifier,
                                    "reconcile".to_string(),
                                    false,
                                    "Reconcile is working again",
                                    &format!("Profile reconcile failed: {}", e),
                                ).await;
                            } else {
                                metrics::counter!("reconciles_total").increment(1);
                                notif_state.lock().await.notify_change(
                                    &notifier,
                                    "reconcile".to_string(),
                                    true,
                                    "Reconcile is working again",
                                    "unused",
                                ).await;
                            }
                            metrics::histogram!("reconcile_duration_seconds").record(started.elapsed().as_secs_f64());
                            match update_progress_cid(
                                &pool,
                                &ipfs,
                                &notifier,
                                &notif_state,
                                active_pins.clone(),
                                stalled_pins.clone(),
                                skip_pins.clone(),
                                base_concurrency.clone(),
                                &mut extra_concurrency,
                            ).await {
                                Ok(true) => {},
                                Ok(false) => break,
                                Err(e) => tracing::error!(error=?e, "progress update error"),
                            }
                        }
                    } => {}
                }
            }
        }
    }

    Ok(())
}

///
/// Periodically:
/// 1. Fetch miner profile CID from chain.
/// 2. If it changed, update DB and spawn a background task
///    that does a *no-timeout cat* of the profile CID and
///    writes the current_latest_miner_profile (list of CIDs to pin)
///    into the DB.
///
pub async fn update_profile_cid(
    cfg: &Settings,
    pool: &Arc<CidPool>,
    chain: &mut Chain,
    ipfs: &Arc<Ipfs>,
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

        if let Some(cid) = cid_opt {
            let pool_clone = Arc::clone(pool);
            let ipfs_clone = Arc::clone(ipfs);

            // Background task, *no timeout* on the cat.
            tokio::spawn(async move {
                let cid_clone = cid.clone();
                if let Err(e) = async {
                    tracing::info!(profile_cid = %cid_clone, "Fetching miner profile for new CID");
                    let profile: Vec<FileInfo> = ipfs_clone
                        .cat_no_timeout::<Vec<FileInfo>>(&cid_clone)
                        .await
                        .context("Failed to get miner profile from IPFS")?;

                    let desired: Vec<String> =
                        profile.iter().map(|p| p.cid.trim().to_string()).collect();

                    tracing::info!(
                        pins = desired.len(),
                        profile_cid = %cid_clone,
                        "miner_profile_loaded_and_stored"
                    );

                    pool_clone
                        .set_profile_pins(&desired)
                        .context("Failed to persist latest miner profile pins")?;

                    Ok::<(), anyhow::Error>(())
                }
                .await
                {
                    tracing::error!(
                        error = ?e,
                        profile_cid = %cid_clone,
                        "background_profile_update_failed"
                    );
                    let _ = pool_clone.record_failure(
                        Some(&cid_clone),
                        "profile_cat",
                        &format!("{:?}", e),
                    );
                }
            });
        }
    }
    Ok(())
}

pub async fn reconcile_once<P, C, F>(
    pool: &Arc<P>,
    ipfs: &Arc<C>,
    notifier: &Arc<MultiNotifier>,
    notif_state: &Arc<Mutex<NotifState>>,
    active_pins: &Arc<Mutex<HashMap<String, ActiveTask>>>,
    pending_pins: &PinSet,
    skip_pins: &PinSet,
    base_concurrency: &Arc<Semaphore>,
    extra_concurrency: &Arc<Semaphore>,
    disk_fn: F,
) -> Result<()>
where
    P: PoolTrait + 'static,
    C: IpfsClient + 'static,
    F: Fn() -> (Vec<(u64, u64)>, f64),
{
    tracing::info!("Reconcile commenced");

    // Desired CIDs now come from DB: current_latest_miner_profile
    let desired = match pool.get_profile_pins()? {
        Some(list) => list,
        None => {
            tracing::warn!("no_profile_pins_set_yet");
            return Ok(());
        }
    };

    tracing::info!(pins = desired.len(), "profile_loaded");

    let pin_list = ipfs.pin_ls_all().await?;
    let _ = pool.merge_pins(&pin_list); // all top level pins are added to database to keep track of unnecessary pins

    // Poisoning protection when starting new pin tasks
    {
        let mut skip = skip_pins.lock().await;

        // If all desired CIDs are in the skiplist, clear it to retry them.
        let has_non_skipped = desired.iter().any(|cid| !skip.contains(cid));
        if !has_non_skipped && !skip.is_empty() {
            tracing::info!("All desired CIDs are in skiplist; clearing skiplist for retry");
            skip.clear();
        }

        for cid in desired.iter() {
            if skip.contains(cid) {
                // Recently stalled/errored; skip for now so they don't poison concurrency.
                continue;
            }

            let (started, permits_free) = spawn_pin_task(
                ipfs.clone(),
                cid.clone(),
                &active_pins.clone(),
                &pending_pins.clone(),
                base_concurrency.clone(),
                extra_concurrency.clone(),
            )
            .await;

            if !permits_free {
                break;
            }

            if started {
                // New background pin task started

                let _ = pool.touch_progress(cid);
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
                            "unused",
                        )
                        .await;
                });
            }
        }
    }

    let to_unpin = pool.sync_pins(desired)?;

    for cid in to_unpin {
        tracing::info!("Removing pin");

        let ipfs = ipfs.clone();
        let pool = pool.clone();
        let active_pins_map = active_pins.clone();
        tokio::spawn(async move {
            let res: Result<()> = async {
                {
                    let mut active = active_pins_map.lock().await;
                    if let Some(task) = active.remove(&cid) {
                        let _ = task.cancel_tx.send(());
                    }
                }
                ipfs.pin_rm(&cid).await
            }
            .await;

            if let Err(e) = &res {
                let _ = pool.record_failure(Some(&cid), "unpin", &format!("{:?}", e));
                tracing::error!(?e, cid, "Unpin failed");
            }
        });
    }

    let (disks, ipfs_location_disk_usage) = disk_fn();
    metrics::gauge!("ipfs_disk_available_percent").set(ipfs_location_disk_usage);

    for (_i, disk) in disks.iter().enumerate() {
        let available = disk.0 as f64 / disk.1 as f64 * 100.0;

        let gb: f64 = disk.1 as f64 / (1024.0 * 1024.0 * 1024.0);

        let diskm = "disk_space_for_ipfs".to_string();
        let okm = format!(
            "Disk used for IPFS storage available space left: {:.2}% of {:.2} GB",
            available, gb
        );
        let errm = format!(
            "Disk used for IPFS storage available space left: {:.2}% of {:.2} GB",
            available, gb
        );

        if available < 50.0 {
            tracing::warn!(
                "Available space on disk used for IPFS storage: {:.2}%",
                available
            );

            notif_state
                .lock()
                .await
                .notify_change(notifier, diskm, false, &okm, &errm)
                .await;
        } else {
            tracing::info!(
                "Available space on disk used for IPFS storage: {:.2}%",
                available
            );
            notif_state
                .lock()
                .await
                .notify_change(notifier, diskm, true, &okm, &errm)
                .await;
        };
    }

    notif_state
        .lock()
        .await
        .notify_change(
            notifier,
            "cant_find_ipfs_disk".to_string(),
            ipfs_location_disk_usage != 404.0,
            "Disk containing ipfs storage found",
            "Disk containing ipfs storage not found",
        )
        .await;

    tracing::info!("Reconcile finished");
    Ok(())
}

async fn spawn_pin_task<C: IpfsClient + 'static>(
    ipfs: Arc<C>,
    cid: String,
    active_pins: &Arc<Mutex<HashMap<String, ActiveTask>>>,
    pending_pins: &PinSet,
    base_concurrency: Arc<Semaphore>,
    extra_concurrency: Arc<Semaphore>,
) -> (bool, bool) {
    let mut active = active_pins.lock().await;

    if active.contains_key(&cid) {
        return (false, true);
    }

    // Try to get a permit without waiting: first from base, then from extra.
    let permit = if let Ok(p) = base_concurrency.clone().try_acquire_owned() {
        Some(p)
    } else if let Ok(p) = extra_concurrency.clone().try_acquire_owned() {
        Some(p)
    } else {
        None
    };

    if let Some(permit) = permit {
        let (tx, rx) = mpsc::channel();
        let (cancel_tx, cancel_rx) = oneshot::channel();

        // Store in map before spawn
        active.insert(
            cid.clone(),
            ActiveTask {
                progress_rx: rx,
                cancel_tx,
                permit: Some(permit),
            },
        );

        tokio::spawn({
            let active_map = active_pins.clone();

            async move {
                tokio::select! {
                    res = ipfs.pin_add_with_progress(&cid, tx) => {
                        if let Err(e) = res {
                            tracing::error!("pinning {} failed: {:?}", cid, e);
                        }

                        let mut active = active_map.lock().await;
                        if let Some(task) = active.get_mut(&cid) {
                            // Drop permit, freeing the slot.
                            task.permit.take();
                        }
                    }

                    _ = cancel_rx => {
                        tracing::info!("pinning {} canceled", cid);
                        // Permit will be dropped when ActiveTask is removed from the map.
                    }
                }
            }
        });
        (true, true)
    } else {
        // No slot available → mark as pending
        let mut pending = pending_pins.lock().await;
        if !pending.contains(&cid) {
            // tracing::info!("CID {} added to pending queue", cid);
            pending.insert(cid);
        }
        (false, false)
    }
}

pub async fn update_progress_cid<P, C>(
    pool: &Arc<P>,
    ipfs: &Arc<C>,
    notifier: &Arc<MultiNotifier>,
    notif_state: &Arc<Mutex<NotifState>>,
    active_pins: Arc<Mutex<HashMap<String, ActiveTask>>>,
    stalled_pins: PinSet,
    skip_pins: PinSet,
    _base_concurrency: Arc<Semaphore>,
    extra_concurrency: &mut Arc<Semaphore>,
) -> Result<bool>
where
    P: PoolTrait + 'static,
    C: IpfsClient + 'static,
{
    let mut active = active_pins.lock().await;
    let mut finished = Vec::new();
    let mut pin_complete = false;
    let mut errored = Vec::new();

    tracing::info!("{} Active pins", active.len());
    let mut updates = HashMap::new();

    // 1) Drain progress channels
    for (cid, task) in active.iter_mut() {
        let mut latest: Option<PinProgress> = None;

        while let Ok(progress) = task.progress_rx.try_recv() {
            latest = Some(progress);
        }

        if let Some(p) = latest {
            match &p {
                PinProgress::Blocks(v) => {
                    tracing::info!("CID {} progress: {} blocks", &cid[0..16], v);
                    if stalled_pins.lock().await.contains(cid) {
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
                    let notifier = notifier.clone();
                    let cid_t = cid.clone();
                    let notif_state_t = notif_state.clone();

                    tokio::spawn(async move {
                        notif_state_t
                            .lock()
                            .await
                            .notify_change(
                                &notifier,
                                format!("Completed pin {}", cid_t),
                                true,
                                &format!("Pin task complete for {}", cid_t),
                                "unused",
                            )
                            .await;
                    });

                    finished.push(cid.clone());
                    pin_complete = true;
                }
                PinProgress::Error(e) => {
                    tracing::error!("CID {} pin error: {}", &cid[0..16], e);
                    let _ = pool.record_failure(Some(cid), "pin", &format!("{:?}", e));
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
                PinProgress::Raw(line) => {
                    tracing::debug!("CID {} raw: {}", &cid[0..16], line);
                }
            }
            updates.insert(cid.to_owned(), p);
        }
    }

    // 2) Persist progress in DB
    let _ = pool.update_progress(&updates)?;

    // 3) Stalled pins based on DB timestamps
    let stall = pool.stalled_pins()?;

    let mut stalled_to_cancel = Vec::new();
    for (cid, _task) in active.iter_mut() {
        let cid_clone = cid.clone();

        if !stalled_pins.lock().await.contains(&cid_clone) && stall.contains(&cid_clone) {
            stalled_pins.lock().await.insert(cid_clone.clone());
            stalled_to_cancel.push(cid_clone.clone());
            tracing::debug!("Stalled progress {}", cid_clone);

            let notifier = notifier.clone();
            let notif_state_t = notif_state.clone();

            tokio::spawn(async move {
                notif_state_t
                    .lock()
                    .await
                    .notify_change(
                        &notifier,
                        format!("Stalled progress {}", cid_clone),
                        false,
                        "unused",
                        &format!("Task {} stalling - will be cancelled", cid_clone),
                    )
                    .await;
            });
        }
    }

    // Add stalled + errored CIDs to skiplist
    if !stalled_to_cancel.is_empty() || !errored.is_empty() {
        let mut skip = skip_pins.lock().await;
        for cid in &stalled_to_cancel {
            skip.insert(cid.clone());
        }
        for cid in &errored {
            skip.insert(cid.clone());
        }
    }

    let mut some_stalled_or_errored = false;

    // 4) Cancel stalled tasks
    tracing::info!("Cancelling {} stalled tasks", stalled_to_cancel.len());
    for cid in stalled_to_cancel {
        some_stalled_or_errored = true;
        tracing::info!("Cancelling stalled task: {}", &cid[0..16]);
        if let Some(task) = active.remove(&cid) {
            let _ = task.cancel_tx.send(());
        }
        // Permit is released when ActiveTask (and its OwnedSemaphorePermit) is dropped.
    }

    // 5) Cancel errored tasks
    for cid in errored {
        some_stalled_or_errored = true;
        if let Some(task) = active.remove(&cid) {
            let _ = task.cancel_tx.send(());
        }
    }

    // 6) Per-CID verify for just-completed pins
    for cid in finished {
        let ipfs = ipfs.clone();
        let pool = pool.clone();
        let active_pins = active_pins.clone();
        let notifier = notifier.clone();
        let notif_state = notif_state.clone();
        let skip_pins = skip_pins.clone();

        tokio::spawn(async move {
            match ipfs.pin_ls_single(&cid).await {
                Ok(true) => {
                    // Verified OK, nothing to do. ActiveTask stays in map so we don't repin.
                }
                Ok(false) => {
                    tracing::warn!(
                        "CID {} reported complete but not found in pin/ls result",
                        &cid[0..16]
                    );
                    let _ = pool.mark_incomplete(&cid);
                    let _ = pool.record_failure(
                        Some(&cid),
                        "verify",
                        "Completed pin not found in local pin list",
                    );

                    {
                        let mut active = active_pins.lock().await;
                        active.remove(&cid);
                    }

                    {
                        let mut skip = skip_pins.lock().await;
                        skip.insert(cid.clone());
                    }

                    notif_state
                        .lock()
                        .await
                        .notify_change(
                            &notifier,
                            format!("Completed pin {}", cid),
                            false,
                            "unused",
                            &format!("Pin {} no longer verifiable via pin/ls", cid),
                        )
                        .await;
                }
                Err(e) => {
                    tracing::error!("pin_ls_single failed for {}: {:?}", &cid[0..16], e);
                    let _ = pool.record_failure(Some(&cid), "verify", &format!("{:?}", e));

                    {
                        let mut active = active_pins.lock().await;
                        active.remove(&cid);
                    }

                    {
                        let mut skip = skip_pins.lock().await;
                        skip.insert(cid.clone());
                    }

                    notif_state
                        .lock()
                        .await
                        .notify_change(
                            &notifier,
                            format!("Completed pin {}", cid),
                            false,
                            "unused",
                            &format!("Pin {} verification error: {}", cid, e),
                        )
                        .await;
                }
            }
        });
    }

    // 7) Adaptive concurrency tuning
    let completed: HashSet<String> = pool.completed_pins()?;
    let mut any_ongoing = false;
    let mut all_ongoing_progressed = true;

    for cid in active.keys() {
        if completed.contains(cid) {
            continue; // Completed pins don't count as ongoing.
        }

        any_ongoing = true;
        match updates.get(cid) {
            Some(PinProgress::Blocks(_)) | Some(PinProgress::Done) => {
                // this CID made progress in this tick
            }
            _ => {
                all_ongoing_progressed = false;
                break;
            }
        }
    }

    drop(active); // release lock before touching semaphores

    if any_ongoing {
        if all_ongoing_progressed {
            extra_concurrency.add_permits(1);
        } else {
            *extra_concurrency = Arc::new(Semaphore::new(0));
        }
    }

    // 8) Metrics
    metrics::gauge!("active_pins").set(active_pins.lock().await.len() as f64);
    metrics::gauge!("stalled_pins").set(stalled_pins.lock().await.len() as f64);

    Ok(pin_complete || some_stalled_or_errored)
}

// Make test module visible when compiling tests from external file.
#[cfg(test)]
mod tests;
