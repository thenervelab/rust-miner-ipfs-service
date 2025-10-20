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
    disk::disk_usage,
    ipfs::Client as Ipfs,
    ipfs::IpfsClient,
    model::{FileInfo, PinState},
    notifier::MultiNotifier,
    settings::Settings,
    substrate::Chain,
};

use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};
use metrics_exporter_prometheus::PrometheusBuilder;

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

    if let Some(port) = cfg.monitoring.metrics_port.or(Some(9464)) {
        PrometheusBuilder::new()
            .with_http_listener(([0, 0, 0, 0], port))
            // you can also .add_global_label("service", "rust-miner-ipfs-service")
            .install() // sets global recorder + starts the HTTP listener
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

    let concurrency = Arc::new(Semaphore::new(cfg.service.initial_pin_concurrency));

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
            };

            if let Err(e) = crate::monitoring::run_health_server(cfg).await {
                tracing::error!("Monitoring server failed: {:?}", e);
            }
        });
    }

    tracing::info!("Commencing node operation");

    let mut poll = time::interval(Duration::from_secs(cfg.service.poll_interval_secs));
    poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
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
                        let started = std::time::Instant::now();
                        if let Err(e) = reconcile_once(&pool, &ipfs, &notifier, &notif_state,
                                &active_pins,
                                &pending_pins,
                                &concurrency,
                                disk_usage,
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
                        if let Err(e) = update_progress_cid(&pool, &ipfs, &notifier, &notif_state, active_pins.clone(),  stalled_pins.clone(), concurrency.clone()).await {
                            tracing::error!(error=?e, "progress update error");
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

pub async fn reconcile_once<P, C, F>(
    pool: &Arc<P>,
    ipfs: &Arc<C>,
    notifier: &Arc<MultiNotifier>,
    notif_state: &Arc<Mutex<NotifState>>,
    active_pins: &Arc<Mutex<HashMap<String, ActiveTask>>>,
    pending_pins: &PinSet,
    concurrency: &Arc<Semaphore>,
    disk_fn: F,
) -> Result<()>
where
    P: PoolTrait + 'static,
    C: IpfsClient + 'static,
    F: Fn() -> (Vec<(u64, u64)>, f64),
{
    tracing::info!("Reconcile commenced");

    let cid_opt = pool.get_profile()?;
    let Some(profile_cid) = cid_opt else {
        tracing::warn!("no_profile_cid_set_yet");
        return Ok(());
    };

    let profile: Vec<FileInfo> = ipfs
        .cat::<Vec<FileInfo>>(&profile_cid)
        .await
        .context("Failed to get miner profile from IPFS")?;

    tracing::info!(pins = profile.len(), "profile_loaded");

    let pin_list = ipfs.pin_ls_all().await?;
    let _ = pool.merge_pins(&pin_list); // all top level pins are added to database to keep track of unnecessary pins

    let desired: Vec<String> = profile.iter().map(|p| p.cid.trim().to_string()).collect();

    for cid in desired.iter() {
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

    if let Ok(list) = pin_state_errors {
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
                    .notify_change(notifier, cidm, false, &okm, &errm)
                    .await;
            } else {
                notif_state
                    .lock()
                    .await
                    .notify_change(notifier, cidm, true, &okm, &errm)
                    .await;
            };
        }
    };

    let (disks, ipfs_location_disk_usage) = disk_fn();
    metrics::gauge!("ipfs_disk_available_percent").set(ipfs_location_disk_usage);

    for (_i, disk) in disks.iter().enumerate() {
        let available = disk.0 as f64 / disk.1 as f64 * 100.0;

        let gb: f64 = disk.1 as f64 / (1024.0 * 1024.0 * 1024.0);

        let diskm = format!("disk_space_for_ipfs");
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
                "Available space on disk used for IPFS storage: {}%",
                available
            );

            notif_state
                .lock()
                .await
                .notify_change(notifier, diskm, false, &okm, &errm)
                .await;
        } else {
            tracing::info!(
                "Available space on disk used for IPFS storage: {}%",
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
            format!("cant_find_ipfs_disk"),
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

    false
}

pub async fn update_progress_cid<P, C>(
    pool: &Arc<P>,
    ipfs: &Arc<C>,
    notifier: &Arc<MultiNotifier>,
    notif_state: &Arc<Mutex<NotifState>>,
    active_pins: Arc<Mutex<HashMap<String, ActiveTask>>>,
    stalled_pins: PinSet,
    concurrency: Arc<Semaphore>,
) -> Result<()>
where
    P: PoolTrait + 'static,
    C: IpfsClient + 'static,
{
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
                }
                PinProgress::Error(ref e) => {
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

    let completed: HashSet<String> = pool.completed_pins()?;
    if !completed.is_empty() {
        match ipfs.pin_verify().await {
            Ok(pin_states) => {
                let verified: HashSet<String> = pin_states
                    .into_iter()
                    .filter(|p| p.ok)
                    .map(|p| p.cid)
                    .collect();

                for cid in completed.difference(&verified) {
                    if let Some(task) = active.remove(cid) {
                        let _ = task.cancel_tx.send(());
                    }

                    let _ = pool.mark_incomplete(cid);

                    let _ = pool.record_failure(
                        Some(cid),
                        "verify",
                        "Completed pin not verifiable on IPFS",
                    );

                    let notifier = notifier.clone();
                    let cid = cid.clone();
                    let notif_state_t = notif_state.clone();
                    tokio::spawn(async move {
                        notif_state_t
                            .lock()
                            .await
                            .notify_change(
                                &notifier,
                                format!("Completed pin {}", cid),
                                false,
                                "unused",
                                &format!("Pin {} no longer verifiable", cid),
                            )
                            .await;
                    });
                }
            }
            Err(e) => {
                tracing::error!("pin_verify failed: {:?}", e);
            }
        }
    }

    metrics::gauge!("active_pins").set(active.len() as f64);
    metrics::gauge!("stalled_pins").set(stalled_pins.lock().await.len() as f64);

    Ok(())
}

#[cfg(test)]
mod tests;
