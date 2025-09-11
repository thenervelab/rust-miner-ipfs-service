use anyhow::{Context, Result};
use futures::stream::{FuturesUnordered, StreamExt};
use sqlx::SqlitePool;
use std::{sync::Arc, time::Duration};
use tokio::{sync::Notify, time};

use crate::notifier::MultiNotifier;

use crate::{
    db,
    disk::disk_usage,
    ipfs::Client as Ipfs,
    model::{FileInfo, PinState},
    settings::Settings,
    substrate::Chain,
};

pub async fn run(cfg: Settings, pool: SqlitePool, notifier: MultiNotifier) -> Result<()> {
    let shutdown = Arc::new(Notify::new());
    {
        let s = shutdown.clone();
        ctrlc::set_handler(move || {
            s.notify_waiters();
        })
        .expect("ctrlc");
    }

    let chain = Chain::connect(&cfg.substrate.ws_url).await?;

    let ipfs = Ipfs::new(cfg.ipfs.api_url.clone());

    let mut poll = time::interval(Duration::from_secs(cfg.service.poll_interval_secs));
    let mut reconcile = time::interval(Duration::from_secs(cfg.service.reconcile_interval_secs));
    let mut gc = time::interval(Duration::from_secs(cfg.service.ipfs_gc_interval_secs));
    let mut health_check =
        time::interval(Duration::from_secs(cfg.service.conn_check_interval_secs));

    loop {
        tokio::select! {
            _ = shutdown.notified() => {
                tracing::info!("shutdown");
                break;
            }
            _ = health_check.tick()=> {
                if let Err(e) = ipfs.check_health().await {
                    tracing::error!("IPFS health check failed: {:?}", e);
                    notifier.notify_all("IPFS node connectivity check failed", &format!("IPFS node connectivity check failed: {}", e)).await;
                }
                if let Err(e) = chain.check_health().await {
                    tracing::error!("Substrate health check failed: {:?}", e);
                    notifier.notify_all("Blockchain RPC node connectivity check failed", &format!("Blockchain RPC node connectivity check failed: {}", e)).await;
                }
            }
            _ = poll.tick() => { if let Err(e) = update_profile_cid(&cfg, &pool, &chain).await {
                    tracing::warn!(error=?e, "update_profile_cid_failed");
                    notifier.notify_all("Profile update failed", &format!("{}", e)).await;
                }
            }
            _ = reconcile.tick() => {
                if let Err(e) = reconcile_once(&cfg, &pool, &notifier).await {
                    tracing::error!(error=?e, "reconcile_failed");
                    notifier.notify_all("Profile reconcile failed", &format!("{}", e)).await;
                }
            }
            _ = gc.tick() => {
                if let Err(e) = ipfs.gc().await {
                    tracing::error!(error=?e, "gc_failed");
                    notifier.notify_all("IPFS GC failed", &format!("{}", e)).await;
                }
            }
        }
    }

    Ok(())
}

pub async fn update_profile_cid(cfg: &Settings, pool: &SqlitePool, chain: &Chain) -> Result<()> {
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
) -> Result<()> {
    let ipfs = Ipfs::new(cfg.ipfs.api_url.clone());

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

    // Mark all unseen, then mark listed CIDs as seen
    db::mark_all_unseen(pool).await?;

    let desired: Vec<String> = profile.iter().map(|p| p.cid.trim().to_string()).collect();

    // Pin new/ensure present
    let sem = Arc::new(tokio::sync::Semaphore::new(
        cfg.service.max_concurrent_ipfs_ops as usize,
    ));
    let mut tasks = FuturesUnordered::new();
    for cid in desired.iter() {
        let cid = cid.clone();
        let ipfs = ipfs.clone();
        let pool = pool.clone();
        let sem_c = sem.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = sem_c.acquire().await.unwrap();

            let res: Result<()> = async {
                match ipfs.pin_add(&cid).await.context("pin_add") {
                    Ok(()) => {}
                    Err(e) => {
                        println!("Pin attempt failure {}", e);
                    }
                }

                db::record_pin(&pool, &cid, true).await?;
                db::mark_seen(&pool, &cid).await?;
                Ok(())
            }
            .await;

            if let Err(e) = &res {
                let _ = db::record_failure(&pool, Some(&cid), "pin", &format!("{:?}", e)).await;
            }

            res
        }));
    }

    while let Some(res) = tasks.next().await {
        if let Err(e) = res {
            tracing::warn!(?e, "task_join_err");
        }
    }

    // Unpin removed
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
                let e = match p.err {
                    Some(e) => e,
                    _ => "unknown".to_string(),
                };
                println!("Problem with pinned CID: {}, Error: {}", p.cid, e);
                notifier
                    .notify_all(
                        "Problem with pinned CID",
                        &format!("CID: {} error: {}", p.cid, e),
                    )
                    .await;
            }
        }
        _ => {}
    };

    let (disks, program_location_disk_usage) = match disk_usage() {
        Ok((v, f)) => (v, f),
        _ => (vec![], 404.0),
    };

    for i in 0..disks.len() {
        let available = disks[i].0 as f64 / disks[i].1 as f64 * 100.0;

        if available < 50.0 {
            let gb: f64 = disks[i].1 as f64 / (1024.0 * 1024.0 * 1024.0);
            println!("Disk {} available space: {}%", i, available);
            notifier
                .notify_all(
                    &format!("Low disk space on disk {}", i),
                    &format!(
                        "Disk {} available space left: {:.2}% of {:.2} GB",
                        i, available, gb
                    ),
                )
                .await;
        };
    }

    Ok(())
}
