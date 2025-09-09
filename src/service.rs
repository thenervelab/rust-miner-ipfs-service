use anyhow::{Context, Result};
use backoff::{ExponentialBackoff, future::retry};
use futures::stream::{FuturesUnordered, StreamExt};
use sqlx::SqlitePool;
use std::{sync::Arc, time::Duration};
use tokio::{sync::Notify, time};

use crate::{db, ipfs::Client as Ipfs, model::Profile, settings::Settings, substrate::Chain};

pub async fn run(cfg: Settings, pool: SqlitePool) -> Result<()> {
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

    loop {
        tokio::select! {
            _ = shutdown.notified() => {
                tracing::info!("shutdown");
                break;
            }
            _ = poll.tick() => {
                if let Err(e) = update_profile_cid(&cfg, &pool, &chain).await { tracing::warn!(error=?e, "update_profile_cid_failed"); }
            }
            _ = reconcile.tick() => {
                if let Err(e) = reconcile_once(&cfg, &pool).await { tracing::error!(error=?e, "reconcile_failed"); }
            }
            _ = gc.tick() => {
                if let Err(e) = ipfs.gc().await { tracing::error!(error=?e, "gc_failed"); }
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

pub async fn reconcile_once(cfg: &Settings, pool: &SqlitePool) -> Result<()> {
    let ipfs = Ipfs::new(cfg.ipfs.api_url.clone());

    let cid_opt = db::get_profile(pool).await?;
    let Some(profile_cid) = cid_opt else {
        tracing::warn!("no_profile_cid_set_yet");
        return Ok(());
    };

    // Fetch profile JSON from IPFS
    let profile: Profile = retry(backoff(&cfg), || async {
        let p = ipfs
            .cat_json::<Profile>(&profile_cid)
            .await
            .context("fetch_profile")?;
        Ok(p)
    })
    .await?;

    tracing::info!(pins = profile.pin.len(), "profile_loaded");

    // Mark all unseen, then mark listed CIDs as seen
    db::mark_all_unseen(pool).await?;

    let desired: Vec<String> = profile
        .pin
        .iter()
        .map(|p| p.cid.trim().to_string())
        .collect();

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
            let res: Result<()> = retry(backoff_default(), || async {
                ipfs.pin_add(&cid).await.context("pin_add")?;
                db::record_pin(&pool, &cid, true).await?;
                db::mark_seen(&pool, &cid).await?;
                Ok(())
            })
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
            let res: Result<()> = retry(backoff_default(), || async {
                ipfs.pin_rm(&cid).await.context("pin_rm")?;
                db::delete_cid(&pool, &cid).await?;
                Ok(())
            })
            .await;
            if let Err(e) = &res {
                let _ = db::record_failure(&pool, Some(&cid), "unpin", &format!("{:?}", e)).await;
            }
            if let Err(e) = res {
                tracing::error!(?e, cid, "unpin_failed");
            }
        });
    }

    Ok(())
}

fn backoff(cfg: &Settings) -> ExponentialBackoff {
    let mut b = backoff_default();
    b.max_elapsed_time = Some(Duration::from_secs(cfg.service.retry_max_elapsed_secs));
    b
}

fn backoff_default() -> ExponentialBackoff {
    let mut b = ExponentialBackoff::default();
    b.initial_interval = Duration::from_millis(500);
    b.max_interval = Duration::from_secs(10);
    b.multiplier = 1.8;
    b.randomization_factor = 0.15;
    b
}
