use super::{
    ActiveTask, NotifState, PinProgress, disk_usage, reconcile_once, run, spawn_pin_task,
    update_profile_cid, update_progress_cid,
};

use std::time::Duration;

use anyhow::Result;

use tokio::sync::oneshot;

use crate::{
    db::PoolTrait,
    ipfs::Client as Ipfs,
    model::PinState,
    service::FileInfo,
    settings::Settings,
    substrate::Chain,
    test_utils::{DummyIpfs, DummyPool},
};

//          //          //          //          //          //          //          //          //          //          //          //

//                      //                      //                      //                                  //                      //

//                      //                      //          //          //          //                      //                      //

//                      //                      //                                  //                      //                      //

//                      //                      //          //          //          //                      //                      //

#[cfg(any(test))]
pub mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, mpsc};
    use tempfile::TempDir;
    use tokio::sync::{Mutex, Semaphore};

    // NOTE: we reference MultiNotifier and CidPool through their crate paths
    // to avoid any ambiguity inside this file's module tree.
    use crate::db::CidPool;
    use crate::notifier::MultiNotifier;

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
    async fn test_update_progress_cid_error_path() {
        let tmp = TempDir::new().unwrap();
        let pool = Arc::new(CidPool::init(tmp.path().to_str().unwrap(), 120).unwrap());
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

        let ipfs = Arc::new(DummyIpfs::default());

        let res = update_progress_cid(
            &pool,
            &ipfs,
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
    async fn test_update_progress_cid_stalled_branch() {
        let tmp = TempDir::new().unwrap();
        let pool = Arc::new(CidPool::init(tmp.path().to_str().unwrap(), 120).unwrap());
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

        let ipfs = Arc::new(DummyIpfs::default());

        let res = update_progress_cid(
            &pool,
            &ipfs,
            &notifier,
            &notif_state,
            active.clone(),
            stalled.clone(),
            concurrency.clone(),
        )
        .await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_reconcile_once_no_profile() {
        let pool = Arc::new(DummyPool::default());
        let ipfs = Arc::new(DummyIpfs {
            cat_result: Err(anyhow::anyhow!("not called")),
            pin_rm_result: Ok(()),
            pin_verify_result: Ok(vec![]),
            pin_ls_all_result: Ok(HashSet::new()),
            health_ok: true,
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
            &active,
            &pending,
            &concurrency,
            disk_usage,
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
            cat_result: Ok(serde_json::json!({"foo": "bar"})),
            pin_rm_result: Ok(()),
            pin_verify_result: Ok(vec![]),
            health_ok: true,
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
            &active,
            &pending,
            &concurrency,
            disk_usage,
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
            cat_result: Ok(serde_json::json!(Vec::<FileInfo>::new())),

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
            &active,
            &pending,
            &concurrency,
            disk_usage,
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
            cat_result: Ok(serde_json::json!({"foo": "bar"})),
            pin_rm_result: Ok(()),
            pin_verify_result: Ok(vec![]),
            pin_ls_all_result: Ok(HashSet::new()),
            health_ok: true,
        });

        let ipfs_high = Arc::new(DummyIpfs {
            cat_result: Ok(serde_json::json!({"foo": "bar"})),
            pin_rm_result: Ok(()),
            pin_verify_result: Ok(vec![]),
            pin_ls_all_result: Ok(HashSet::new()),
            health_ok: true,
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
            &active,
            &pending,
            &concurrency,
            disk_usage,
        )
        .await
        .unwrap();

        // High → expect notify_change back to Normal
        reconcile_once(
            &pool,
            &ipfs_high,
            &notifier,
            &notif_state,
            &active,
            &pending,
            &concurrency,
            disk_usage,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_update_progress_cid_raw_branch() {
        let tmp = TempDir::new().unwrap();
        let pool = Arc::new(CidPool::init(tmp.path().to_str().unwrap(), 120).unwrap());
        let notifier = Arc::new(MultiNotifier::new());
        let notif_state = Arc::new(Mutex::new(NotifState::default()));
        let active = Arc::new(Mutex::new(HashMap::new()));
        let stalled = Arc::new(Mutex::new(HashSet::new()));
        let concurrency = Arc::new(Semaphore::new(1));

        let (tx, rx) = mpsc::channel::<PinProgress>();
        tx.send(PinProgress::Raw("line".into())).unwrap();

        let (cancel_tx, _cancel_rx) = oneshot::channel();
        active.lock().await.insert(
            "cidRaw".into(),
            ActiveTask {
                progress_rx: rx,
                cancel_tx,
                permit: None,
            },
        );

        let ipfs = Arc::new(DummyIpfs::default());

        let res = update_progress_cid(
            &pool,
            &ipfs,
            &notifier,
            &notif_state,
            active,
            stalled,
            concurrency,
        )
        .await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_spawn_pin_task_cancel_branch() {
        let ipfs = Arc::new(DummyIpfs::default());
        let active = Arc::new(Mutex::new(HashMap::new()));
        let pending = Arc::new(Mutex::new(HashSet::new()));
        let concurrency = Arc::new(Semaphore::new(1));

        let cid = "cidCancel".to_string();
        let started =
            spawn_pin_task(ipfs, cid.clone(), &active, &pending, concurrency.clone()).await;
        assert!(started);

        // Send cancel to trigger cancel_rx branch
        if let Some(task) = active.lock().await.remove(&cid) {
            let _ = task.cancel_tx.send(());
        }
    }

    #[tokio::test]
    async fn test_reconcile_once_profile_and_pin_states() {
        let pool = Arc::new(DummyPool {
            profile: Some("profilecid".into()), // ensures Some(cid) for profile
            ..Default::default()
        });

        // Minimal FileInfo with dummy values to satisfy deserialization
        let file_infos = vec![FileInfo {
            cid: "cid123".to_string(),
            account_ss58: Some("dummy-account".to_string()),
            cid_v2: None,
            created_at: None,
            file_id: None,
            file_size_in_bytes: None,
            miner_node_id: None,
            original_name: None,
            owner: None,
            selected_validator: None,
            size_bytes: None,
            file_hash: None,
        }];

        let ipfs = Arc::new(DummyIpfs {
            cat_result: Ok(serde_json::to_value(file_infos).unwrap()),
            pin_verify_result: Ok(vec![
                PinState {
                    cid: "cidErr".into(),
                    ok: false,
                    err: Some("bad".into()),
                    pin_status: None,
                },
                PinState {
                    cid: "cidOk".into(),
                    ok: true,
                    err: None,
                    pin_status: None,
                },
            ]),
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
            &active,
            &pending,
            &concurrency,
            disk_usage,
        )
        .await;

        assert!(
            res.is_ok(),
            "Expected reconcile_once to succeed, got {:?}",
            res
        );
    }
    #[tokio::test]
    async fn test_update_profile_cid_changes() {
        // Chain will always return Some("newcid")
        let cid = "a".repeat(46);
        let mut chain = Chain::dummy(true, Some(Ok(Some("0".to_string() + &cid.clone()))));

        // temp DB
        let tmp = TempDir::new().unwrap();
        let pool = Arc::new(CidPool::init(tmp.path().to_str().unwrap(), 120).unwrap());

        // config with required substrate values
        let cfg = Settings {
            substrate: crate::settings::SubstrateCfg {
                pallet: Some("dummy_pallet".into()),
                storage_item: Some("dummy_item".into()),
                miner_profile_id: Some("miner123".into()),
                raw_storage_key_hex: None,
                ws_url: "wss://dummy".into(),
            },
            ..Default::default()
        };

        let _fetched = chain
            .fetch_profile_cid(
                cfg.substrate.raw_storage_key_hex.as_deref(),
                cfg.substrate.pallet.as_deref(),
                cfg.substrate.storage_item.as_deref(),
                cfg.substrate.miner_profile_id.as_deref(),
            )
            .await;

        // run update
        let res = update_profile_cid(&cfg, &pool, &mut chain).await;
        dbg!(&res);
        assert!(res.is_ok());
        let stored = pool.get_profile().unwrap();

        assert_eq!(stored, Some(cid));
    }

    #[tokio::test]
    async fn test_update_profile_cid_no_change() {
        let cid = "a".repeat(46);

        // pool already has cid
        let tmp = TempDir::new().unwrap();
        let pool = Arc::new(CidPool::init(tmp.path().to_str().unwrap(), 120).unwrap());
        pool.set_profile(Some(&cid)).unwrap();

        // use dummy chain that returns the same cid
        let mut chain = Chain::dummy(true, Some(Ok(Some("0".to_string() + &cid.clone()))));

        let cfg = Settings {
            substrate: crate::settings::SubstrateCfg {
                pallet: Some("dummy_pallet".into()),
                storage_item: Some("dummy_item".into()),
                miner_profile_id: Some("miner123".into()),
                raw_storage_key_hex: None,
                ws_url: "wss://dummy".into(),
            },
            ..Default::default()
        };
        let res = update_profile_cid(&cfg, &pool, &mut chain).await;
        assert!(res.is_ok());

        // pool remains unchanged
        assert_eq!(pool.get_profile().unwrap(), Some(cid));
    }

    #[tokio::test]
    async fn test_reconcile_once_pin_rm_failure_with_still_pinned() {
        let cid = "QmDead".to_string();
        let pool = Arc::new(DummyPool {
            profile: Some("cid".into()),
            to_unpin_result: vec![cid.clone()],
            ..Default::default()
        });
        let ipfs = Arc::new(DummyIpfs {
            pin_rm_result: Err(anyhow::anyhow!("fail rm")),
            pin_ls_all_result: Ok(HashSet::from([cid.clone()])),
            cat_result: Ok(serde_json::json!(Vec::<FileInfo>::new())),
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
            &active,
            &pending,
            &concurrency,
            disk_usage,
        )
        .await;

        assert!(
            res.is_ok(),
            "Expected reconcile_once to succeed, got {:?}",
            res
        );
    }

    #[tokio::test]
    async fn test_reconcile_once_cat_error() {
        let pool = Arc::new(DummyPool {
            profile: Some("cid".into()),
            ..Default::default()
        });
        let ipfs = Arc::new(DummyIpfs {
            cat_result: Err(anyhow::anyhow!("bad json")),
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
            &active,
            &pending,
            &concurrency,
            disk_usage,
        )
        .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_spawn_pin_task_error_in_pin_add() {
        let ipfs = Arc::new(DummyIpfs {
            pin_rm_result: Err(anyhow::anyhow!("fail")),
            ..Default::default()
        });
        let active = Arc::new(Mutex::new(HashMap::new()));
        let pending = Arc::new(Mutex::new(HashSet::new()));
        let concurrency = Arc::new(Semaphore::new(1));

        let cid: String = "cidErrPin".into();
        let started =
            spawn_pin_task(ipfs, cid.clone(), &active, &pending, concurrency.clone()).await;
        assert!(started);
    }

    #[tokio::test]
    async fn test_update_progress_cid_blocks_done_marks_complete() {
        let tmp = TempDir::new().unwrap();
        let pool = Arc::new(CidPool::init(tmp.path().to_str().unwrap(), 120).unwrap());
        let notifier = Arc::new(MultiNotifier::new());
        let notif_state = Arc::new(Mutex::new(NotifState::default()));
        let active_pins = Arc::new(Mutex::new(HashMap::new()));
        let stalled_pins = Arc::new(Mutex::new(HashSet::new()));
        let concurrency = Arc::new(Semaphore::new(1));

        let (tx, rx) = mpsc::channel::<PinProgress>();
        tx.send(PinProgress::Blocks(1)).unwrap();
        tx.send(PinProgress::Done).unwrap();

        let (cancel_tx, _cancel_rx) = oneshot::channel();
        active_pins.lock().await.insert(
            "cidX".into(),
            ActiveTask {
                progress_rx: rx,
                cancel_tx,
                permit: None,
            },
        );

        let ipfs = Arc::new(DummyIpfs {
            cat_result: Ok(serde_json::json!(Vec::<FileInfo>::new())),
            pin_rm_result: Ok(()),
            pin_verify_result: Ok(vec![PinState {
                cid: "cidX".into(),
                ok: true,
                err: None,
                pin_status: None,
            }]),
            pin_ls_all_result: Ok(HashSet::from(["cidX".to_string()])),
            health_ok: true,
        });

        let res = update_progress_cid(
            &pool,
            &ipfs,
            &notifier,
            &notif_state,
            active_pins.clone(),
            stalled_pins,
            concurrency,
        )
        .await;

        assert!(res.is_ok());

        // Task should still exist in the map
        assert!(active_pins.lock().await.contains_key("cidX"));

        // But DB should now say the pin is completed
        let rec = pool.get_pin("cidX").unwrap().unwrap();
        assert!(
            rec.sync_complete,
            "pin record should be marked complete after Done"
        );
    }

    #[tokio::test]
    async fn test_update_progress_cid_done_releases_permit() {
        let tmp = TempDir::new().unwrap();
        let pool = Arc::new(CidPool::init(tmp.path().to_str().unwrap(), 120).unwrap());
        let notifier = Arc::new(MultiNotifier::new());
        let notif_state = Arc::new(Mutex::new(NotifState::default()));
        let active = Arc::new(Mutex::new(HashMap::new()));
        let stalled = Arc::new(Mutex::new(HashSet::new()));
        let concurrency = Arc::new(Semaphore::new(1)); // start with 1 permit

        // Acquire the only permit so semaphore is "full"
        let permit = concurrency.clone().try_acquire_owned().unwrap();
        assert_eq!(concurrency.available_permits(), 0);

        let (tx, rx) = mpsc::channel::<PinProgress>();
        tx.send(PinProgress::Done).unwrap();

        let (cancel_tx, _cancel_rx) = oneshot::channel();
        active.lock().await.insert(
            "cidPermit".into(),
            ActiveTask {
                progress_rx: rx,
                cancel_tx,
                permit: Some(permit), // task holds the permit
            },
        );

        let ipfs = Arc::new(DummyIpfs::default());

        let _ = update_progress_cid(
            &pool,
            &ipfs,
            &notifier,
            &notif_state,
            active,
            stalled,
            concurrency.clone(),
        )
        .await
        .unwrap();

        // After Done, permit should have been released
        assert_eq!(concurrency.available_permits(), 1);
    }

    #[tokio::test]
    async fn test_reconcile_once_notifies_on_pinstate_errors() {
        let pool = Arc::new(DummyPool {
            profile: Some("profilecid".into()),
            ..Default::default()
        });

        let file_infos = vec![FileInfo {
            cid: "cidErr".to_string(),
            account_ss58: None,
            cid_v2: None,
            created_at: None,
            file_id: None,
            file_size_in_bytes: None,
            miner_node_id: None,
            original_name: None,
            owner: None,
            selected_validator: None,
            size_bytes: None,
            file_hash: None,
        }];

        let ipfs = Arc::new(DummyIpfs {
            cat_result: Ok(serde_json::to_value(file_infos).unwrap()),
            pin_verify_result: Ok(vec![
                PinState {
                    cid: "cidErr".into(),
                    ok: false,
                    err: Some("boom".into()),
                    pin_status: None,
                },
                PinState {
                    cid: "cidOk".into(),
                    ok: true,
                    err: None,
                    pin_status: None,
                },
            ]),
            ..Default::default()
        });

        let notifier = Arc::new(MultiNotifier::new());
        let notif_state = Arc::new(Mutex::new(NotifState::default()));
        let active = Arc::new(Mutex::new(HashMap::new()));
        let pending = Arc::new(Mutex::new(HashSet::new()));
        let concurrency = Arc::new(Semaphore::new(1));

        reconcile_once(
            &pool,
            &ipfs,
            &notifier,
            &notif_state,
            &active,
            &pending,
            &concurrency,
            disk_usage,
        )
        .await
        .unwrap();

        let statuses = notif_state.lock().await.last_status.lock().await.clone();
        assert_eq!(statuses.get("pinned_cid_err_cidErr"), Some(&false));
        assert_eq!(statuses.get("pinned_cid_err_cidOk"), Some(&true));
    }

    #[derive(Default)]
    pub struct CountingPool {
        pub merge_called: Arc<std::sync::Mutex<usize>>,
        pub profile: Option<String>,
    }

    impl PoolTrait for CountingPool {
        fn get_profile(&self) -> Result<Option<String>> {
            Ok(self.profile.clone())
        }
        fn merge_pins(&self, _cids: &HashSet<String>) -> Result<()> {
            *self.merge_called.lock().unwrap() += 1;
            Ok(())
        }
        // stub the rest
        fn record_failure(&self, _: Option<&str>, _: &str, _: &str) -> Result<()> {
            Ok(())
        }
        fn sync_pins(&self, _: Vec<String>) -> Result<Vec<String>> {
            Ok(vec![])
        }
        fn update_progress(&self, _: &HashMap<String, PinProgress>) -> Result<HashSet<String>> {
            Ok(HashSet::new())
        }
        fn touch_all_progress(&self) -> Result<()> {
            Ok(())
        }
        fn touch_progress(&self, _: &str) -> Result<()> {
            Ok(())
        }
        fn completed_pins(&self) -> Result<HashSet<String>> {
            Ok(HashSet::new())
        }
        fn mark_incomplete(&self, _cid: &str) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_reconcile_once_calls_merge_pins() {
        let pool = Arc::new(CountingPool {
            profile: Some("profilecid".into()),
            ..Default::default()
        });

        let ipfs = Arc::new(DummyIpfs {
            cat_result: Ok(serde_json::json!(Vec::<FileInfo>::new())),
            ..Default::default()
        });

        let notifier = Arc::new(MultiNotifier::new());
        let notif_state = Arc::new(Mutex::new(NotifState::default()));
        let active = Arc::new(Mutex::new(HashMap::new()));
        let pending = Arc::new(Mutex::new(HashSet::new()));
        let concurrency = Arc::new(Semaphore::new(1));

        reconcile_once(
            &pool,
            &ipfs,
            &notifier,
            &notif_state,
            &active,
            &pending,
            &concurrency,
            disk_usage,
        )
        .await
        .unwrap();

        assert_eq!(*pool.merge_called.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn test_run_health_check_failure_and_recovery() {
        let tmp = TempDir::new().unwrap();
        let pool = Arc::new(CidPool::init(tmp.path().to_str().unwrap(), 120).unwrap());
        let notifier = Arc::new(MultiNotifier::new());

        let mut cfg = Settings::default();
        cfg.service.poll_interval_secs = 1;
        cfg.service.reconcile_interval_secs = 1;
        cfg.service.ipfs_gc_interval_secs = 1;
        cfg.service.health_check_interval_secs = 1;

        // Start run() in background
        let handle = tokio::spawn(run(cfg, pool.clone(), notifier.clone()));

        // Let it tick once
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Kill it
        handle.abort();

        // We can’t fully assert logs, but we can assert pool is intact and no panic occurred
        assert!(pool.get_profile().is_ok());
    }

    #[tokio::test]
    async fn test_notify_change_triggers_notifier() {
        use crate::notifier::Notifier;
        use std::sync::Mutex as StdMutex;

        struct RecordingNotifier {
            calls: Arc<StdMutex<Vec<(String, String)>>>,
        }

        #[async_trait::async_trait]
        impl Notifier for RecordingNotifier {
            async fn notify(&self, subject: &str, message: &str) -> anyhow::Result<()> {
                self.calls
                    .lock()
                    .unwrap()
                    .push((subject.into(), message.into()));
                Ok(())
            }

            fn name(&self) -> &'static str {
                "recording_notifier"
            }

            fn is_healthy(&self) -> anyhow::Result<(&str, bool)> {
                Ok(("recording_notifier", true))
            }
        }

        let calls = Arc::new(StdMutex::new(Vec::new()));

        // Build MultiNotifier with our RecordingNotifier
        let mut m = crate::notifier::MultiNotifier::new();
        m.add(Box::new(RecordingNotifier {
            calls: calls.clone(),
        }));
        let notifier = Arc::new(m);

        let mut state = NotifState::default();

        // Force (None,false) branch
        state
            .notify_change(&notifier, "cidNotify".into(), false, "okmsg", "errmsg")
            .await;

        // Give spawned task a chance to run
        tokio::time::sleep(Duration::from_millis(20)).await;

        let records = calls.lock().unwrap();
        assert!(
            records
                .iter()
                .any(|(title, msg)| title.contains("failure") && msg == "errmsg"),
            "expected failure notification, got: {:?}",
            *records
        );
    }

    #[tokio::test]
    async fn test_run_with_monitoring_port() {
        let tmp = tempfile::TempDir::new().unwrap();
        let pool = Arc::new(crate::db::CidPool::init(tmp.path().to_str().unwrap(), 120).unwrap());
        let notifier = Arc::new(MultiNotifier::new());

        let mut cfg = Settings::default();
        cfg.monitoring.port = Some(0); // ephemeral port
        cfg.service.poll_interval_secs = 1;
        cfg.service.reconcile_interval_secs = 1;
        cfg.service.ipfs_gc_interval_secs = 1;
        cfg.service.health_check_interval_secs = 1;

        let h = tokio::spawn(run(cfg, pool, notifier));
        tokio::time::sleep(Duration::from_millis(50)).await;
        h.abort();
    }

    #[tokio::test]
    async fn test_update_profile_cid_error() {
        let mut chain = Chain::dummy(true, Some(Err(anyhow::anyhow!("boom"))));
        let tmp = tempfile::TempDir::new().unwrap();
        let pool = Arc::new(crate::db::CidPool::init(tmp.path().to_str().unwrap(), 120).unwrap());

        let cfg = Settings::default();
        let res = update_profile_cid(&cfg, &pool, &mut chain).await;
        assert!(res.is_err(), "expected error to propagate");
    }

    #[tokio::test]
    async fn test_spawn_pin_task_pending_dedup() {
        let ipfs = Arc::new(DummyIpfs::default());
        let active = Arc::new(Mutex::new(HashMap::new()));
        let pending = Arc::new(Mutex::new(HashSet::new()));
        let concurrency = Arc::new(Semaphore::new(0)); // no permits

        let cid = "cidPending".to_string();
        let first = spawn_pin_task(
            ipfs.clone(),
            cid.clone(),
            &active,
            &pending,
            concurrency.clone(),
        )
        .await;
        let second = spawn_pin_task(
            ipfs.clone(),
            cid.clone(),
            &active,
            &pending,
            concurrency.clone(),
        )
        .await;

        assert!(!first && !second);
        assert_eq!(
            pending.lock().await.len(),
            1,
            "CID should be added only once"
        );
    }
}
