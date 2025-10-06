use anyhow::Result;
use bincode::{
    config,
    serde::{decode_from_slice, encode_to_vec},
};
use parity_db::{Db, Options};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::PathBuf,
};

use crate::service::PinProgress;

pub trait PoolTrait: Send + Sync {
    fn sync_pins(&self, cids: Vec<String>) -> Result<Vec<String>>;
    fn merge_pins(&self, cids: &HashSet<String>) -> Result<()>;
    #[allow(dead_code)]
    fn update_progress(&self, updates: &HashMap<String, PinProgress>) -> Result<HashSet<String>>;
    #[allow(dead_code)]
    fn touch_all_progress(&self) -> Result<()>;
    fn get_profile(&self) -> Result<Option<String>>;
    fn touch_progress(&self, cid: &str) -> Result<()>;
    fn record_failure(&self, cid: Option<&str>, action: &str, error: &str) -> Result<()>;
    fn completed_pins(&self) -> Result<HashSet<String>>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PinRecord {
    pub last_progress: u64,
    pub last_progress_at: u64,
    pub total_blocks: u64,
    pub sync_complete: bool,
}

pub struct CidPool {
    db: parity_db::Db,
}

impl CidPool {
    /// Initialize the database with 3 columns:
    /// 0 = pins, 1 = service profile, 2 = failures
    pub fn init(path: &str) -> Result<Self> {
        let mut db_path = PathBuf::from(path);

        // If the path looks like a file (has an extension), normalize to a directory
        if db_path.extension().is_some() {
            let mut new_path = db_path.clone();
            let ext = new_path.extension().unwrap().to_string_lossy().into_owned();
            new_path.set_extension(""); // strip extension
            let dir_name = format!("{}_dir", ext);
            db_path = new_path.with_file_name(format!(
                "{}_{}",
                new_path.file_name().unwrap().to_string_lossy(),
                dir_name
            ));
        }

        // Ensure parent dir exists
        if let Some(parent) = db_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut opts = Options::with_columns(&db_path, 3);

        opts.columns[0].btree_index = true; // pins → need iteration
        opts.columns[1].btree_index = false; // profile → single key only
        opts.columns[2].btree_index = true; // failures → useful to iterate logs

        // Try to open existing DB
        match Db::open(&opts) {
            Ok(db) => Ok(Self { db }),
            Err(e) => {
                tracing::warn!("Failed to open DB at {:?}: {}. Recreating...", db_path, e);

                if db_path.exists() {
                    fs::remove_dir_all(&db_path)?;
                }
                fs::create_dir_all(&db_path)?;

                let mut opts = Options::with_columns(&db_path, 3);

                opts.columns[0].btree_index = true; // pins → need iteration
                opts.columns[1].btree_index = false; // profile → single key only
                opts.columns[2].btree_index = true; // failures → useful to iterate logs

                let db = Db::open_or_create(&opts)?;
                Ok(Self { db })
            }
        }
    }

    pub fn put_pin(&self, cid: &str, rec: &PinRecord) -> Result<()> {
        let key = cid.as_bytes();
        let value = encode_to_vec(rec, config::standard())?;
        self.db.commit([(0, key, Some(value))])?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn get_pin(&self, cid: &str) -> Result<Option<PinRecord>> {
        if let Some(val) = self.db.get(0, cid.as_bytes())? {
            let (rec, _len): (PinRecord, usize) = decode_from_slice(&val, config::standard())?;
            Ok(Some(rec))
        } else {
            Ok(None)
        }
    }

    pub fn del_pin(&self, cid: &str) -> Result<()> {
        self.db.commit([(0, cid.as_bytes(), None)])?;
        Ok(())
    }

    pub fn set_profile(&self, cid: Option<&str>) -> Result<()> {
        let value = cid.map(|c| c.as_bytes().to_vec());
        self.db.commit([(1, b"profile", value)])?;
        Ok(())
    }

    pub fn get_profile(&self) -> Result<Option<String>> {
        if let Some(val) = self.db.get(1, b"profile")? {
            Ok(Some(String::from_utf8(val)?))
        } else {
            Ok(None)
        }
    }

    pub fn record_failure(&self, cid: Option<&str>, action: &str, error: &str) -> Result<()> {
        #[derive(Serialize, Deserialize)]
        struct Failure {
            cid: Option<String>,
            action: String,
            error: String,
            occurred_at: i64,
        }

        let f = Failure {
            cid: cid.map(|c| c.to_string()),
            action: action.to_string(),
            error: error.to_string(),
            occurred_at: chrono::Utc::now().timestamp(),
        };

        let value = encode_to_vec(&f, config::standard())?;
        let key = format!("failure-{}", f.cid.clone().unwrap_or_default());

        self.db.commit([(2, key.as_bytes(), Some(value))])?;
        Ok(())
    }

    pub fn sync_pins(&self, cids: Vec<String>) -> Result<Vec<String>> {
        let desired: HashSet<String> = cids.into_iter().collect();

        // Collect all current pins
        let mut current = HashSet::new();
        let mut iter = self.db.iter(0)?;
        while let Some((key, _val)) = iter.next()? {
            let cid = String::from_utf8(key)?;
            current.insert(cid);
        }

        let to_add: Vec<String> = desired.difference(&current).cloned().collect();
        let to_del: Vec<String> = current.difference(&desired).cloned().collect();

        for cid in to_add {
            let rec = PinRecord {
                last_progress: 0_u64,
                last_progress_at: chrono::Utc::now().timestamp() as u64,
                total_blocks: 0,
                sync_complete: false,
            };
            self.put_pin(&cid, &rec)?;
        }

        let mut vec_unpin: Vec<String> = vec![];

        // Delete pins not in desired list
        for cid in to_del {
            self.del_pin(&cid)?;
            vec_unpin.push(cid);
        }

        Ok(vec_unpin)
    }

    pub fn merge_pins(&self, cids: &HashSet<String>) -> anyhow::Result<()> {
        use std::collections::HashSet;

        // Convert input list into a set for efficient lookup

        // Collect existing pins
        let mut existing = HashSet::new();
        let mut iter = self.db.iter(0)?;
        while let Some((key, _)) = iter.next()? {
            existing.insert(String::from_utf8(key)?);
        }

        // Add only the missing ones
        for cid in cids.difference(&existing) {
            let rec = PinRecord {
                last_progress: 0,
                last_progress_at: chrono::Utc::now().timestamp() as u64,
                total_blocks: 0,
                sync_complete: false,
            };
            self.put_pin(cid, &rec)?;
        }

        Ok(())
    }

    pub fn show_state(&self) -> anyhow::Result<()> {
        // current profile, if you have that stored separately
        if let Some(prof) = self.get_profile()? {
            tracing::info!("current_profile: {:?}", prof);
        } else {
            tracing::info!("current_profile: None");
        }

        // iterate through all pinned CIDs
        let mut iter = self.db.iter(0)?;
        while let Some((key, val)) = iter.next()? {
            let cid = String::from_utf8(key.to_vec())?;
            let (rec, _): (PinRecord, usize) = decode_from_slice(&val, config::standard())?;
            tracing::info!(
                "pinned: {} total blocks: {} last progress: {} at: {} complete: {}",
                cid,
                rec.total_blocks,
                rec.last_progress,
                rec.last_progress_at,
                rec.sync_complete
            );
        }

        Ok(())
    }

    pub fn update_progress(
        &self,
        updates: &HashMap<String, PinProgress>,
    ) -> Result<HashSet<String>> {
        let mut ops = Vec::new();
        let mut stale = HashSet::new();
        let now = chrono::Utc::now().timestamp() as u64;

        for (cid, progress) in updates {
            let key = cid.as_bytes();

            let existing = if let Some(val) = self.db.get(0, key)? {
                let (rec, _): (PinRecord, usize) = decode_from_slice(&val, config::standard())?;
                Some(rec)
            } else {
                None
            };

            let mut rec = existing.unwrap_or(PinRecord {
                last_progress: 0,
                last_progress_at: 0,
                total_blocks: 0,
                sync_complete: false,
            });

            match progress {
                PinProgress::Blocks(v) => {
                    if *v > rec.last_progress {
                        rec.last_progress = *v;
                        rec.last_progress_at = now;
                    }
                }
                PinProgress::Done => {
                    rec.sync_complete = true;
                    rec.last_progress_at = now;
                }
                PinProgress::Error(e) => {
                    tracing::error!("Error progress passed into update_progress: {}", e);
                }
                PinProgress::Raw(_) => {
                    // Skip raw lines
                }
            }

            if !rec.sync_complete
                && rec.last_progress_at > 0
                && now.saturating_sub(rec.last_progress_at) > 60
            {
                stale.insert(cid.clone());
            }

            let value = encode_to_vec(&rec, config::standard())?;
            ops.push((0, key, Some(value)));
        }

        if !ops.is_empty() {
            self.db.commit(ops)?;
        }

        Ok(stale)
    }

    pub fn touch_all_progress(&self) -> anyhow::Result<()> {
        let mut iter = self.db.iter(0)?; // column 0 = pins
        let now = chrono::Utc::now().timestamp() as u64;

        let mut batch = Vec::new();

        while let Some((key, val)) = iter.next()? {
            let (mut rec, _): (PinRecord, usize) = decode_from_slice(&val, config::standard())?;
            rec.last_progress_at = now;

            let new_val = encode_to_vec(&rec, config::standard())?;
            batch.push((0, key, Some(new_val)));
        }

        if !batch.is_empty() {
            self.db.commit(batch)?;
        }

        Ok(())
    }

    pub fn touch_progress(&self, cid: &str) -> anyhow::Result<()> {
        let key = cid.as_bytes();
        let now = chrono::Utc::now().timestamp() as u64;

        if let Some(val) = self.db.get(0, key)? {
            let (mut rec, _): (PinRecord, usize) = decode_from_slice(&val, config::standard())?;
            rec.last_progress_at = now;

            let new_val = encode_to_vec(&rec, config::standard())?;
            let batch = vec![(0, key.to_vec(), Some(new_val))];

            self.db.commit(batch)?;
        }

        Ok(())
    }

    pub fn completed_pins(&self) -> Result<HashSet<String>> {
        let mut iter = self.db.iter(0)?; // column 0 = pins
        let mut completed = HashSet::new();

        while let Some((key, val)) = iter.next()? {
            let (rec, _): (PinRecord, usize) = decode_from_slice(&val, config::standard())?;
            if rec.sync_complete {
                completed.insert(String::from_utf8(key.to_vec())?);
            }
        }

        Ok(completed)
    }
}

impl PoolTrait for CidPool {
    fn sync_pins(&self, cids: Vec<String>) -> Result<Vec<String>> {
        self.sync_pins(cids)
    }
    fn merge_pins(&self, cids: &HashSet<String>) -> Result<()> {
        self.merge_pins(cids)
    }
    fn update_progress(&self, updates: &HashMap<String, PinProgress>) -> Result<HashSet<String>> {
        self.update_progress(updates)
    }
    fn touch_all_progress(&self) -> Result<()> {
        self.touch_all_progress()
    }

    fn get_profile(&self) -> Result<Option<String>> {
        self.get_profile()
    }
    fn touch_progress(&self, cid: &str) -> Result<()> {
        self.touch_progress(cid)
    }
    fn record_failure(&self, cid: Option<&str>, action: &str, error: &str) -> Result<()> {
        self.record_failure(cid, action, error)
    }
    fn completed_pins(&self) -> Result<HashSet<String>> {
        self.completed_pins()
    }
}

//          //          //          //          //          //          //          //          //          //          //          //

//                      //                      //                      //                                  //                      //

//                      //                      //          //          //          //                      //                      //

//                      //                      //                                  //                      //                      //

//                      //                      //          //          //          //                      //                      //

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use tempfile::TempDir;

    fn new_pool() -> (CidPool, TempDir) {
        let dir = TempDir::new().unwrap();
        let pool = CidPool::init(dir.path().to_str().unwrap()).unwrap();
        (pool, dir)
    }

    #[test]
    fn put_and_get_pin_roundtrip() {
        let (pool, _tmpdir) = new_pool(); // keep _tmpdir alive for test lifetime

        let rec = PinRecord {
            last_progress: 5,
            last_progress_at: 100,
            total_blocks: 50,
            sync_complete: false,
        };
        pool.put_pin("cid1", &rec).unwrap();

        let fetched = pool.get_pin("cid1").unwrap().unwrap();
        assert_eq!(fetched.last_progress, 5);
    }

    #[test]
    fn delete_pin() {
        let (pool, _tmpdir) = new_pool(); // keep _tmpdir alive for test lifetime

        let rec = PinRecord {
            last_progress: 1,
            last_progress_at: 2,
            total_blocks: 3,
            sync_complete: false,
        };
        pool.put_pin("cidX", &rec).unwrap();
        pool.del_pin("cidX").unwrap();
        assert!(pool.get_pin("cidX").unwrap().is_none());
    }

    #[test]
    fn merge_and_sync_pins() {
        let (pool, _tmpdir) = new_pool(); // keep _tmpdir alive for test lifetime

        let mut set = HashSet::new();
        set.insert("cidA".to_string());
        set.insert("cidB".to_string());
        pool.merge_pins(&set).unwrap();
        assert!(pool.get_pin("cidA").unwrap().is_some());
        assert!(pool.get_pin("cidB").unwrap().is_some());

        pool.sync_pins(vec!["cidC".to_string()]).unwrap();
        assert!(pool.get_pin("cidC").unwrap().is_some());
        assert!(!pool.get_pin("cidA").unwrap().is_some());
        assert!(!pool.get_pin("cidB").unwrap().is_some());
    }

    #[test]
    fn touch_progress_updates_timestamp() {
        let (pool, _tmpdir) = new_pool(); // keep _tmpdir alive for test lifetime

        let rec = PinRecord {
            last_progress: 1,
            last_progress_at: 0,
            total_blocks: 10,
            sync_complete: false,
        };
        pool.put_pin("cidP", &rec).unwrap();

        pool.touch_progress("cidP").unwrap();

        let first_rec = pool.get_pin("cidP").unwrap().unwrap();
        assert!(first_rec.last_progress_at > rec.last_progress_at);

        std::thread::sleep(std::time::Duration::from_secs(1));

        pool.touch_progress("cidP").unwrap();

        let new_rec = pool.get_pin("cidP").unwrap().unwrap();
        assert!(new_rec.last_progress_at > first_rec.last_progress_at);
    }

    #[test]
    fn record_failure_stores_entry() {
        let (pool, _tmpdir) = new_pool(); // keep _tmpdir alive for test lifetime

        pool.record_failure(Some("cidErr"), "pin", "failure to be persisted")
            .unwrap();

        let mut iter = pool.db.iter(2).unwrap(); // must use 1, not 2
        let entry = iter.next().unwrap();
        assert!(entry.is_some());
    }

    #[test]
    fn set_and_get_profile_roundtrip() {
        let (pool, _tmpdir) = new_pool();

        pool.set_profile(Some("profile_cid")).unwrap();
        assert_eq!(pool.get_profile().unwrap(), Some("profile_cid".to_string()));

        pool.set_profile(None).unwrap();
        assert_eq!(pool.get_profile().unwrap(), None);
    }

    #[test]
    fn update_progress_variants() {
        let (pool, _tmpdir) = new_pool();

        // Start with a pin
        let rec = PinRecord {
            last_progress: 0,
            last_progress_at: 0,
            total_blocks: 0,
            sync_complete: false,
        };
        pool.put_pin("cidU", &rec).unwrap();

        let mut updates = HashMap::new();
        updates.insert("cidU".to_string(), PinProgress::Blocks(10));
        let stale = pool.update_progress(&updates).unwrap();
        assert!(stale.is_empty());
        assert_eq!(pool.get_pin("cidU").unwrap().unwrap().last_progress, 10);

        updates.insert("cidU".to_string(), PinProgress::Done);
        pool.update_progress(&updates).unwrap();
        assert!(pool.get_pin("cidU").unwrap().unwrap().sync_complete);

        updates.insert("cidU".to_string(), PinProgress::Error("oops".into()));
        pool.update_progress(&updates).unwrap();

        updates.insert("cidU".to_string(), PinProgress::Raw("ignored".into()));
        pool.update_progress(&updates).unwrap();
    }

    #[test]
    fn touch_all_progress_updates_all() {
        let (pool, _tmpdir) = new_pool();

        pool.put_pin(
            "cid1",
            &PinRecord {
                last_progress: 0,
                last_progress_at: 0,
                total_blocks: 0,
                sync_complete: false,
            },
        )
        .unwrap();

        pool.put_pin(
            "cid2",
            &PinRecord {
                last_progress: 0,
                last_progress_at: 0,
                total_blocks: 0,
                sync_complete: false,
            },
        )
        .unwrap();

        pool.touch_all_progress().unwrap();

        let r1 = pool.get_pin("cid1").unwrap().unwrap();
        let r2 = pool.get_pin("cid2").unwrap().unwrap();
        assert!(r1.last_progress_at > 0);
        assert!(r2.last_progress_at > 0);
    }

    #[test]
    fn record_failure_with_none_cid() {
        let (pool, _tmpdir) = new_pool();
        pool.record_failure(None, "action", "err").unwrap();

        let mut iter = pool.db.iter(2).unwrap();
        let (key, _val) = iter.next().unwrap().unwrap();
        assert!(String::from_utf8(key).unwrap().starts_with("failure-"));
    }

    #[test]
    fn get_pin_nonexistent_returns_none() {
        let (pool, _tmpdir) = new_pool();
        assert!(pool.get_pin("nope").unwrap().is_none());
    }

    #[test]
    fn show_state_runs_without_panic() {
        let (pool, _tmpdir) = new_pool();
        pool.set_profile(Some("prof")).unwrap();
        pool.put_pin(
            "cidS",
            &PinRecord {
                last_progress: 1,
                last_progress_at: 2,
                total_blocks: 3,
                sync_complete: false,
            },
        )
        .unwrap();

        // Just check it doesn't panic
        pool.show_state().unwrap();
    }
}
