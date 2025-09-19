use anyhow::Result;
use bincode::{
    config,
    serde::{decode_from_slice, encode_to_vec},
};
use parity_db::{Db, Options};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};

#[derive(Debug, Serialize, Deserialize)]
pub struct PinRecord {
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
                eprintln!("Failed to open DB at {:?}: {}. Recreating...", db_path, e);

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

    pub fn get_pin(&self, cid: &str) -> Result<Option<PinRecord>> {
        if let Some(val) = self.db.get(0, cid.as_bytes())? {
            let (rec, _len): (PinRecord, usize) = decode_from_slice(&val, config::standard())?;
            Ok(Some(rec))
        } else {
            Ok(None)
        }
    }

    /// Delete a pin record
    pub fn del_pin(&self, cid: &str) -> Result<()> {
        self.db.commit([(0, cid.as_bytes(), None)])?;
        Ok(())
    }

    /// Save current profile
    pub fn set_profile(&self, cid: Option<&str>) -> Result<()> {
        let value = cid.map(|c| c.as_bytes().to_vec());
        self.db.commit([(1, b"profile", value)])?;
        Ok(())
    }

    /// Get current profile
    pub fn get_profile(&self) -> Result<Option<String>> {
        if let Some(val) = self.db.get(1, b"profile")? {
            Ok(Some(String::from_utf8(val)?))
        } else {
            Ok(None)
        }
    }

    /// Record a failure
    pub fn record_failure(&self, cid: Option<&str>, action: &str, error: &str) -> Result<()> {
        let ts = chrono::Utc::now().timestamp_millis().to_be_bytes();
        let mut key = b"fail_".to_vec();
        key.extend_from_slice(&ts);

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
        self.db.commit([(1, key.as_bytes(), Some(value))])?;
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
            println!("current_profile: {:?}", prof);
        } else {
            println!("current_profile: None");
        }

        // iterate through all pinned CIDs
        let mut iter = self.db.iter(0)?;
        while let Some((key, val)) = iter.next()? {
            let cid = String::from_utf8(key.to_vec())?;
            let (rec, _): (PinRecord, usize) = decode_from_slice(&val, config::standard())?;
            println!(
                "pinned: {} total blocks: {} progress at: {} complete: {}",
                cid, rec.total_blocks, rec.last_progress_at, rec.sync_complete
            );
        }

        Ok(())
    }
}
