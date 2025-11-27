#![cfg(test)] // only compiled when running tests

use std::sync::Arc;

use crate::{
    db::PoolTrait,
    ipfs::IpfsClient,
    service::{PinProgress, ProgressSender},
};
use async_trait::async_trait;

use anyhow::Result;
use std::collections::{HashMap, HashSet};

pub struct DummyIpfs {
    pub cat_result: Result<serde_json::Value>,
    pub pin_rm_result: Result<()>,
    pub pin_ls_all_result: Result<HashSet<String>>,
    pub pin_ls_single_result: Result<bool>,
    pub health_ok: bool,

    // control success/failure of connect_bootstrap()
    pub connect_bootstrap_result: Result<()>,
}

impl Default for DummyIpfs {
    fn default() -> Self {
        Self {
            cat_result: Ok(serde_json::json!({})),
            pin_rm_result: Ok(()),
            pin_ls_all_result: Ok(HashSet::new()),
            pin_ls_single_result: Ok(true),
            health_ok: true,
            connect_bootstrap_result: Ok(()), // default to success
        }
    }
}

#[async_trait]
impl IpfsClient for DummyIpfs {
    async fn cat<T>(&self, _cid: &str) -> Result<T>
    where
        T: serde::de::DeserializeOwned + Send,
    {
        match &self.cat_result {
            Ok(val) => Ok(serde_json::from_value(val.clone())?),
            Err(e) => Err(anyhow::anyhow!(e.to_string())),
        }
    }

    async fn pin_rm(&self, _cid: &str) -> Result<()> {
        match &self.pin_rm_result {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow::anyhow!(e.to_string())),
        }
    }

    async fn pin_ls_single(&self, _cid: &str) -> Result<bool> {
        match &self.pin_ls_single_result {
            Ok(v) => Ok(*v),
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
        if self.health_ok {
            Ok(())
        } else {
            Err(anyhow::anyhow!("ipfs down"))
        }
    }

    async fn pin_ls_all(&self) -> Result<HashSet<String>> {
        match &self.pin_ls_all_result {
            Ok(v) => Ok(v.clone()),
            Err(e) => Err(anyhow::anyhow!(e.to_string())),
        }
    }

    async fn connect_bootstrap(&self, _addr: &str) -> Result<()> {
        match &self.connect_bootstrap_result {
            Ok(_) => Ok(()),
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

    fn update_progress(&self, _updates: &HashMap<String, PinProgress>) -> Result<HashSet<String>> {
        Ok(HashSet::new())
    }

    fn touch_all_progress(&self) -> Result<()> {
        Ok(())
    }

    fn touch_progress(&self, _cid: &str) -> Result<()> {
        Ok(())
    }

    fn completed_pins(&self) -> Result<HashSet<String>> {
        Ok(HashSet::new())
    }

    fn mark_incomplete(&self, _cid: &str) -> Result<()> {
        Ok(())
    }

    fn stalled_pins(&self) -> Result<HashSet<String>> {
        Ok(HashSet::new())
    }

    fn get_profile_pins(&self) -> Result<Option<Vec<String>>> {
        // For tests:
        // - If `profile` is set, treat it as a single CID to pin.
        // - If `profile` is None, behave like "no profile pins yet".
        Ok(self.profile.as_ref().map(|cid| vec![cid.clone()]))
    }
}
