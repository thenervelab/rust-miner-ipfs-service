#![cfg(test)] // only compiled when running tests

use std::sync::Arc;

use crate::{
    db::PoolTrait,
    ipfs::IpfsClient,
    model::PinState,
    service::{PinProgress, ProgressSender},
};
use async_trait::async_trait;

use anyhow::Result;
use std::collections::{HashMap, HashSet};

pub struct DummyIpfs {
    pub cat_result: Result<serde_json::Value>,
    pub pin_rm_result: Result<()>,
    pub pin_verify_result: Result<Vec<PinState>>,
    pub pin_ls_all_result: Result<HashSet<String>>,
    pub health_ok: bool,
}

impl Default for DummyIpfs {
    fn default() -> Self {
        Self {
            cat_result: Ok(serde_json::json!({})), // empty JSON
            pin_rm_result: Ok(()),
            pin_verify_result: Ok(vec![]),
            pin_ls_all_result: Ok(HashSet::new()),
            health_ok: true,
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
}
