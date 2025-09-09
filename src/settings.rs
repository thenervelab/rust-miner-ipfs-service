use anyhow::{Context, Result};
use figment::{
    Figment,
    providers::{Env, Format, Json, Serialized, Toml},
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ServiceCfg {
    pub poll_interval_secs: u64,
    pub reconcile_interval_secs: u64,
    pub ipfs_gc_interval_secs: u64,
    pub max_concurrent_ipfs_ops: usize,
    pub retry_max_elapsed_secs: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DbCfg {
    pub path: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IpfsCfg {
    pub api_url: String,
    pub gateway_url: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubstrateCfg {
    pub ws_url: String,
    pub pallet: Option<String>,
    pub storage_item: Option<String>,
    pub miner_profile_id: Option<String>,
    pub raw_storage_key_hex: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Settings {
    pub service: ServiceCfg,
    pub db: DbCfg,
    pub ipfs: IpfsCfg,
    pub substrate: SubstrateCfg,
}

pub async fn load(path: Option<&str>) -> Result<Settings> {
    let mut fig = Figment::from(Serialized::defaults(Settings{
        service: ServiceCfg{
            poll_interval_secs: 10,
            reconcile_interval_secs: 10,
            ipfs_gc_interval_secs: 30,
            max_concurrent_ipfs_ops: 16,
            retry_max_elapsed_secs: 10,
        },
        db: DbCfg{ path: "./miner.db".into() },
        ipfs: IpfsCfg{ api_url: "http://127.0.0.1:5001".into(), gateway_url: Some("http://127.0.0.1:8080".into()) },
        substrate: SubstrateCfg{ ws_url: "ws://127.0.0.1:9944".into(), pallet: None, storage_item: None, miner_profile_id: None, raw_storage_key_hex: None },
    }))
    .merge(Env::prefixed("MINER_")) // e.g. MINER_SERVICE__POLL_INTERVAL_SECS
    ;

    if let Some(p) = path {
        if p.ends_with(".toml") {
            fig = fig.merge(Toml::file(p));
        } else if p.ends_with(".json") {
            fig = fig.merge(Json::file(p));
        } else {
            fig = fig.merge(Toml::file(p));
        }
    } else {
        // best-effort defaults
        fig = fig.merge(Toml::file("./config.toml"));
    }

    fig.extract::<Settings>().context("invalid_config")
}
