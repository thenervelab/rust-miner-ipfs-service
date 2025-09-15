use anyhow::{Context, Result};
use figment::providers::Format;
use figment::{
    Figment,
    providers::{Env, Json, Serialized, Toml},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceCfg {
    pub poll_interval_secs: u64,
    pub reconcile_interval_secs: u64,
    pub conn_check_interval_secs: u64,
    pub ipfs_gc_interval_secs: u64,
    pub max_concurrent_ipfs_ops: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbCfg {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpfsCfg {
    pub api_url: String,
    pub gateway_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubstrateCfg {
    pub ws_url: String,
    pub raw_storage_key_hex: Option<String>,
    pub pallet: Option<String>,
    pub storage_item: Option<String>,
    pub miner_profile_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelegramCfg {
    pub bot_token: Option<String>,
    pub chat_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GmailCfg {
    pub username: Option<String>,
    pub app_password: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringConfig {
    pub port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    pub service: ServiceCfg,
    pub db: DbCfg,
    pub ipfs: IpfsCfg,
    pub substrate: SubstrateCfg,
    pub telegram: TelegramCfg,
    pub gmail: GmailCfg,
    pub monitoring: MonitoringConfig,
}

pub async fn load(path: Option<&str>) -> Result<Settings> {
    let defaults = Settings {
        service: ServiceCfg {
            poll_interval_secs: 10,
            reconcile_interval_secs: 10,
            ipfs_gc_interval_secs: 3600,
            max_concurrent_ipfs_ops: 8,
            conn_check_interval_secs: 30,
        },
        db: DbCfg {
            path: "./miner.db".into(),
        },
        substrate: SubstrateCfg {
            ws_url: "ws://127.0.0.1:9944".into(),
            pallet: None,
            storage_item: None,
            miner_profile_id: None,
            raw_storage_key_hex: None,
        },
        ipfs: IpfsCfg {
            api_url: "http://127.0.0.1:5001".into(),
            gateway_url: Some("http://127.0.0.1:8080".into()),
        },
        telegram: TelegramCfg {
            bot_token: None,
            chat_id: None,
        },
        gmail: GmailCfg {
            username: None,
            app_password: None,
            from: None,
            to: None,
        },
        monitoring: MonitoringConfig { port: Some(9090) },
    };

    let mut fig = Figment::from(Serialized::defaults(defaults)).merge(Env::prefixed("MINER_"));

    if let Some(p) = path {
        if p.ends_with(".toml") {
            fig = fig.merge(Toml::file(p));
        } else if p.ends_with(".json") {
            fig = fig.merge(Json::file(p));
        } else {
            fig = fig.merge(Toml::file(p));
        }
    } else {
        fig = fig.merge(Toml::file("./config.toml"));
    }

    fig.extract::<Settings>().context("invalid_config")
}
