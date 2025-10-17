use anyhow::{Context, Result};
use figment::providers::Format;
use figment::{
    Figment,
    providers::{Env, Json, Serialized, Toml},
};
use serde::{Deserialize, Serialize};

const DEFAULT_POLL_INTERVAL_SECS: u64 = 12;
const DEFAULT_RECONCILE_INTERVAL_SECS: u64 = 12;
const HEALTH_CHECK_INTERVAL_SECS: u64 = 30;
const IPFS_CAT_TIMEOUT_SECS: u64 = 30;
const IPFS_GC_INTERVAL_SECS: u64 = 300;
const DEFAULT_CONCURRENCY: usize = 32;
const DEFAULT_STALLING_DETECTION_SECS: u64 = 120;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ServiceCfg {
    pub poll_interval_secs: u64,
    pub reconcile_interval_secs: u64,
    pub health_check_interval_secs: u64,
    pub ipfs_cat_timeout_secs: u64,
    pub ipfs_gc_interval_secs: u64,
    pub stalling_pin_task_detection: u64,
    pub initial_pin_concurrency: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DbCfg {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IpfsCfg {
    pub api_url: String,
    pub bootstrap: Vec<String>,
    pub gc_after_unpin: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SubstrateCfg {
    pub ws_url: String,
    pub raw_storage_key_hex: Option<String>,
    pub pallet: Option<String>,
    pub storage_item: Option<String>,
    pub miner_profile_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TelegramCfg {
    pub bot_token: Option<String>,
    pub chat_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GmailCfg {
    pub username: Option<String>,
    pub app_password: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MonitoringConfig {
    pub port: Option<u16>,
    pub metrics_port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Settings {
    pub service: ServiceCfg,
    pub db: DbCfg,
    pub ipfs: IpfsCfg,
    pub substrate: SubstrateCfg,
    pub telegram: TelegramCfg,
    pub gmail: GmailCfg,
    pub monitoring: MonitoringConfig,
}

pub async fn load(path: Option<&str>, with_env: bool, with_conf: bool) -> Result<Settings> {
    let defaults = Settings {
        service: ServiceCfg {
            poll_interval_secs: DEFAULT_POLL_INTERVAL_SECS,
            reconcile_interval_secs: DEFAULT_RECONCILE_INTERVAL_SECS,
            health_check_interval_secs: HEALTH_CHECK_INTERVAL_SECS,
            ipfs_cat_timeout_secs: IPFS_CAT_TIMEOUT_SECS,
            ipfs_gc_interval_secs: IPFS_GC_INTERVAL_SECS,
            initial_pin_concurrency: DEFAULT_CONCURRENCY,
            stalling_pin_task_detection: DEFAULT_STALLING_DETECTION_SECS,
        },
        db: DbCfg {
            path: "./miner_db_pool".into(),
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
            bootstrap: vec![],
            gc_after_unpin: false,
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
        monitoring: MonitoringConfig {
            port: Some(9090),
            metrics_port: Some(9464),
        },
    };

    let mut fig = Figment::from(Serialized::defaults(defaults));

    if with_env {
        fig = fig.merge(Env::prefixed("MINER_").split("__"));
    }

    if let Some(p) = path {
        if p.ends_with(".toml") {
            fig = fig.merge(Toml::file(p));
        } else if p.ends_with(".json") {
            fig = fig.merge(Json::file(p));
        } else {
            fig = fig.merge(Toml::file(p));
        }
    } else if with_conf {
        fig = fig.merge(Toml::file("./config.toml"));
    }

    fig.extract::<Settings>().context("invalid_config")
}

//          //          //          //          //          //          //          //          //          //          //          //

//                      //                      //                      //                                  //                      //

//                      //                      //          //          //          //                      //                      //

//                      //                      //                                  //                      //                      //

//                      //                      //          //          //          //                      //                      //

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[serial_test::serial]
    #[tokio::test]
    async fn loads_defaults_when_no_file_or_env() {
        let settings = load(None, false, false).await.unwrap();
        assert_eq!(settings.db.path, "./miner_db_pool");
        assert_eq!(settings.service.poll_interval_secs, 12);
        assert_eq!(settings.ipfs.api_url, "http://127.0.0.1:5001");
        assert_eq!(settings.monitoring.port, Some(9090));
    }

    #[serial_test::serial]
    #[tokio::test]
    async fn merges_with_toml_file() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("config.toml");
        fs::write(
            &file,
            r#"
        [db]
        path = "custom.db"
        [service]
        poll_interval_secs = 42
        "#,
        )
        .unwrap();

        let settings = load(Some(file.to_str().unwrap()), false, true)
            .await
            .unwrap();
        assert_eq!(settings.db.path, "custom.db");
        assert_eq!(settings.service.poll_interval_secs, 42);
        // Unchanged defaults
        assert_eq!(settings.ipfs.api_url, "http://127.0.0.1:5001");
    }

    #[serial_test::serial]
    #[tokio::test]
    async fn merges_with_json_file() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("config.json");
        fs::write(
            &file,
            r#"
        {
          "db": { "path": "db_from_json.db" },
          "ipfs": { "api_url": "http://json:5001" }
        }
        "#,
        )
        .unwrap();

        let settings = load(Some(file.to_str().unwrap()), false, true)
            .await
            .unwrap();
        assert_eq!(settings.db.path, "db_from_json.db");
        assert_eq!(settings.ipfs.api_url, "http://json:5001");
    }

    #[serial_test::serial]
    #[tokio::test]
    async fn env_vars_override_defaults() {
        // RAII guard that sets environment variables and restores previous state on drop.
        struct EnvGuard {
            prev: Vec<(String, Option<std::ffi::OsString>)>,
        }

        impl EnvGuard {
            fn new() -> Self {
                EnvGuard { prev: Vec::new() }
            }

            /// set an env var while saving previous value (if any)
            fn set<K: AsRef<str>, V: AsRef<str>>(&mut self, k: K, v: V) {
                let key = k.as_ref().to_string();
                let previous = std::env::var_os(&key);
                self.prev.push((key.clone(), previous));
                // unsafe but only used in tests to emulate environment
                unsafe { std::env::set_var(&key, v.as_ref()) };
            }
        }

        impl Drop for EnvGuard {
            fn drop(&mut self) {
                for (key, maybe_val) in self.prev.drain(..) {
                    match maybe_val {
                        // unsafe but only used in tests to emulate environment
                        Some(val) => unsafe { std::env::set_var(&key, val) },
                        // unsafe but only used in tests to emulate environment
                        None => unsafe { std::env::remove_var(&key) },
                    }
                }
            }
        }

        // Use serial_test to avoid races with other tests that touch environment.
        let mut guard = EnvGuard::new();
        guard.set("MINER_DB__PATH", "env.db");
        guard.set("MINER_SERVICE__POLL_INTERVAL_SECS", "123");

        // Debug: confirm env vars are set correctly before calling load
        tracing::info!("MINER_DB__PATH = {:?}", std::env::var("MINER_DB__PATH"));
        tracing::info!(
            "MINER_SERVICE__POLL_INTERVAL_SECS = {:?}",
            std::env::var("MINER_SERVICE__POLL_INTERVAL_SECS")
        );

        // IMPORTANT: call load with with_env=true and with_conf=false
        let settings = load(None, true, false).await.unwrap();

        assert_eq!(settings.db.path, "env.db");
        assert_eq!(settings.service.poll_interval_secs, 123);

        // EnvGuard will restore env automatically here
    }

    #[serial_test::serial]
    #[tokio::test]
    async fn invalid_toml_file_returns_error() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("broken.toml");
        fs::write(&file, "this = [not valid toml").unwrap();

        let err = load(Some(file.to_str().unwrap()), false, true)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("invalid_config"));
    }

    #[serial_test::serial]
    #[tokio::test]
    async fn non_toml_json_extension_treated_as_toml() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("config.conf");
        fs::write(&file, "[db]\npath = \"conf.db\"\n").unwrap();

        let settings = load(Some(file.to_str().unwrap()), false, true)
            .await
            .unwrap();
        assert_eq!(settings.db.path, "conf.db");
    }
}
