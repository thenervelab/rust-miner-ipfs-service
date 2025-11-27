use anyhow::Result;
use futures_util::StreamExt;
use reqwest::Url;
use serde::de::DeserializeOwned;
use serde_json::Deserializer;
use std::collections::HashSet;
use std::time::Duration;

use crate::{
    parse::{parse_json, parse_parquet},
    service::{PinProgress, ProgressSender},
};

#[derive(Clone)]
pub struct Client {
    base: Url,
    http: reqwest::Client,
}

#[async_trait::async_trait]
pub trait IpfsClient: Send + Sync {
    async fn cat<T: DeserializeOwned + Send>(&self, cid: &str) -> Result<T>;
    async fn pin_add_with_progress(&self, cid: &str, tx: ProgressSender) -> Result<()>;
    async fn pin_rm(&self, cid: &str) -> Result<()>;

    async fn pin_ls_single(&self, cid: &str) -> Result<bool>;

    #[allow(dead_code)]
    async fn gc(&self) -> Result<()>;
    #[allow(dead_code)]
    async fn check_health(&self) -> Result<()>;
    async fn pin_ls_all(&self) -> Result<HashSet<String>>;
    async fn connect_bootstrap(&self, addr: &str) -> Result<()>;
}

impl Client {
    pub fn new(api_url: String) -> Self {
        let base = Url::parse(&api_url).expect("invalid ipfs api url");
        let http = reqwest::Client::builder()
            .user_agent("miner-ipfs-service/0.1")
            .build()
            .unwrap();
        Self { base, http }
    }

    pub async fn check_health(&self) -> Result<()> {
        let url = self.base.join("/api/v0/id")?;
        let _res = self.http.post(url).send().await?;
        Ok(())
    }

    pub async fn cat<T>(&self, cid: &str) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let url = self.base.join("/api/v0/cat")?;
        let resp = async_std::future::timeout(
            Duration::from_secs(30),
            self.http.post(url).query(&[("arg", cid)]).send(),
        )
        .await??
        .error_for_status()?;

        let bytes = resp.bytes().await?;

        if let Ok(text) = std::str::from_utf8(&bytes) {
            if let Ok(val) = parse_json::<T>(text) {
                return Ok(val);
            }
        }

        parse_parquet::<T>(&bytes)
    }

    /// Cat helper *without* the 30s timeout – used for background
    /// miner profile fetch so that we don't artificially time out.
    pub async fn cat_no_timeout<T>(&self, cid: &str) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let url = self.base.join("/api/v0/cat")?;
        let resp = self
            .http
            .post(url)
            .query(&[("arg", cid)])
            .send()
            .await?
            .error_for_status()?;

        let bytes = resp.bytes().await?;

        if let Ok(text) = std::str::from_utf8(&bytes) {
            if let Ok(val) = parse_json::<T>(text) {
                return Ok(val);
            }
        }

        parse_parquet::<T>(&bytes)
    }

    pub async fn pin_add_with_progress(&self, cid: &str, tx: ProgressSender) -> Result<()> {
        let url = self.base.join("/api/v0/pin/add")?;
        let resp = self
            .http
            .post(url)
            .query(&[("arg", cid), ("recursive", "true"), ("progress", "true")])
            .send()
            .await?
            .error_for_status()?;

        let mut stream = resp.bytes_stream();
        let mut buffer = Vec::new();

        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    buffer.extend_from_slice(&bytes);

                    let slice: &[u8] = &buffer;
                    let mut de = Deserializer::from_slice(slice).into_iter::<serde_json::Value>();

                    let mut consumed = 0;
                    while let Some(item) = de.next() {
                        match item {
                            Ok(json) => {
                                consumed = de.byte_offset();

                                if let Some(progress) =
                                    json.get("Progress").and_then(|v| v.as_u64())
                                {
                                    let _ = tx.send(PinProgress::Blocks(progress));
                                } else if let Some(pins) =
                                    json.get("Pins").and_then(|v| v.as_array())
                                    && !pins.is_empty()
                                {
                                    let _ = tx.send(PinProgress::Done);
                                }
                            }
                            Err(e) if e.is_eof() => {
                                // not enough data yet → wait for next chunk
                                break;
                            }
                            Err(e) => {
                                let _ = tx.send(PinProgress::Error(e.to_string()));
                                return Err(e.into());
                            }
                        }
                    }

                    // Drop only parsed portion, keep remainder in buffer
                    buffer.drain(0..consumed);
                }
                Err(e) => {
                    let _ = tx.send(PinProgress::Error(e.to_string()));
                    return Err(e.into());
                }
            }
        }

        Ok(())
    }

    pub async fn pin_rm(&self, cid: &str) -> Result<()> {
        let url = self.base.join("/api/v0/pin/rm")?;
        self.http
            .post(url)
            .query(&[("arg", cid), ("recursive", "true")])
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    /// Lightweight per-CID pin check using /api/v0/pin/ls?arg=<cid>.
    pub async fn pin_ls_single(&self, cid: &str) -> Result<bool> {
        let url = self.base.join("/api/v0/pin/ls")?;
        let resp = self
            .http
            .post(url)
            .query(&[("arg", cid), ("type", "recursive")])
            .send()
            .await?
            .error_for_status()?;

        let val: serde_json::Value = resp.json().await?;

        // Older Kubo: Keys map
        if let Some(keys) = val.get("Keys").and_then(|k| k.as_object()) {
            if keys.contains_key(cid) {
                return Ok(true);
            }
        }

        // Newer Kubo: Pins array
        if let Some(arr) = val.get("Pins").and_then(|a| a.as_array()) {
            for v in arr {
                if let Some(pin_cid) = v.get("Cid").and_then(|c| c.as_str()) {
                    if pin_cid == cid {
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }

    pub async fn pin_ls_all(&self) -> Result<HashSet<String>> {
        let mut set = HashSet::new();
        let url = self.base.join("/api/v0/pin/ls")?;
        let resp = self
            .http
            .post(url.clone())
            .query(&[("type", "recursive")])
            .send()
            .await?
            .error_for_status()?;
        let val: serde_json::Value = resp.json().await?;
        if let Some(keys) = val.get("Keys").and_then(|k| k.as_object()) {
            for (cid, _obj) in keys.iter() {
                set.insert(cid.to_string());
            }
        }
        // Newer Kubo returns array format
        if let Some(arr) = val.get("Pins").and_then(|a| a.as_array()) {
            for v in arr {
                if let Some(cid) = v.get("Cid").and_then(|c| c.as_str()) {
                    set.insert(cid.to_string());
                }
            }
        }

        let resp = self
            .http
            .post(url)
            .query(&[("type", "direct")])
            .send()
            .await?
            .error_for_status()?;
        let val: serde_json::Value = resp.json().await?;
        if let Some(keys) = val.get("Keys").and_then(|k| k.as_object()) {
            for (cid, _obj) in keys.iter() {
                set.insert(cid.to_string());
            }
        }
        // Newer Kubo returns array format
        if let Some(arr) = val.get("Pins").and_then(|a| a.as_array()) {
            for v in arr {
                if let Some(cid) = v.get("Cid").and_then(|c| c.as_str()) {
                    set.insert(cid.to_string());
                }
            }
        }

        Ok(set)
    }

    pub async fn gc(&self) -> Result<()> {
        let url = self.base.join("/api/v0/repo/gc")?;
        self.http.post(url).send().await?.error_for_status()?;
        Ok(())
    }

    pub async fn connect_bootstrap(&self, addr: &str) -> anyhow::Result<()> {
        use tokio::time::{Duration, timeout};

        let per_step_timeout = Duration::from_secs(10);
        let mut any_ok = false;

        match self.base.join("/api/v0/bootstrap/add") {
            Ok(url) => {
                match timeout(
                    per_step_timeout,
                    self.http.post(url).query(&[("arg", addr)]).send(),
                )
                .await
                {
                    Ok(Ok(resp)) => match resp.error_for_status_ref() {
                        Ok(_) => {
                            any_ok = true;
                            tracing::info!(step = "bootstrap_add", %addr, "OK");
                        }
                        Err(e) => {
                            tracing::warn!(
                                step = "bootstrap_add",
                                %addr,
                                status = ?resp.status(),
                                error = ?e,
                                "HTTP error"
                            );
                        }
                    },
                    Ok(Err(e)) => {
                        tracing::warn!(step = "bootstrap_add", %addr, error = ?e, "request error")
                    }
                    Err(_) => tracing::warn!(step = "bootstrap_add", %addr, "timed out"),
                }
            }
            Err(e) => tracing::warn!(step = "bootstrap_add", %addr, error = ?e, "bad URL"),
        }

        match self.base.join("/api/v0/swarm/peering/add") {
            Ok(url) => {
                match timeout(
                    per_step_timeout,
                    self.http.post(url).query(&[("arg", addr)]).send(),
                )
                .await
                {
                    Ok(Ok(resp)) => match resp.error_for_status_ref() {
                        Ok(_) => {
                            any_ok = true;
                            tracing::info!(step = "peering_add", %addr, "OK");
                        }
                        Err(e) => {
                            tracing::warn!(
                                step = "peering_add",
                                %addr,
                                status = ?resp.status(),
                                error = ?e,
                                "HTTP error"
                            );
                        }
                    },
                    Ok(Err(e)) => {
                        tracing::warn!(step = "peering_add", %addr, error = ?e, "request error")
                    }
                    Err(_) => tracing::warn!(step = "peering_add", %addr, "timed out"),
                }
            }
            Err(e) => tracing::warn!(step = "peering_add", %addr, error = ?e, "bad URL"),
        }

        match self.base.join("/api/v0/swarm/connect") {
            Ok(url) => {
                match timeout(
                    per_step_timeout,
                    self.http.post(url).query(&[("arg", addr)]).send(),
                )
                .await
                {
                    Ok(Ok(resp)) => match resp.error_for_status_ref() {
                        Ok(_) => {
                            any_ok = true;
                            tracing::info!(step = "swarm_connect", %addr, "OK");
                        }
                        Err(e) => {
                            tracing::warn!(
                                step = "swarm_connect",
                                %addr,
                                status = ?resp.status(),
                                error = ?e,
                                "HTTP error"
                            );
                        }
                    },
                    Ok(Err(e)) => {
                        tracing::warn!(step = "swarm_connect", %addr, error = ?e, "request error")
                    }
                    Err(_) => tracing::warn!(step = "swarm_connect", %addr, "timed out"),
                }
            }
            Err(e) => tracing::warn!(step = "swarm_connect", %addr, error = ?e, "bad URL"),
        }

        if !any_ok {
            tracing::warn!(%addr, "connect_bootstrap: all steps failed for address");
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl IpfsClient for Client {
    async fn cat<T: DeserializeOwned + Send>(&self, cid: &str) -> Result<T> {
        self.cat(cid).await
    }

    async fn pin_add_with_progress(&self, cid: &str, tx: ProgressSender) -> Result<()> {
        self.pin_add_with_progress(cid, tx).await
    }

    async fn pin_rm(&self, cid: &str) -> Result<()> {
        self.pin_rm(cid).await
    }

    async fn pin_ls_single(&self, cid: &str) -> Result<bool> {
        self.pin_ls_single(cid).await
    }

    async fn pin_ls_all(&self) -> Result<HashSet<String>> {
        self.pin_ls_all().await
    }

    async fn gc(&self) -> Result<()> {
        self.gc().await
    }

    async fn check_health(&self) -> Result<()> {
        self.check_health().await
    }

    async fn connect_bootstrap(&self, addr: &str) -> Result<()> {
        self.connect_bootstrap(addr).await
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
    use httpmock::prelude::*;
    use serde_json::json;
    use std::sync::mpsc;

    #[derive(serde::Deserialize, Debug)]
    struct Dummy {
        name: String,
    }

    #[tokio::test]
    async fn check_health_ok() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.path("/api/v0/id");
            then.status(200).body("{}");
        });

        let client = Client::new(server.base_url());
        assert!(client.check_health().await.is_ok());
    }

    #[tokio::test]
    async fn cat_valid_and_invalid() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.path("/api/v0/cat").query_param("arg", "cid1");
            then.status(200).body(r#"{"name":"alice"}"#);
        });

        server.mock(|when, then| {
            when.path("/api/v0/cat").query_param("arg", "cid2");
            then.status(200).body("not_json");
        });

        let client = Client::new(server.base_url());

        #[derive(serde::Deserialize)]
        struct Dummy {
            name: String,
        }

        // valid
        let val: Dummy = client.cat("cid1").await.unwrap();
        assert_eq!(val.name, "alice");

        // invalid
        let res: Result<Dummy> = client.cat("cid2").await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn pin_add_with_progress_blocks_and_done() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.path("/api/v0/pin/add");
            then.status(200).body(r#"{"Progress":10}{"Pins":["abc"]}"#);
        });

        let client = Client::new(server.base_url());
        let (tx, rx) = mpsc::channel();
        client.pin_add_with_progress("abc", tx).await.unwrap();

        let first = rx.recv().unwrap();
        assert!(matches!(first, PinProgress::Blocks(10)));

        let second = rx.recv().unwrap();
        assert!(matches!(second, PinProgress::Done));
    }

    #[tokio::test]
    async fn pin_add_with_progress_newline_delimited() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.path("/api/v0/pin/add");
            then.status(200).body(
                r#"{"Progress":10}
                   {"Progress":20}
                   {"Pins":["abc"]}"#,
            );
        });

        let client = Client::new(server.base_url());
        let (tx, rx) = mpsc::channel();
        client.pin_add_with_progress("abc", tx).await.unwrap();

        let updates: Vec<_> = rx.try_iter().collect();
        assert!(matches!(updates[0], PinProgress::Blocks(10)));
        assert!(matches!(updates[1], PinProgress::Blocks(20)));
        assert!(matches!(updates[2], PinProgress::Done));
    }

    #[tokio::test]
    async fn pin_add_with_progress_error_branch() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.path("/api/v0/pin/add");
            then.status(200).body("this_is_not_json");
        });

        let client = Client::new(server.base_url());
        let (tx, rx) = mpsc::channel();
        let res = client.pin_add_with_progress("abc", tx).await;
        assert!(res.is_err());

        let got = rx.recv().unwrap();
        assert!(matches!(got, PinProgress::Error(_)));
    }

    #[tokio::test]
    async fn pin_rm_ok() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.path("/api/v0/pin/rm");
            then.status(200).body("{}");
        });

        let client = Client::new(server.base_url());
        assert!(client.pin_rm("cid").await.is_ok());
    }

    #[tokio::test]
    async fn pin_rm_fails_on_non_200() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.path("/api/v0/pin/rm");
            then.status(500).body("error");
        });

        let client = Client::new(server.base_url());
        let res = client.pin_rm("badcid").await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn pin_ls_all_handles_keys_and_pins() {
        let server = MockServer::start();

        // first call (recursive)
        server.mock(|when, then| {
            when.path("/api/v0/pin/ls").query_param("type", "recursive");
            then.status(200).json_body(json!({
                "Keys": { "cid1": {} },
                "Pins": [{ "Cid": "cid2" }]
            }));
        });

        // second call (direct)
        server.mock(|when, then| {
            when.path("/api/v0/pin/ls").query_param("type", "direct");
            then.status(200).json_body(json!({
                "Keys": { "cid3": {} },
                "Pins": [{ "Cid": "cid4" }]
            }));
        });

        let client = Client::new(server.base_url());
        let set = client.pin_ls_all().await.unwrap();

        assert!(set.contains("cid1"));
        assert!(set.contains("cid2"));
        assert!(set.contains("cid3"));
        assert!(set.contains("cid4"));
    }

    #[tokio::test]
    async fn gc_ok() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.path("/api/v0/repo/gc");
            then.status(200).body("{}");
        });

        let client = Client::new(server.base_url());
        assert!(client.gc().await.is_ok());
    }

    #[tokio::test]
    async fn trait_forwarding_cat() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.path("/api/v0/cat");
            then.status(200).body(r#"{"name":"bob"}"#);
        });

        let client = Client::new(server.base_url());
        let obj: Dummy = IpfsClient::cat(&client, "cidx").await.unwrap();
        assert_eq!(obj.name, "bob");
    }

    #[tokio::test]
    async fn connect_bootstrap_ok() {
        let server = MockServer::start();

        // Example bootstrap multiaddr (as provided).
        let addr =
            "/ip4/104.131.131.82/tcp/4001/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ";

        // Expect POST /api/v0/swarm/connect?arg=<addr>
        server.mock(|when, then| {
            when.path("/api/v0/swarm/connect").query_param("arg", addr);
            then.status(200)
                .body(r#"{"Strings":["connect <peer> success"]}"#);
        });

        let client = Client::new(server.base_url());
        let res = client.connect_bootstrap(addr).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn connect_bootstrap_suppresses_non_200_and_returns_ok() {
        use httpmock::MockServer;

        let server = MockServer::start();

        // Full multiaddr including /p2p/PeerID
        let addr = "/ip4/203.0.113.5/tcp/4001/p2p/12D3KooWAbCdEfGh";

        // Mock all three endpoints the client now calls, all returning non-200.
        let bootstrap_mock = server.mock(|when, then| {
            when.path("/api/v0/bootstrap/add").query_param("arg", addr);
            then.status(500).body("bootstrap add failed");
        });

        let peering_mock = server.mock(|when, then| {
            when.path("/api/v0/swarm/peering/add")
                .query_param("arg", addr);
            then.status(500).body("peering add failed");
        });

        let connect_mock = server.mock(|when, then| {
            when.path("/api/v0/swarm/connect").query_param("arg", addr);
            then.status(500).body("dial error");
        });

        let client = Client::new(server.base_url());
        let res = client.connect_bootstrap(addr).await;

        assert!(
            res.is_ok(),
            "connect_bootstrap should suppress errors and return Ok"
        );

        bootstrap_mock.assert();
        peering_mock.assert();
        connect_mock.assert();
    }
}
