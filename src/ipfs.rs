use anyhow::{Context, Result};
use futures_util::StreamExt;
use reqwest::Url;
use serde_json::Deserializer;
use std::collections::HashSet;
use std::time::Duration;

use crate::{
    model::PinState,
    service::{PinProgress, ProgressSender},
};

#[derive(Clone)]
pub struct Client {
    base: Url,
    http: reqwest::Client,
}

#[async_trait::async_trait]
pub trait IpfsClient: Send + Sync {
    async fn cat_json<T: serde::de::DeserializeOwned + Send>(&self, cid: &str) -> Result<T>;
    async fn pin_add_with_progress(&self, cid: &str, tx: ProgressSender) -> Result<()>;
    async fn pin_rm(&self, cid: &str) -> Result<()>;
    async fn pin_verify(&self) -> Result<Vec<PinState>>;
    #[allow(dead_code)]
    async fn gc(&self) -> Result<()>;
    #[allow(dead_code)]
    async fn check_health(&self) -> Result<()>;
    async fn pin_ls_all(&self) -> Result<HashSet<String>>;
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

    pub async fn cat_json<T: for<'de> serde::Deserialize<'de>>(&self, cid: &str) -> Result<T> {
        // Try gateway first if base looks like a gateway, otherwise use /api/v0/cat
        let url = self.base.join("/api/v0/cat")?;

        let resp = async_std::future::timeout(
            Duration::from_secs(30),
            self.http.post(url).query(&[("arg", cid)]).send(),
        )
        .await;

        let resp = resp??.error_for_status()?;

        let body = resp.text().await?;

        let json: T = serde_json::from_str(&body).context("invalid_profile_json")?;

        Ok(json)
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
                    let mut de = serde_json::Deserializer::from_slice(slice)
                        .into_iter::<serde_json::Value>();

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
                                {
                                    if !pins.is_empty() {
                                        let _ = tx.send(PinProgress::Done);
                                    }
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

    pub async fn pin_verify(&self) -> Result<Vec<PinState>> {
        let verify_url = self.base.join("/api/v0/pin/verify")?;
        let response_verify = self
            .http
            .post(verify_url)
            .query(&[("verbose", "true")])
            .send()
            .await?
            .error_for_status()?;

        let body = response_verify.text().await?;

        let stream = Deserializer::from_str(&body).into_iter::<PinState>();

        let mut verification: Vec<PinState> = Vec::new();
        for obj in stream {
            verification.push(obj?);
        }

        let mut response: Vec<PinState> = vec![];

        for pin_state in verification {
            response.push(pin_state)
        }

        Ok(response)
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
}

#[async_trait::async_trait]
impl IpfsClient for Client {
    async fn cat_json<T: serde::de::DeserializeOwned + Send>(&self, cid: &str) -> Result<T> {
        self.cat_json(cid).await
    }

    async fn pin_add_with_progress(&self, cid: &str, tx: ProgressSender) -> anyhow::Result<()> {
        self.pin_add_with_progress(cid, tx).await
    }

    async fn pin_rm(&self, cid: &str) -> Result<()> {
        self.pin_rm(cid).await
    }

    async fn pin_verify(&self) -> Result<Vec<PinState>> {
        self.pin_verify().await
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
    use std::sync::mpsc;

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
}
