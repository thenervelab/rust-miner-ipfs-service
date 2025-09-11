use crate::model::PinState;
use anyhow::{Context, Result};
use reqwest::Url;
use serde_json::Deserializer;

#[derive(Clone)]
pub struct Client {
    base: Url,
    http: reqwest::Client,
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
        let res = self.http.post(url).send().await?;

        Ok(())
    }

    pub async fn cat_json<T: for<'de> serde::Deserialize<'de>>(&self, cid: &str) -> Result<T> {
        // Try gateway first if base looks like a gateway, otherwise use /api/v0/cat
        let url = self.base.join("/api/v0/cat")?;
        let resp = self.http.post(url).query(&[("arg", cid)]).send().await;

        let resp = resp?.error_for_status()?;

        let body = resp.text().await?;

        let json: T = serde_json::from_str(&body).context("invalid_profile_json")?;

        Ok(json)
    }

    pub async fn pin_add(&self, cid: &str) -> Result<()> {
        let url = self.base.join("/api/v0/pin/add")?;
        let response = self
            .http
            .post(url)
            .query(&[("arg", cid), ("recursive", "true"), ("progress", "true")])
            .send()
            .await?
            .error_for_status()?;

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
            if !pin_state.ok {
                response.push(pin_state)
            }
        }

        Ok(response)
    }

    // pub async fn pin_ls_all(&self) -> Result<HashSet<String>> {
    //     let url = self.base.join("/api/v0/pin/ls")?;
    //     let resp = self.http.post(url).send().await?.error_for_status()?;
    //     let val: serde_json::Value = resp.json().await?;
    //     let mut set = HashSet::new();
    //     if let Some(keys) = val.get("Keys").and_then(|k| k.as_object()) {
    //         for (cid, _obj) in keys.iter() {
    //             set.insert(cid.to_string());
    //         }
    //         return Ok(set);
    //     }
    //     // Newer Kubo returns array format
    //     if let Some(arr) = val.get("Pins").and_then(|a| a.as_array()) {
    //         for v in arr {
    //             if let Some(cid) = v.get("Cid").and_then(|c| c.as_str()) {
    //                 set.insert(cid.to_string());
    //             }
    //         }
    //         return Ok(set);
    //     }
    //     bail!("unexpected pin ls response: {}", val);
    // }

    pub async fn gc(&self) -> Result<()> {
        let url = self.base.join("/api/v0/repo/gc")?;
        self.http.post(url).send().await?.error_for_status()?;
        Ok(())
    }
}
