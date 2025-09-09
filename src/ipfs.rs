use anyhow::{Context, Result};
use reqwest::Url;

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
        self.http
            .post(url)
            .query(&[("arg", cid), ("progress", "false")])
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
