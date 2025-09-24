use anyhow::{Context, Result};
use async_trait::async_trait;
use hex::FromHex;
use std::sync::Arc;
use subxt::{OnlineClient, PolkadotConfig, dynamic, storage::DefaultAddress};

// Define an async trait that abstracts the bits of Subxt we use so we can mock it in tests.
// The trait returns raw bytes for storage values (the same bytes you'd get from `val.encoded()`).
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait SubxtClient: Send + Sync {
    async fn blocks_at_latest(&self) -> Result<()>;
    async fn fetch_raw(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>>;
    async fn fetch_storage(
        &self,
        addr: DefaultAddress<
            Vec<dynamic::Value>,
            dynamic::DecodedValueThunk,
            subxt::utils::Yes,
            subxt::utils::Yes,
            subxt::utils::Yes,
        >,
    ) -> Result<Option<Vec<u8>>>;
}

// Real implementation that delegates to `OnlineClient<PolkadotConfig>`.
pub struct RealSubxtClient {
    inner: OnlineClient<PolkadotConfig>,
}

impl RealSubxtClient {
    pub fn new(inner: OnlineClient<PolkadotConfig>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl SubxtClient for RealSubxtClient {
    async fn blocks_at_latest(&self) -> Result<()> {
        let _ = self.inner.blocks().at_latest().await?;
        Ok(())
    }

    async fn fetch_raw(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let storage = self.inner.storage().at_latest().await?;
        Ok(storage.fetch_raw(key).await?)
    }

    async fn fetch_storage(
        &self,
        addr: DefaultAddress<
            Vec<dynamic::Value>,
            dynamic::DecodedValueThunk,
            subxt::utils::Yes,
            subxt::utils::Yes,
            subxt::utils::Yes,
        >,
    ) -> Result<Option<Vec<u8>>> {
        let storage = self.inner.storage().at_latest().await?;
        let maybe_val = storage.fetch(&addr).await?;
        Ok(maybe_val.map(|v| v.encoded().to_vec()))
    }
}

#[derive(Clone)]
pub struct Chain {
    client: Arc<dyn SubxtClient>,
    url: String,
}

impl Chain {
    // pub fn new(client: Arc<dyn SubxtClient>, url: String) -> Self {
    //     Self {
    //         client: client,
    //         url: url,
    //     }
    // }

    pub fn client(&self) -> Arc<dyn SubxtClient> {
        Arc::clone(&self.client)
    }

    /// Connect using a real OnlineClient and wrap with our SubxtClient trait object.
    pub async fn connect(ws_url: &str) -> Result<Self> {
        let client = OnlineClient::<PolkadotConfig>::from_url(ws_url).await?;
        Ok(Self {
            client: std::sync::Arc::new(RealSubxtClient::new(client)),
            url: ws_url.to_string(),
        })
    }

    /// For tests: construct Chain from a boxed SubxtClient implementation
    #[allow(dead_code)]
    pub fn from_client_boxed(client: Box<dyn SubxtClient>, url: &str) -> Self {
        Self {
            client: std::sync::Arc::from(client),
            url: url.to_string(),
        }
    }

    pub async fn check_health(&mut self) -> Result<()> {
        match self.client().blocks_at_latest().await {
            Ok(()) => Ok(()),
            Err(_) => {
                // try to recreate real client
                let client = OnlineClient::<PolkadotConfig>::from_url(self.url.clone()).await?;
                self.client = std::sync::Arc::new(RealSubxtClient::new(client));
                Ok(())
            }
        }
    }

    /// fetch_profile_cid implements the same logic as before but uses the SubxtClient trait.
    /// When `raw_storage_key_hex` is provided we use `fetch_raw` directly.
    pub async fn fetch_profile_cid(
        &mut self,
        raw_storage_key_hex: Option<&str>,
        pallet: Option<&str>,
        storage_item: Option<&str>,
        miner_profile_id: Option<&str>,
    ) -> Result<Option<String>> {
        if let Some(raw_key) = raw_storage_key_hex {
            let key = <Vec<u8>>::from_hex(raw_key).context("invalid raw_storage_key_hex")?;
            let data = self.client.fetch_raw(key).await?;
            return Ok(data.map(bytes_to_string));
        }

        let pallet = pallet.context("missing pallet in config")?;
        let item = storage_item.context("missing storage_item in config")?;
        let miner_hex = miner_profile_id.context("missing miner_profile_id in config")?;

        let key_bytes = miner_hex.as_bytes().to_vec();

        let storage_addr = dynamic::storage(
            pallet,
            item,
            vec![subxt::dynamic::Value::from_bytes(key_bytes)],
        );

        // ensure health (may replace client if unhealthy)
        match self.check_health().await {
            Ok(()) => {}
            _ => {
                let client = OnlineClient::<PolkadotConfig>::from_url(self.url.clone()).await?;
                self.client = std::sync::Arc::new(RealSubxtClient::new(client));
            }
        };

        let maybe_profile = self.client.fetch_storage(storage_addr).await?;

        if let Some(val_encoded) = maybe_profile {
            if val_encoded.len() <= 1 {
                return Ok(None);
            }
            let valencode = &val_encoded[1..];

            if valencode.len() < 59 {
                if valencode.len() == 46 {
                    let cid = String::from_utf8(valencode.to_vec())?;
                    tracing::info!("Found CID (string form) {}", cid);
                    return Ok(Some(cid));
                } else {
                    return Ok(None);
                }
            }

            // >= 59: treat as hex-form CID and also attempt string
            let _cid_hex: String = hex::encode(valencode);
            let cid = String::from_utf8(valencode.to_vec())?;
            tracing::info!("Found CID (string form) {}", cid);
            return Ok(Some(cid));
        }

        Ok(None)
    }
}

fn bytes_to_string(bytes: Vec<u8>) -> String {
    String::from_utf8(bytes).unwrap_or_else(|b| format!("0x{}", hex::encode(b.into_bytes())))
}

//          //          //          //          //          //          //          //          //          //          //          //

//                      //                      //                      //                                  //                      //

//                      //                      //          //          //          //                      //                      //

//                      //                      //                                  //                      //                      //

//                      //                      //          //          //          //                      //                      //

#[cfg(test)]
mod tests {
    use super::*;
    use mockall::predicate::*;

    #[tokio::test]
    async fn storage_fetch_long_returns_string() {
        let mut mock = MockSubxtClient::new();

        // Allow health check to succeed
        mock.expect_blocks_at_latest().returning(|| Ok(()));

        let mut encoded = vec![0u8];
        encoded.extend_from_slice(&b"z".repeat(100));
        mock.expect_fetch_storage()
            .returning(move |_addr| Ok(Some(encoded.clone())));

        let mut chain = Chain::from_client_boxed(Box::new(mock), "wss://none");
        let got = chain
            .fetch_profile_cid(None, Some("p"), Some("i"), Some("miner"))
            .await
            .unwrap();
        assert_eq!(got, Some("z".repeat(100)));
    }

    #[tokio::test]
    async fn storage_fetch_short_other_length_returns_none() {
        let mut mock = MockSubxtClient::new();

        // Allow health check to succeed
        mock.expect_blocks_at_latest().returning(|| Ok(()));

        let mut encoded = vec![0u8];
        encoded.extend_from_slice(&b"x".repeat(10));
        mock.expect_fetch_storage()
            .returning(move |_addr| Ok(Some(encoded.clone())));

        let mut chain = Chain::from_client_boxed(Box::new(mock), "wss://none");
        let got = chain
            .fetch_profile_cid(None, Some("p"), Some("i"), Some("miner"))
            .await
            .unwrap();
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn storage_fetch_short_46_bytes_returns_string() {
        let mut mock = MockSubxtClient::new();

        // Allow health check to succeed
        mock.expect_blocks_at_latest().returning(|| Ok(()));

        // return encoded bytes where val.encoded()[1..] has length 46 and is valid utf8
        let mut encoded = vec![0u8];
        encoded.extend_from_slice(&b"a".repeat(46));
        mock.expect_fetch_storage()
            .returning(move |_addr| Ok(Some(encoded.clone())));

        let mut chain = Chain::from_client_boxed(Box::new(mock), "wss://none");
        let got = chain
            .fetch_profile_cid(None, Some("p"), Some("i"), Some("miner"))
            .await
            .unwrap();
        assert_eq!(got, Some("a".repeat(46)));
    }

    #[tokio::test]
    async fn storage_fetch_none_returns_none() {
        let mut mock = MockSubxtClient::new();

        // Allow health check to succeed
        mock.expect_blocks_at_latest().returning(|| Ok(()));

        mock.expect_fetch_storage().returning(|_addr| Ok(None));

        let mut chain = Chain::from_client_boxed(Box::new(mock), "wss://none");
        let res = chain
            .fetch_profile_cid(None, Some("p"), Some("i"), Some("miner"))
            .await
            .unwrap();
        assert!(res.is_none());
    }

    #[tokio::test]
    async fn missing_pallet_or_item_errors() {
        let mut mock = MockSubxtClient::new();

        // Allow health check to succeed
        mock.expect_blocks_at_latest().returning(|| Ok(()));

        let mut chain = Chain::from_client_boxed(Box::new(mock), "wss://none");

        // missing pallet
        let err = chain
            .fetch_profile_cid(None, None, Some("item"), Some("miner"))
            .await
            .unwrap_err();
        assert!(format!("{err:?}").contains("missing pallet in config"));

        // missing item
        let err = chain
            .fetch_profile_cid(None, Some("p"), None, Some("miner"))
            .await
            .unwrap_err();
        assert!(format!("{err:?}").contains("missing storage_item in config"));

        // missing miner
        let err = chain
            .fetch_profile_cid(None, Some("p"), Some("i"), None)
            .await
            .unwrap_err();
        assert!(format!("{err:?}").contains("missing miner_profile_id in config"));
    }

    #[tokio::test]
    async fn raw_storage_key_invalid_hex_errors() {
        let mut mock = MockSubxtClient::new();

        // Allow health check to succeed
        mock.expect_blocks_at_latest().returning(|| Ok(()));

        let mut chain = Chain::from_client_boxed(Box::new(mock), "wss://none");

        let err = chain
            .fetch_profile_cid(Some("not-hex"), None, None, None)
            .await
            .unwrap_err();

        assert_eq!(format!("{err}"), "invalid raw_storage_key_hex");
    }

    #[tokio::test]
    async fn raw_storage_key_returns_string() {
        let mut mock = MockSubxtClient::new();

        // Allow health check to succeed
        mock.expect_blocks_at_latest().returning(|| Ok(()));

        mock.expect_fetch_raw()
            .with(eq(vec![1, 2, 3]))
            .times(1)
            .returning(|_k| Ok(Some(b"profile-cid".to_vec())));

        let mut chain = Chain::from_client_boxed(Box::new(mock), "wss://none");
        let got = chain
            .fetch_profile_cid(Some("010203"), None, None, None)
            .await
            .unwrap();
        assert_eq!(got, Some("profile-cid".to_string()));
    }
}
