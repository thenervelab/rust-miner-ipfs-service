use anyhow::{Context, Result};
use hex::FromHex;
use subxt::dynamic::storage;
use subxt::{OnlineClient, PolkadotConfig};

pub struct Chain {
    client: OnlineClient<PolkadotConfig>,
}

impl Chain {
    pub async fn connect(ws_url: &str) -> Result<Self> {
        let client = OnlineClient::<PolkadotConfig>::from_url(ws_url).await?;
        Ok(Self { client })
    }

    pub async fn fetch_profile_cid(
        &self,
        raw_storage_key_hex: Option<&str>,
        pallet: Option<&str>,
        storage_item: Option<&str>,
        miner_account_hex: Option<&str>,
    ) -> Result<Option<String>> {
        if let Some(raw_key) = raw_storage_key_hex {
            let key = <Vec<u8>>::from_hex(raw_key).context("invalid raw_storage_key_hex")?;
            let storage = self.client.storage().at_latest().await?;
            let data: Option<Vec<u8>> = storage.fetch_raw(key).await?;
            return Ok(data.map(bytes_to_string));
        }

        let pallet = pallet.context("missing pallet in config")?;
        let item = storage_item.context("missing storage_item in config")?;
        let miner_hex = miner_account_hex.context("missing miner_account_hex in config")?;
        let account = <[u8; 32]>::from_hex(miner_hex).context("invalid miner_account_hex")?;

        let addr = storage(pallet, item, vec![account.to_vec().into()]);
        let storage_client = self.client.storage().at_latest().await?;
        let maybe_val = storage_client.fetch(&addr).await?;
        let bytes: Option<Vec<u8>> = maybe_val.map(|val| val.encoded().to_vec());

        Ok(bytes.map(bytes_to_string))
    }
}

fn bytes_to_string(bytes: Vec<u8>) -> String {
    String::from_utf8(bytes).unwrap_or_else(|b| format!("0x{}", hex::encode(b.into_bytes())))
}
