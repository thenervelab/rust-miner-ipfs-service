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

    pub async fn check_health(&self) -> Result<()> {
        let latest_block = self.client.blocks().at_latest().await?; // self.client.rpc().system_health().await?; 

        Ok(())
    }

    pub async fn fetch_profile_cid(
        &self,
        raw_storage_key_hex: Option<&str>,
        pallet: Option<&str>,
        storage_item: Option<&str>,
        miner_profile_id: Option<&str>,
    ) -> Result<Option<String>> {
        if let Some(raw_key) = raw_storage_key_hex {
            let key = <Vec<u8>>::from_hex(raw_key).context("invalid raw_storage_key_hex")?;
            let storage = self.client.storage().at_latest().await?;
            let data: Option<Vec<u8>> = storage.fetch_raw(key).await?;
            return Ok(data.map(bytes_to_string));
        }

        let pallet = pallet.context("missing pallet in config")?;
        let item = storage_item.context("missing storage_item in config")?;
        let miner_hex = miner_profile_id.context("missing miner_profile_id in config")?;

        let key_bytes = miner_hex.as_bytes().to_vec();

        let storage_addr = storage(
            pallet,
            item,
            vec![subxt::dynamic::Value::from_bytes(key_bytes)],
        );

        let maybe_profile = self
            .client
            .storage()
            .at_latest()
            .await?
            .fetch(&storage_addr)
            .await?;

        if let Some(val) = maybe_profile {
            let valencode = &val.encoded()[1..];

            if valencode.len() < 59 {
                return Ok(None);
            }

            let cid_hex: String = hex::encode(valencode);

            println!("Found CID (hex form) {:#?}", cid_hex);

            let cid = String::from_utf8(valencode.to_vec())?;

            println!("Found CID (string form) {:#?}", cid);
            return Ok(Some(cid));
        }

        return Ok(None);
    }
}

fn bytes_to_string(bytes: Vec<u8>) -> String {
    String::from_utf8(bytes).unwrap_or_else(|b| format!("0x{}", hex::encode(b.into_bytes())))
}
