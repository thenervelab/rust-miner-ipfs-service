[`Coverage Report`](https://jubilant-adventure-pgevypq.pages.github.io/)


A generated rust service that mirrors the functionality of `thenervelab/miner-ipfs-service`:


- Fetch current **miner profile CID** from a Substrate-based chain (configurable pallet/storage).
- Download **profile JSON** from IPFS (via Kubo HTTP API) and resolve list of CIDs to **pin**.
- **Pin** new CIDs, **unpin** removed ones, track state in **ParityDB**.
- Periodic **IPFS GC**.
- Rich **observability** (structured logs via tracing), **CLI**, **monitoring api**, **notifications** and **graceful shutdown**.


## Quickstart


```bash
# 1) Build
cargo build --release


# 2) Create config (see sample below)
cp config.sample.toml config.toml


# 3) Run
./target/release/miner-ipfs-service run
```



### Sample config (`config.sample.toml`)
```toml
[service]
poll_interval_secs = 6 # how often to check chain for profile CID changes
reconcile_interval_secs = 300 # run full reconciliation this often
ipfs_gc_interval_secs = 3600


[db]
path = "./miner.db"


[ipfs]
# Kubo API endpoint
api_url = "http://127.0.0.1:5001"

[substrate]
# WebSocket endpoint for Substrate node
ws_url = "ws://127.0.0.1:9944"
# Read the miner profile CID from this pallet/storage.
# Option A — dynamic storage lookup:
pallet = "IpfsPallet" # e.g., your pallet name
storage_item = "MinerProfile" # e.g., storage map (AccountId32 -> BoundedVec<u8>)
# 32-byte hex account id of the miner whose profile we track (without 0x)
miner_profile_id = "12D3KooWDEfckrwi1YC3Pv9fqwFz8GGpW1xoVPWdu7mEGsjVwSV1"

[telegram]
bot_token = "YOUR_TELEGRAM_BOT_TOKEN" # eg. 123456789:AAG9RuJiqgOGIfFbOPBpAo6QhIJoD9mCdDs
chat_id = "YOUR_CHAT_ID"

[gmail]
username = "your@gmail.com"
app_password = "your apps pass word"
from = "your@gmail.com"
to = "alert-recipient@example.com"

```


## Profile JSON schema


The service expects the miner profile referenced by the on-chain CID to be a JSON document containing an array of CIDs to pin:


```json
{
"version": 1,
"pin": [
{ "cid": "Qm...", "priority": 10 },
{ "cid": "bafy..." }
]
}
```


Only the `cid` field is required. Extra fields are ignored but logged.


---


## Systemd unit (example)


```ini
[Unit]
Description=Rust Miner IPFS Service
After=network-online.target ipfs.service
Wants=network-online.target


[Service]
User=miner
Group=miner
WorkingDirectory=/opt/miner-ipfs-service
ExecStart=/opt/miner-ipfs-service/miner-ipfs-service run --config /opt/miner-ipfs-service/config.toml
Restart=on-failure
RestartSec=5
Environment=RUST_LOG=info,miner_ipfs_service=debug


[Install]
WantedBy=multi-user.target
```


---