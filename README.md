

A generated rust service that mirrors the functionality of `thenervelab/miner-ipfs-service`:


- Fetch current **miner profile CID** from a Substrate-based chain (configurable pallet/storage).
- Download **profile JSON** from IPFS (via Kubo HTTP API) and resolve list of CIDs to **pin**.
- **Pin** new CIDs, **unpin** removed ones, track state in **SQLite**.
- **Retry** failed operations with exponential backoff; persist failures for inspection.
- Periodic **IPFS GC**.
- Rich **observability** (structured logs via tracing), **CLI**, and **graceful shutdown**.


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
poll_interval_secs = 60 # how often to check chain for profile CID changes
reconcile_interval_secs = 300 # run full reconciliation this often
max_concurrent_ipfs_ops = 8
retry_max_elapsed_secs = 600
ipfs_gc_interval_secs = 3600


[db]
path = "./miner.db"


[ipfs]
# Kubo API endpoint
api_url = "http://127.0.0.1:5001"
# Optional IPFS HTTP Gateway used only for profile JSON fetch (fallback to /api/v0/cat)
gateway_url = "http://ipfs.io"


[substrate]
# WebSocket endpoint for Substrate node
ws_url = "ws://127.0.0.1:9944"
# Read the miner profile CID from this pallet/storage.
# Option A — dynamic storage lookup:
pallet = "HippiusMiner" # e.g., your pallet name
storage_item = "MinerProfileCid" # e.g., storage map (AccountId32 -> BoundedVec<u8>)
# 32-byte hex account id of the miner whose profile we track (without 0x)
miner_account_hex = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"


# Option B — raw storage key (overrides above if set)
# raw_storage_key_hex = "26aa394eea5630e07c48ae0c9558cef7..."
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