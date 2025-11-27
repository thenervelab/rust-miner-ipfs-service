[`Coverage Report`](https://thenervelab.github.io/rust-miner-ipfs-service/)


A generated rust service that mirrors the functionality of `thenervelab/miner-ipfs-service`:


- Fetch current **miner profile CID** from a Substrate-based chain (configurable pallet/storage).
- Download **profile JSON** from IPFS (via Kubo HTTP API) and resolve list of CIDs to **pin**.
- **Pin** new CIDs, **unpin** removed ones, track state in **ParityDB**.
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
poll_interval_secs = 30 # How often to poll the chain for miner profile json CID changes
reconcile_interval_secs = 6 # How often check progress on ongoing pin tasks
health_check_interval_secs = 30 # How often to check substrate / ipfs node connection
initial_pin_concurrency = 32 # Initial number of concurrent pin tasks 
# Increases if all tasks show regular progress
stalling_pin_task_detection = 120 # How many seconds without progress in a pin task
# To trigger stalling pin task warning notification

[db]
path = "./miner_db_pool"


[ipfs]
# Kubo API endpoint
api_url = "http://127.0.0.1:5001"
bootstrap = [
  "/ip4/104.131.131.82/tcp/4001/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
  "/dns4/ipfs-bootnode-1.hippius.network/tcp/4001/p2p/12D3KooWAtWvvmkeA6y7CAGXhRZMGKYJkHkG7LQAcGearpV4QwKG"
]

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

[monitoring]
port = 9090
metrics_port = 9464

```


## Profile JSON schema


The service expects the miner profile referenced by the on-chain CID to be a JSON document containing an array of CIDs to pin:

### Example:

```json
[
  {
    "account_ss58": "5CRyFwmSHJC7EeGLGbU1G8ycuoxu8sQxExhfBhkwNPtQU5n2",
    "cid": "bafkreieo4sqlujkrkqt4bug6zvlqkoh7oi2b6iz3ewldj4iv34ruihjv7a",
    "cid_v2": "bafkreieo4sqlujkrkqt4bug6zvlqkoh7oi2b6iz3ewldj4iv34ruihjv7a",
    "created_at": 0,
    "file_hash": [98,97,102,107,...],
    "file_id": "0f8272d225e5bdb04ff4b383a5bdd0eb34ef208786755152d39ceb80787805ed",
    "file_size_in_bytes": 16,
    "miner_node_id": "12D3KooWBGedBmJifS4MhWFmtKkR3xjHkYTGnw6Qw8fKtgFaY3Dp",
    "original_name": "target/release/.fingerprint/crc-catalog-e73f70b140dd4938/lib-crc_catalog",
    "owner": "5CRyFwmSHJC7EeGLGbU1G8ycuoxu8sQxExhfBhkwNPtQU5n2",
    "selected_validator": "5G1Qj93Fy22grpiGKq6BEvqqmS2HVRs3jaEdMhq9absQzs6g",
    "size_bytes": 16
  },
  {
    "account_ss58": "5HoreGVb17XhY3wanDvzoAWS7yHYbc5uMteXqRNTiZ6Txkqq",
    "cid": "Qmbbh7CtJtfRP7JWXNPj2kHAnjCEhkCYvhqS46zbJFaS7z",
    "cid_v2": "Qmbbh7CtJtfRP7JWXNPj2kHAnjCEhkCYvhqS46zbJFaS7z",
    "created_at": 0,
    "file_hash": [81,109,98,98,...],
    "file_id": "81ebf3f86cbaee1757ac7be997235b75a69e1f4184989386e5fca6a28faf7816",
    "file_size_in_bytes": 31564,
    "miner_node_id": "12D3KooWBGedBmJifS4MhWFmtKkR3xjHkYTGnw6Qw8fKtgFaY3Dp",
    "original_name": "tmpckosueh5",
    "owner": "5HoreGVb17XhY3wanDvzoAWS7yHYbc5uMteXqRNTiZ6Txkqq",
    "selected_validator": "5G1Qj93Fy22grpiGKq6BEvqqmS2HVRs3jaEdMhq9absQzs6g",
    "size_bytes": 31564
  }
]
```

Only the `cid` field is required. Extra fields are ignored but logged.


---

## Running as a Systemd Service on Linux

To run the service continuously in the background and start automatically on boot, you can install it as a **systemd service**.

### 1. Install the binary
Build your release binary and move it into a permanent location (e.g., `/opt/miner-ipfs-service`):

bash
```
# Build release binary (or download release)
cargo build --release

# Create install directory
sudo mkdir -p /opt/miner-ipfs-service

# Copy binary and sample config

sudo cp ./target/release/miner-ipfs-service /opt/miner-ipfs-service/
sudo cp ./config.sample.toml /opt/miner-ipfs-service/config.toml

# Make sure it’s executable

sudo chmod +x /opt/miner-ipfs-service/miner-ipfs-service
```

### 2. Create a dedicated user

It’s good practice to run services under a non-root user:

```
sudo useradd --system --no-create-home --shell /usr/sbin/nologin --user-group miner
sudo chown -R miner:miner /opt/miner-ipfs-service
```

### 3. Configure the service

Edit the config file (/opt/miner-ipfs-service/config.toml) with your desired settings.
Most importantly api_url (for kubo api, making sure that ipfs kubo api is available), ws_url (for blockchain rpc node), miner_profile_id.
Optionally gmail and telegram credentials for notifications.

### 4. Create a systemd unit file

Create /etc/systemd/system/miner-ipfs-service.service:
```
[Unit]
Description=Rust Miner IPFS Service
After=network-online.target ipfs.service
Wants=network-online.target

[Service]
User=miner
Group=miner
WorkingDirectory=/opt/miner-ipfs-service
ExecStart=/opt/miner-ipfs-service/miner-ipfs-service run
Restart=on-failure
RestartSec=5
Environment=RUST_LOG=info,miner_ipfs_service=debug

[Install]
WantedBy=multi-user.target
```
### 5. Reload systemd and enable the service
```
sudo systemctl daemon-reload
sudo systemctl enable miner-ipfs-service
sudo systemctl start miner-ipfs-service
```

### 6. Check logs
```
# Show current status
systemctl status miner-ipfs-service

# Follow logs in real time
journalctl -u miner-ipfs-service -f
```

### 7. Managing the service
```
#Start:
sudo systemctl start miner-ipfs-service

#Stop: 
sudo systemctl stop miner-ipfs-service

#Restart: 
sudo systemctl restart miner-ipfs-service

Enable on boot: 
#sudo systemctl enable miner-ipfs-service

Disable on boot: 
#sudo systemctl disable miner-ipfs-service
```
