use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct FileInfo {
    pub account_ss58: String,
    pub cid: String,
    pub cid_v2: String,
    pub created_at: u64,
    pub file_hash: Vec<u8>,
    pub file_id: String,
    pub file_size_in_bytes: u64,
    pub miner_node_id: String,
    pub original_name: String,
    pub owner: String,
    pub selected_validator: String,
    pub size_bytes: u64,
}
