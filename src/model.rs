use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Profile {
    #[allow(dead_code)]
    pub version: Option<u32>,
    #[serde(default)]
    pub pin: Vec<PinItem>,
}

#[derive(Debug, Deserialize)]
pub struct PinItem {
    pub cid: String,
    #[allow(dead_code)]
    #[serde(default)]
    pub priority: Option<i32>,
}
