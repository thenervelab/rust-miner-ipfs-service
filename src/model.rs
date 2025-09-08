use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Profile {
    //    pub version: Option<u32>,
    #[serde(default)]
    pub pin: Vec<PinItem>,
}

#[derive(Debug, Deserialize)]
pub struct PinItem {
    pub cid: String,
    //    #[serde(default)]
    //    pub priority: Option<i32>,
}
