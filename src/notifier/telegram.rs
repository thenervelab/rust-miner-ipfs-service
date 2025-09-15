use crate::notifier::Notifier;
use anyhow::Context;
use anyhow::Result;
use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct TelegramUpdate {
    //    update_id: i64,
    message: Option<TelegramMessage>,
}

#[derive(Debug, Deserialize)]
struct TelegramMessage {
    chat: TelegramChat,
}

#[derive(Debug, Deserialize)]
struct TelegramChat {
    id: i64,
}

pub struct TelegramNotifier {
    bot_token: String,
    chat_id: String,
    client: Client,
}

impl TelegramNotifier {
    pub async fn new(bot_token: String, chat_id: Option<String>) -> Result<Self> {
        let client = Client::new();
        let chat_id = match chat_id {
            Some(id) => id,
            None => {
                // Try to fetch from getUpdates
                let url = format!("https://api.telegram.org/bot{}/getUpdates", bot_token);
                let resp = client
                    .get(&url)
                    .send()
                    .await
                    .context("telegram getUpdates failed")?;
                let json: serde_json::Value = resp.json().await?;
                let updates: Vec<TelegramUpdate> = serde_json::from_value(json["result"].clone())
                    .context("invalid Telegram updates")?;

                let first_id = updates
                    .into_iter()
                    .filter_map(|u| u.message.map(|m| m.chat.id))
                    .next()
                    .context("no chat_id found in getUpdates")?;

                first_id.to_string()
            }
        };

        Ok(Self {
            bot_token,
            chat_id,
            client: client,
        })
    }
}

#[async_trait]
impl Notifier for TelegramNotifier {
    async fn notify(&self, subject: &str, message: &str) -> Result<()> {
        let text = format!("🚨 {}\n{}", subject, message);
        let url = format!("https://api.telegram.org/bot{}/sendMessage", self.bot_token);
        self.client
            .post(&url)
            .json(&serde_json::json!({"chat_id": self.chat_id, "text": text}))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    fn name(&self) -> &'static str {
        "telegram"
    }

    fn is_healthy(&self) -> Result<(&str, bool)> {
        Ok((self.name(), true))
    }
}
