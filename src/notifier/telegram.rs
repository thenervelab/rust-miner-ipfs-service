use crate::notifier::Notifier;
use anyhow::Context;
use anyhow::Result;
use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct TelegramUpdate {
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
    base_url: String,
}

impl TelegramNotifier {
    pub async fn new(bot_token: String, chat_id: Option<String>) -> Result<Self> {
        Self::with_base_url(bot_token, chat_id, "https://api.telegram.org".to_string()).await
    }

    pub async fn with_base_url(
        bot_token: String,
        chat_id: Option<String>,
        base_url: String,
    ) -> Result<Self> {
        let client = Client::new();
        let chat_id = match chat_id {
            Some(id) => id,
            None => {
                // Try to fetch from getUpdates
                let url = format!("{}/bot{}/getUpdates", base_url, bot_token);
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
            client,
            base_url,
        })
    }
}

#[async_trait]
impl Notifier for TelegramNotifier {
    async fn notify(&self, subject: &str, message: &str) -> Result<()> {
        let text = format!("🚨 {}\n{}", subject, message);
        let url = format!("{}/bot{}/sendMessage", self.base_url, self.bot_token);
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

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;

    #[tokio::test]
    async fn test_new_with_chat_id() {
        let n = TelegramNotifier::new("token".to_string(), Some("123".to_string()))
            .await
            .unwrap();
        assert_eq!(n.chat_id, "123");
    }

    #[tokio::test]
    async fn test_new_fetches_chat_id() {
        let server = MockServer::start();
        let m = server.mock(|when, then| {
            when.method(GET).path("/botTOKEN/getUpdates");
            then.status(200).json_body(serde_json::json!({
                "ok": true,
                "result": [{
                    "message": { "chat": { "id": 42 } }
                }]
            }));
        });

        let n = TelegramNotifier::with_base_url("TOKEN".to_string(), None, server.base_url())
            .await
            .unwrap();
        assert_eq!(n.chat_id, "42");
        m.assert();
    }

    #[tokio::test]
    async fn test_notify_sends_message() {
        let server = MockServer::start();
        let m = server.mock(|when, then| {
            when.method(POST)
                .path("/botTOKEN/sendMessage")
                .json_body(serde_json::json!({
                    "chat_id": "42",
                    "text": "🚨 subject\nbody"
                }));
            then.status(200).json_body(serde_json::json!({"ok": true}));
        });

        let n = TelegramNotifier {
            bot_token: "TOKEN".to_string(),
            chat_id: "42".to_string(),
            client: Client::new(),
            base_url: server.base_url(),
        };

        let res = n.notify("subject", "body").await;
        assert!(res.is_ok());
        m.assert();
    }

    #[test]
    fn test_name_and_health() {
        let n = TelegramNotifier {
            bot_token: "T".to_string(),
            chat_id: "C".to_string(),
            client: Client::new(),
            base_url: "http://localhost".to_string(),
        };

        assert_eq!(n.name(), "telegram");
        let (name, healthy) = n.is_healthy().unwrap();
        assert_eq!(name, "telegram");
        assert!(healthy);
    }
}
