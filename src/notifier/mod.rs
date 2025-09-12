use anyhow::Result;
use async_trait::async_trait;

pub mod gmail;
pub mod telegram;

#[async_trait]
pub trait Notifier: Send + Sync {
    async fn notify(&self, subject: &str, message: &str) -> Result<()>;
    fn name(&self) -> &'static str;
    fn is_healthy(&self) -> Result<(&str, bool)>; // you can extend this
}

pub struct MultiNotifier {
    pub notifiers: Vec<Box<dyn Notifier>>,
}

impl MultiNotifier {
    pub fn new() -> Self {
        Self { notifiers: vec![] }
    }
    pub fn add(&mut self, n: Box<dyn Notifier>) {
        self.notifiers.push(n);
    }

    pub async fn notify_all(&self, subject: &str, message: &str) {
        for n in &self.notifiers {
            if let Err(e) = n.notify(subject, message).await {
                tracing::error!("notifier failed: {:?}", e);
            }
        }
    }

    pub fn health_check(&self) -> Vec<Result<(&str, bool)>> {
        let mut statuses = Vec::new();
        for n in self.notifiers.iter() {
            statuses.push(n.is_healthy());
        }
        statuses
    }
}

pub async fn build_notifier_from_config(cfg: &crate::settings::Settings) -> Result<MultiNotifier> {
    let mut m = MultiNotifier::new();
    if let (Some(bot), Some(chat)) = (cfg.telegram.bot_token.clone(), cfg.telegram.chat_id.clone())
    {
        let t = telegram::TelegramNotifier::new(bot, Some(chat)).await;
        match t {
            Ok(t) => m.add(Box::new(t)),
            _ => {}
        }
    } else if let Some(bot) = cfg.telegram.bot_token.clone() {
        let t = telegram::TelegramNotifier::new(bot, None).await;
        match t {
            Ok(t) => m.add(Box::new(t)),
            _ => {}
        }
    }
    if let (Some(user), Some(app_pass), Some(from), Some(to)) = (
        cfg.gmail.username.clone(),
        cfg.gmail.app_password.clone(),
        cfg.gmail.from.clone(),
        cfg.gmail.to.clone(),
    ) {
        let g = gmail::GmailNotifier::new(&user, &app_pass, &from, &to)?;
        m.add(Box::new(g));
    }
    Ok(m)
}
