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
                tracing::error!("notifier failed: {:?} {} {}", e, subject, message);
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

//          //          //          //          //          //          //          //          //          //          //          //

//                      //                      //                      //                                  //                      //

//                      //                      //          //          //          //                      //                      //

//                      //                      //                                  //                      //                      //

//                      //                      //          //          //          //                      //                      //

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

    struct MockNotifier {
        name: &'static str,
        should_fail: bool,
        notified: Arc<Mutex<Vec<(String, String)>>>,
    }

    #[async_trait]
    impl Notifier for MockNotifier {
        async fn notify(&self, subject: &str, message: &str) -> Result<()> {
            if self.should_fail {
                anyhow::bail!("forced failure");
            }
            self.notified
                .lock()
                .unwrap()
                .push((subject.to_string(), message.to_string()));
            Ok(())
        }

        fn name(&self) -> &'static str {
            self.name
        }

        fn is_healthy(&self) -> Result<(&str, bool)> {
            Ok((self.name, !self.should_fail))
        }
    }

    fn dummy_settings() -> crate::settings::Settings {
        crate::settings::Settings {
            service: Default::default(),
            db: Default::default(),
            ipfs: Default::default(),
            substrate: Default::default(),
            telegram: Default::default(),
            gmail: Default::default(),
            monitoring: Default::default(),
        }
    }

    fn make_notifier(
        name: &'static str,
        should_fail: bool,
        notified: &Arc<Mutex<Vec<(String, String)>>>,
    ) -> Box<dyn Notifier> {
        Box::new(MockNotifier {
            name,
            should_fail,
            notified: notified.clone(),
        })
    }

    fn notified_messages(notified: &Arc<Mutex<Vec<(String, String)>>>) -> Vec<(String, String)> {
        notified.lock().unwrap().clone()
    }

    #[tokio::test]
    async fn test_notify_all_success() {
        let notified = Arc::new(Mutex::new(vec![]));
        let mut m = MultiNotifier::new();
        m.add(make_notifier("ok", false, &notified));

        m.notify_all("subj", "msg").await;

        let items = notified_messages(&notified);
        assert_eq!(items, vec![("subj".into(), "msg".into())]);
    }

    #[tokio::test]
    async fn test_notify_all_error_doesnt_break() {
        let notified = Arc::new(Mutex::new(vec![]));
        let mut m = MultiNotifier::new();
        m.add(make_notifier("ok", false, &notified));
        m.add(make_notifier("fail", true, &notified));

        m.notify_all("s", "m").await;

        // one success, one fail
        assert_eq!(notified_messages(&notified).len(), 1);
    }

    #[tokio::test]
    async fn test_notify_all_empty() {
        let m = MultiNotifier::new();
        m.notify_all("irrelevant", "irrelevant").await; // should not panic
        assert!(m.health_check().is_empty());
    }

    #[tokio::test]
    async fn test_notify_all_multiple_success() {
        let notified = Arc::new(Mutex::new(vec![]));
        let mut m = MultiNotifier::new();
        m.add(make_notifier("n1", false, &notified));
        m.add(make_notifier("n2", false, &notified));

        m.notify_all("s", "m").await;

        assert_eq!(notified_messages(&notified).len(), 2);
    }

    #[test]
    fn test_health_check_success() {
        let notified = Arc::new(Mutex::new(vec![]));
        let mut m = MultiNotifier::new();
        m.add(make_notifier("healthy", false, &notified));

        let binding = m.health_check();

        let (name, healthy) = binding[0].as_ref().unwrap();
        assert_eq!(*name, "healthy");
        assert!(healthy);
    }

    #[test]
    fn test_health_check_failure() {
        let notified = Arc::new(Mutex::new(vec![]));
        let mut m = MultiNotifier::new();
        m.add(make_notifier("unhealthy", true, &notified));

        let binding = m.health_check();
        let (name, healthy) = binding[0].as_ref().unwrap();
        assert_eq!(*name, "unhealthy");
        assert!(!healthy);
    }

    #[tokio::test]
    async fn test_build_notifier_from_config_empty() {
        let cfg = dummy_settings();
        let m = build_notifier_from_config(&cfg).await.unwrap();
        assert!(m.notifiers.is_empty());
    }

    #[tokio::test]
    async fn test_build_notifier_from_config_with_telegram_token_only() {
        let mut cfg = dummy_settings();
        cfg.telegram.bot_token = Some("fake-token".into());

        let result = build_notifier_from_config(&cfg).await;
        assert!(result.is_ok()); // should build MultiNotifier without panic
        // not asserting non-empty because TelegramNotifier::new may fail
    }

    #[tokio::test]
    async fn test_build_notifier_from_config_with_telegram_token_and_chat() {
        let mut cfg = dummy_settings();
        cfg.telegram.bot_token = Some("fake-token".into());
        cfg.telegram.chat_id = Some("fake-chat".into());

        let m = build_notifier_from_config(&cfg).await.unwrap();
        assert!(!m.notifiers.is_empty());
    }

    #[tokio::test]
    async fn test_build_notifier_from_config_with_gmail() {
        let mut cfg = dummy_settings();
        cfg.gmail.username = Some("user".into());
        cfg.gmail.app_password = Some("app-pass".into());
        cfg.gmail.from = Some("from@example.com".into());
        cfg.gmail.to = Some("to@example.com".into());

        let m = build_notifier_from_config(&cfg).await.unwrap();
        assert!(!m.notifiers.is_empty());
    }
}
