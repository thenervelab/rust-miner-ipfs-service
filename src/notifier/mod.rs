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

    #[tokio::test]
    async fn test_notify_all_success() {
        let notified = Arc::new(Mutex::new(vec![]));
        let n = MockNotifier {
            name: "mock",
            should_fail: false,
            notified: notified.clone(),
        };

        let mut m = MultiNotifier::new();
        m.add(Box::new(n));

        m.notify_all("subj", "msg").await;

        let items = notified.lock().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0], ("subj".to_string(), "msg".to_string()));
    }

    #[tokio::test]
    async fn test_notify_all_error_doesnt_break() {
        let notified = Arc::new(Mutex::new(vec![]));
        let n1 = MockNotifier {
            name: "ok",
            should_fail: false,
            notified: notified.clone(),
        };
        let n2 = MockNotifier {
            name: "fail",
            should_fail: true,
            notified: notified.clone(),
        };

        let mut m = MultiNotifier::new();
        m.add(Box::new(n1));
        m.add(Box::new(n2));

        m.notify_all("s", "m").await;

        // one success, one fail
        assert_eq!(notified.lock().unwrap().len(), 1);
    }

    #[test]
    fn test_health_check() {
        let notified = Arc::new(Mutex::new(vec![]));
        let n = MockNotifier {
            name: "mock",
            should_fail: false,
            notified,
        };
        let mut m = MultiNotifier::new();
        m.add(Box::new(n));

        let statuses = m.health_check();
        assert_eq!(statuses.len(), 1);
        let (name, healthy) = statuses[0].as_ref().unwrap();
        assert_eq!(*name, "mock");
        assert!(healthy);
    }
}
