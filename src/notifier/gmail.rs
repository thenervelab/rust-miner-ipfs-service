use crate::notifier::Notifier;
use anyhow::{Context, Result};
use async_trait::async_trait;
use lettre::{
    AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor, message::Mailbox,
    transport::smtp::authentication::Credentials,
};

/// GmailNotifier can use any async transport (real SMTP or stub for tests).
pub struct GmailNotifier<T: AsyncTransport + Send + Sync> {
    from: Mailbox,
    to: Mailbox,
    mailer: T,
}

impl GmailNotifier<AsyncSmtpTransport<Tokio1Executor>> {
    /// Create a new Gmail notifier with real Gmail SMTP transport.
    pub fn new(username: &str, app_password: &str, from: &str, to: &str) -> Result<Self> {
        let creds = Credentials::new(username.to_string(), app_password.to_string());
        let mailer = AsyncSmtpTransport::<Tokio1Executor>::relay("smtp.gmail.com")?
            .credentials(creds)
            .build();
        Ok(Self {
            from: from.parse()?,
            to: to.parse()?,
            mailer,
        })
    }
}

#[async_trait]
impl<T> Notifier for GmailNotifier<T>
where
    T: AsyncTransport + Send + Sync,
    T::Error: std::error::Error + Send + Sync + 'static,
{
    async fn notify(&self, subject: &str, message: &str) -> Result<()> {
        let email = Message::builder()
            .from(self.from.clone())
            .to(self.to.clone())
            .subject(subject)
            .body(message.to_string())?;
        self.mailer.send(email).await.context("send email")?;
        Ok(())
    }

    fn name(&self) -> &'static str {
        "gmail"
    }

    fn is_healthy(&self) -> Result<(&str, bool)> {
        Ok((self.name(), true))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lettre::transport::stub::AsyncStubTransport;

    #[tokio::test]
    async fn test_notify_sends_message() {
        let transport = AsyncStubTransport::new_ok(); // always succeeds
        let g = GmailNotifier {
            from: "from@example.com".parse().unwrap(),
            to: "to@example.com".parse().unwrap(),
            mailer: transport,
        };

        let result = g.notify("hello", "world").await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_name_and_health() {
        let g = GmailNotifier {
            from: "a@example.com".parse().unwrap(),
            to: "b@example.com".parse().unwrap(),
            mailer: AsyncStubTransport::new_ok(),
        };

        assert_eq!(g.name(), "gmail");
        let (name, healthy) = g.is_healthy().unwrap();
        assert_eq!(name, "gmail");
        assert!(healthy);
    }
}
