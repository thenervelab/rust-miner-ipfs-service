use crate::notifier::Notifier;
use anyhow::{Context, Result};
use async_trait::async_trait;
use lettre::{
    AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor, message::Mailbox,
    transport::smtp::authentication::Credentials,
};

pub struct GmailNotifier {
    from: Mailbox,
    to: Mailbox,
    mailer: AsyncSmtpTransport<Tokio1Executor>,
}

impl GmailNotifier {
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
impl Notifier for GmailNotifier {
    async fn notify(&self, subject: &str, message: &str) -> Result<()> {
        let email = Message::builder()
            .from(self.from.clone())
            .to(self.to.clone())
            .subject(subject)
            .body(message.to_string())?;
        self.mailer.send(email).await.context("send email")?;
        Ok(())
    }
}
