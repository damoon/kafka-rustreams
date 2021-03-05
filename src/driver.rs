use async_trait::async_trait;

#[async_trait]
pub trait Driver {
    async fn write_to(&self, topic: &str, msg: super::Message<super::Key, super::Value>);

    async fn stop(self);
}
