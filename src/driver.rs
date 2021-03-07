use async_trait::async_trait;

#[async_trait]
pub trait Driver {
    async fn write(&self, msg: super::Message<super::Key, super::Value>);

    async fn stop(self);
}
