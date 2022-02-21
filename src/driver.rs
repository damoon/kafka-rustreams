use async_trait::async_trait;
use rdkafka::message::ToBytes;
use std::marker::Send;

pub mod kafka;

#[async_trait]
pub trait Driver {
    async fn write<K: ToBytes + Send, V: ToBytes + Send>(&self, topic: &str,  msg: super::Message<K, V>);

    async fn stop(self);
}
