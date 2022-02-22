use crate::{Message};

use super::super::Topology;
use rdkafka::message::ToBytes;

use async_trait::async_trait;

use rdkafka::config::ClientConfig;
use std::collections::HashMap;

pub struct Driver {
}

impl Driver {
    pub fn new(_topo: Topology) -> Driver {
        Driver {  }
    }
}

#[async_trait]
impl super::Driver for Driver {
    async fn write<K: ToBytes+ std::marker::Send, V: ToBytes + std::marker::Send>(&self, _topic: &str, _msg: Message<K, V>) {
        // TODO
    }

    async fn stop(mut self) {
        println!("shutting down");

        // self.tx.send(()).expect("failed to signal shutdown");

        // self.task.await.expect("failed to wait for shutdown");

        println!("shut down complete");
    }
}

pub fn consumer_config(
    bootstrap_servers: &str,
    group_id: &str,
    config_overrides: Option<HashMap<&str, &str>>,
) -> ClientConfig {
    let mut config = ClientConfig::new();

    config.set("group.id", group_id);
    config.set("client.id", "rdkafka_integration_test_client");
    config.set("group.instance.id", "testing-node");
    config.set("bootstrap.servers", bootstrap_servers);
    config.set("enable.partition.eof", "false");
    config.set("session.timeout.ms", "6000");
    config.set("enable.auto.commit", "false");
    config.set("statistics.interval.ms", "500");
    config.set("api.version.request", "true");
    config.set("debug", "all");
    config.set("auto.offset.reset", "earliest");

    if let Some(overrides) = config_overrides {
        for (key, value) in overrides {
            config.set(key, value);
        }
    }

    config
}
