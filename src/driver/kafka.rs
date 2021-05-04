use crate::{Key, Message, Value};

use super::super::Topology;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use async_trait::async_trait;

use rdkafka::config::ClientConfig;
use std::collections::HashMap;

pub struct Driver {
    tx: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

impl Driver {
    pub fn new(_topo: Topology) -> Driver {
        use tokio::sync::oneshot::error::TryRecvError;
        // TODO: create kafka consumer

        // TODO: register topics

        let (tx, mut rx) = oneshot::channel::<()>();

        let task = tokio::spawn(async move {
            // until shutting down
            while let Err(TryRecvError::Empty) = rx.try_recv() {
                // TODO: begin_transaction

                // TODO: for 100ms or
                // TODO: until all streams reached eof
                // TODO: read and process messages

                // TODO: commit
            }
        });

        Driver { tx, task }
    }
}

#[async_trait]
impl super::Driver for Driver {
    async fn write(&self, _msg: Message<Key, Value>) {
        // TODO
    }

    async fn stop(mut self) {
        println!("shutting down");

        self.tx.send(()).expect("failed to signal shutdown");

        self.task.await.expect("failed to wait for shutdown");

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
