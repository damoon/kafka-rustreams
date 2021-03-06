use crate::{Key, Message, Value};

use super::Topology;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use async_trait::async_trait;

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
impl<'a> super::driver::Driver for Driver {
    async fn write_to(&self, _topic: &str, _msg: Message<Key, Value>) {}

    async fn stop(mut self) {
        println!("shutting down");

        self.tx.send(()).expect("failed to signal shutdown");

        self.task.await.expect("failed to wait for shutdown");

        println!("shut down complete");
    }
}
