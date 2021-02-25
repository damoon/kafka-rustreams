use crate::{Message, Key, Value};

use super::Topology;
use tokio::sync::oneshot;
use tokio::{task::JoinHandle};

use async_trait::async_trait;

pub struct Driver {
    tx: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

impl Driver {
    pub fn new(_topo: Topology) -> Driver {
        use tokio::sync::oneshot::error::TryRecvError;
        // TODO
        // create kafka consumer
        // register topics

        let (tx, mut rx) = oneshot::channel::<()>();

        let task = tokio::spawn(async move {
            // until shutting down
            while let Err(TryRecvError::Empty) = rx.try_recv() {
                // begin_transaction
                // read and process messages for 100ms
                // commit
            }
        });

        Driver {
            tx,
            task,
        }
    }
}

#[async_trait]
impl<'a> super::driver::Driver for Driver {
    async fn write_to(&mut self, _topic: &str, _msg: Message<Key, Value>) {}

    async fn stop(mut self) {
        println!("shutting down");

        self.tx
            .send(())
            .expect("failed to signal shutdown");

        self.task
            .await
            .expect("failed to wait for shutdown");

        println!("shut down complete");
    }
}
