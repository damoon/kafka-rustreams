
use super::Topology;
use tokio::sync::oneshot;
use tokio::{task::JoinHandle, time::sleep};

pub struct KafkaDriver {
    tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl KafkaDriver {
    pub fn new() -> KafkaDriver {
        KafkaDriver {
            tx: None,
            task: None,
        }
    }

    pub async fn start(&mut self) {
        use tokio::sync::oneshot;
        use tokio::sync::oneshot::error::TryRecvError;
        // TODO
        // create kafka consumer
        // register topics

        let (tx, mut rx) = oneshot::channel::<()>();
        self.tx = Some(tx);

        self.task = Some(tokio::spawn(async move {
            // until shutting down
            while let Err(TryRecvError::Empty) = rx.try_recv() {
                // begin_transaction
                // read and process messages for 100ms
                // commit
            }
        }));
    }

    pub async fn stop(mut self) {
        if self.task.is_none() || self.tx.is_none() {
            panic!("streams not running")
        }

        println!("shutting down");
        self.tx
            .unwrap()
            .send(())
            .expect("failed to signal shutdown");
        self.task
            .unwrap()
            .await
            .expect("failed to wait for shutdown");
        println!("shut down complete");
    }
}
