
use super::Topology;
use tokio::sync::oneshot;
use tokio::{task::JoinHandle, time::sleep};

use async_trait::async_trait;

pub struct Driver {
    tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl Driver {
    pub fn new(topo: Topology) -> Driver {
        Driver {
            tx: None,
            task: None,
        }
    }
}

#[async_trait]
impl<'a> super::driver::Driver<'a> for Driver {
    fn write_to(&mut self, topic: &str, msg: Option<&'a [u8]>) {}

    fn await_end_of_topic(&mut self) {}
    
    async fn start(&mut self) {
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

    async fn stop(mut self) {
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
