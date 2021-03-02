use tokio::sync::mpsc::{Sender, Receiver};

use std::collections::HashMap;

use crate::Message;

use crate::{Key, Value};

use super::Topology;

use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;

pub struct Driver {
}

pub struct Application {
    inputs: HashMap<&'static str, Sender<Message<Key, Value>>>,
    flush_tx: Vec<Sender<()>>,
    flush_needed: AtomicUsize,
    flushed_rx: Receiver<()>,
}

impl Driver {
    pub fn new(topo: Topology) -> Driver {
        Driver {
        }
    }

    fn start (topo: Topology) -> Application {
        let mut rx = topo.writes_rx;

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Some(message) => match message.payload {
                        Some(p) => print!("{:?}", String::from_utf8(p).unwrap()),
                        None => println!("none"),
                    },
                    None => {
                        // channel closed
                        return;
                    }
                };
            }
        });

        Application {
            inputs: topo.inputs,
            flush_tx: topo.flush_tx,
            flush_needed: topo.flush_needed,
            flushed_rx: topo.flushed_rx,
        }
    }
}

//impl super::driver::Driver for Driver {
//#[async_trait]
impl Application {

    async fn flush(mut self) {
        let expected_acks = self.flush_tx.len() * self.flush_needed.load(Ordering::Relaxed);

        for flusher in self.flush_tx {
            flusher.send(()).await.expect("failed to trigger flush");
        }

        for _ in 0..expected_acks {
            self.flushed_rx.recv().await.expect("failed to receive flush acknowledge");
        }
    }

    async fn stop(self) {
        self.flush().await;
    }

    async fn write_to(&mut self, topic_name: &str, msg: Message<Key, Value>) {
        self.inputs
            .get(topic_name)
            .unwrap()
            .send(msg)
            .await
            .unwrap()
    }
}
