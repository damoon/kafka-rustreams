use tokio::sync::mpsc::{Sender, Receiver};

use std::collections::HashMap;

use crate::Message;

use crate::{Key, Value};

use super::Topology;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct Driver {
}

pub struct Application {
    inputs: HashMap<&'static str, Sender<Message<Key, Value>>>,
    flush_tx: Vec<Sender<()>>,
    flush_needed: Arc<AtomicUsize>,
    flushed_rx: Receiver<()>,

    pub writes_counter: Arc<AtomicUsize>,
}

impl Driver {
    pub fn new() -> Driver {
        Driver {
        }
    }

    pub fn start (self, topo: Topology) -> Application {
        let mut rx = topo.writes_rx;
        let writes_counter = Arc::new(AtomicUsize::new(0));
        let writes_counter_clone = writes_counter.clone();

        tokio::spawn(async move {
            loop {
                // TODO flush does not include this step
                match rx.recv().await {
                    Some(message) => match message.payload {
                        Some(p) => {
                            writes_counter_clone.fetch_add(1, Ordering::SeqCst);
                            //println!("{:?}", String::from_utf8(p).unwrap()),
                        },
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

            writes_counter,
        }
    }
}

//impl super::driver::Driver for Driver {
//#[async_trait]
impl Application {

    pub async fn flush(&mut self) {
        println!("flushing");

        let expected_acks = self.flush_tx.len() * self.flush_needed.load(Ordering::SeqCst);
        println!("await {} flushes", expected_acks);

        for flusher in self.flush_tx.iter() {
            println!("request flush");
            flusher.send(()).await.expect("failed to trigger flush");
            println!("requested flush");
        }

        for _ in 0..expected_acks {
            println!("await flush ack");
            self.flushed_rx.recv().await.expect("failed to receive flush acknowledge");
        }
        println!("all flushes acked");
    }

    pub async fn stop(&mut self) {
        self.flush().await;
    }

    pub async fn write_to(&mut self, topic_name: &str, msg: Message<Key, Value>) {
        self.inputs
            .get(topic_name)
            .unwrap()
            .send(msg)
            .await
            .unwrap()
    }
}
