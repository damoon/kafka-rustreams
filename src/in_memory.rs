use tokio::sync::mpsc::{Receiver, Sender};

use std::collections::HashMap;

use crate::Message;

use crate::{Key, Value};

use super::{StreamMessage, Topology};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct Driver {}

pub struct Application {
    inputs: HashMap<&'static str, Sender<StreamMessage<Key, Value>>>,
    flush_needed: Arc<AtomicUsize>,
    flushed_rx: Receiver<()>,

    pub writes_counter: Arc<AtomicUsize>,
}

impl Driver {
    pub fn new() -> Driver {
        Driver {}
    }

    pub fn start(self, topo: Topology) -> Application {
        let mut rx = topo.writes_rx;
        let flushed_tx = topo.flushed_tx;
        let writes_counter = Arc::new(AtomicUsize::new(0));
        let writes_counter_clone = writes_counter.clone();

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Some(StreamMessage::Message(message)) => match message.payload {
                        Some(p) => {
                            writes_counter_clone.fetch_add(1, Ordering::SeqCst);
                            //println!("{:?}", String::from_utf8(p).unwrap()),
                        }
                        None => println!("none"),
                    },
                    Some(StreamMessage::Flush) => {
                        flushed_tx.send(()).await.expect("failed to ack flush")
                    }
                    None => {
                        // channel closed
                        return;
                    }
                };
            }
        });

        Application {
            inputs: topo.inputs,
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

        let expected_acks = self.inputs.len() * self.flush_needed.load(Ordering::SeqCst);
        println!("await {} flushes", expected_acks);

        for flusher in self.inputs.iter() {
            println!("request flush");
            flusher
                .1
                .send(StreamMessage::Flush)
                .await
                .expect("failed to trigger flush");
            println!("requested flush");
        }

        for _ in 0..expected_acks {
            println!("await flush ack");
            self.flushed_rx
                .recv()
                .await
                .expect("failed to receive flush acknowledge");
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
            .send(StreamMessage::Message(msg))
            .await
            .unwrap()
    }
}
