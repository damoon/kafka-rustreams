use tokio::sync::mpsc::{Receiver, Sender};

use super::{Key, Message, StreamMessage, Topology, Value};

use std::{collections::HashMap, sync::Mutex};

use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use async_trait::async_trait;

pub struct Driver {
    inputs: HashMap<&'static str, Sender<StreamMessage<Key, Value>>>,
    flush_needed: Arc<AtomicUsize>,
    flushed_rx: Receiver<()>,
    reflush_needed: Arc<Mutex<bool>>,
}

#[async_trait]
impl super::driver::Driver for Driver {
    async fn stop(mut self) {
        log::debug!("stopping in memory driver");

        loop {
            {
                let mut reflush_needed = self.reflush_needed.lock().unwrap();
                *reflush_needed = false;
            }

            super::driver::flush(
                self.inputs.clone(),
                self.flush_needed.clone(),
                &mut self.flushed_rx,
            )
            .await;

            let reflush_required: bool;
            {
                reflush_required = *self.reflush_needed.lock().unwrap();
            }
            if reflush_required {
                log::debug!("reflush required");
            } else {
                return;
            }
        }
    }

    async fn write(&self, message: Message<Key, Value>) {
        log::debug!("write test message to topic {}", message.topic);

        let topic_name = message.topic.clone();

        self.inputs
            .get(topic_name.as_str())
            .expect(format!("failed to look up input stream {}", topic_name).as_str())
            .send(StreamMessage::Message(message))
            .await
            .expect("failed to write message")
    }
}

impl Driver {
    pub fn start(topo: Topology) -> Driver {
        log::debug!("start in memory driver");

        let flushed_tx = topo.flushed_tx;
        let mut writes_rx = topo.writes_rx;

        let inputs = topo.inputs.clone();
        let reflush_needed = Arc::new(Mutex::new(false));
        let reflush_needed_ref = reflush_needed.clone();

        tokio::spawn(async move {
            loop {
                match writes_rx.recv().await {
                    Some(StreamMessage::Message(message)) => {
                        log::debug!("message created for topic {}", message.topic);

                        let topic_name = message.topic.clone();
                        let topic = inputs.get(topic_name.as_str());
                        match topic {
                            None => log::debug!("droping message for topic {}", message.topic),
                            Some(sender) => {
                                sender
                                    .send(StreamMessage::Message(message))
                                    .await
                                    .expect("failed to write message");
                            }
                        }

                        let mut reflush_needed = reflush_needed_ref.lock().unwrap();
                        *reflush_needed = true;
                    }
                    Some(StreamMessage::Flush) => {
                        flushed_tx.send(()).await.expect("failed to ack flush")
                    }
                    None => {
                        log::debug!("in memory driver write thread stopped");

                        return;
                    }
                };
            }
        });

        Driver {
            inputs: topo.inputs,
            flush_needed: topo.flush_needed,
            flushed_rx: topo.flushed_rx,
            reflush_needed,
        }
    }
}
