use tokio::sync::mpsc::{Receiver, Sender};

use crate::StreamWrite;

use super::super::{Message, Topology};
use super::{Key, StreamMessage, Value};

use std::collections::HashMap;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;

pub struct Driver {
    inputs: HashMap<&'static str, Sender<StreamMessage<Key, Value>>>,
    flush_needed: Arc<AtomicUsize>,
    flushed_rx: Receiver<()>,
    reflush_needed: Arc<AtomicBool>,
}

#[async_trait]
impl super::Driver for Driver {
    async fn stop(mut self) {
        log::debug!("stopping in memory driver");

        loop {
            self.reflush_needed.store(false, Ordering::SeqCst);

            super::flush(
                self.inputs.clone(),
                self.flush_needed.clone(),
                &mut self.flushed_rx,
            )
            .await;

            if self.reflush_needed.load(Ordering::SeqCst) {
                log::debug!("reflush required");
            } else {
                return;
            }
        }
    }

    async fn write(&self, topic: &str, message: Message<Key, Value>) {
        self.inputs
            .get(topic)
            .unwrap_or_else(|| panic!("failed to look up input stream {}", topic))
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
        let reflush_needed = Arc::new(AtomicBool::new(false));
        let reflush_needed_ref = reflush_needed.clone();

        tokio::spawn(async move {
            loop {
                match writes_rx.recv().await {
                    Some(StreamWrite::Write(topic_name, message)) => {
                        let topic = inputs.get(topic_name.as_str());
                        match topic {
                            None => log::debug!("droping message for topic {topic_name}"),
                            Some(sender) => {
                                sender
                                    .send(StreamMessage::Message(message))
                                    .await
                                    .expect("failed to write message");
                            }
                        }

                        reflush_needed_ref.store(true, Ordering::SeqCst);
                    }
                    Some(StreamWrite::Flush) => {
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
