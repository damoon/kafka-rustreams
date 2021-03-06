use tokio::sync::mpsc::{Receiver, Sender};

use super::{Key, Message, StreamMessage, Topology, Value};

use std::{collections::HashMap, sync::Mutex};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct Driver {
    created_messages: Arc<Mutex<Vec<Message<Key, Value>>>>,
    inputs: HashMap<&'static str, Sender<StreamMessage<Key, Value>>>,
    flush_needed: Arc<AtomicUsize>,
    flushed_rx: Receiver<()>,
}

impl Driver {
    pub fn start(topo: Topology) -> Driver {
        log::debug!("start in memory driver");

        let created_messages = Arc::new(Mutex::new(Vec::new()));

        let flushed_tx = topo.flushed_tx;
        let mut writes_rx = topo.writes_rx;
        let created_messages_clone = created_messages.clone();

        tokio::spawn(async move {
            loop {
                match writes_rx.recv().await {
                    Some(StreamMessage::Message(message)) => {
                        log::debug!("caught created message");

                        created_messages_clone.lock().unwrap().push(message.clone());
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
            created_messages,
            inputs: topo.inputs,
            flush_needed: topo.flush_needed,
            flushed_rx: topo.flushed_rx,
        }
    }

    async fn flush(&mut self) {
        log::debug!("flushing");

        let expected_acks = self.inputs.len() * self.flush_needed.load(Ordering::Relaxed);

        log::debug!("await {} flushes", expected_acks);

        for flusher in self.inputs.iter() {
            log::debug!("request flush");

            flusher
                .1
                .send(StreamMessage::Flush)
                .await
                .expect("failed to trigger flush");

            log::debug!("requested flush");
        }

        for _ in 0..expected_acks {
            log::debug!("await flush ack");

            self.flushed_rx
                .recv()
                .await
                .expect("failed to receive flush acknowledge");
        }

        log::debug!("all flushes acked");
    }

    pub async fn stop(mut self) -> Vec<Message<Key, Value>> {
        log::debug!("stopping in memory driver");

        self.flush().await;

        self.created_messages.lock().unwrap().to_owned()
    }

    pub async fn write_to(&mut self, message: Message<Key, Value>) {
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
