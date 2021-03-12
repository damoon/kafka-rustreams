use tokio::sync::mpsc::{Receiver, Sender};

use super::{Key, Message, StreamMessage, Topology, Value};

use std::{collections::HashMap, sync::Mutex};

use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use async_trait::async_trait;

pub struct Driver {
    created_messages: Arc<Mutex<Vec<Message<Key, Value>>>>,
    inputs: HashMap<&'static str, Sender<StreamMessage<Key, Value>>>,
    flush_needed: Arc<AtomicUsize>,
    flushed_rx: Receiver<()>,
}

#[cfg(test)]
mod tests {
    use super::super::driver::Driver;

    #[tokio::test]
    async fn start_stop_driver() {
        process_messages(0).await
    }

    #[tokio::test]
    async fn copy_one_message() {
        process_messages(1).await
    }

    #[tokio::test]
    async fn copy_fortytwo_messages() {
        process_messages(42).await
    }

    #[tokio::test]
    async fn copy_10k_messages() {
        process_messages(10_000).await
    }

    async fn process_messages(n: usize) {
        let mut topology = super::Topology::default();
        topology.read_from("input_topic").write_to("output_topic");

        let mut driver = super::Driver::start(topology);

        for n in 0..n {
            let msg = super::super::new_message(
                "input_topic".to_string(),
                None,
                Some(format!("hello world {}", n)),
            );

            driver.write(msg).await;
        }

        let messages = driver.created_messages().await;

        driver.stop().await;

        assert_eq!(n, messages.len(), "number of copied messages");
    }
}

#[async_trait]
impl super::driver::Driver for Driver {
    async fn stop(mut self) {
        log::debug!("stopping in memory driver");

        super::driver::flush(
            self.inputs.clone(),
            self.flush_needed.clone(),
            &mut self.flushed_rx,
        )
        .await;
    }

    async fn write(&self, message: Message<Key, Value>) {
        log::debug!("write test message to topic {}", message.topic);

        let topic_name = message.topic.clone();

        self.inputs
            .get(topic_name.as_str())
            .unwrap_or_else(|| panic!("failed to look up input stream {}", topic_name))
            .send(StreamMessage::Message(message))
            .await
            .expect("failed to write message")
    }
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

    pub async fn created_messages(&mut self) -> Vec<Message<Key, Value>> {
        log::debug!("stopping in memory driver");

        super::driver::flush(
            self.inputs.clone(),
            self.flush_needed.clone(),
            &mut self.flushed_rx,
        )
        .await;

        self.created_messages.lock().unwrap().to_owned()
    }
}
