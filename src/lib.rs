use tokio::sync::mpsc::{channel, Receiver, Sender};

use std::collections::HashMap;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

// use rdkafka::message::{Message, OwnedMessage};
pub use rdkafka::message::Timestamp;

pub mod driver;
pub mod example_topologies;
pub mod mapper;

const CHANNEL_BUFFER_SIZE: usize = 1_000;

#[derive(Debug, Clone)]
enum StreamMessage<K, V> {
    Flush,
    Message(Message<K, V>),
}

#[derive(Debug, Clone)]
enum StreamWrite<K, V> {
    Flush,
    Write(String, Message<K, V>),
}

#[derive(Debug, Clone,)]
pub struct Message<K, V> {
    pub key: Option<K>,
    pub value: Option<V>,
    pub timestamp: Timestamp,
    // headers: Option<OwnedHeaders>,
}

pub fn new_message(
    key: Option<String>,
    value: Option<String>,
) -> Message<Key, Value> {
    let key = key.map(|key| key.as_bytes().to_vec());
    let value = value.map(|value| value.as_bytes().to_vec());

    Message {
        key,
        value,
        timestamp: Timestamp::NotAvailable,
    }
}

impl<K, V> Message<K, V> {
    fn with_value<W>(self, value: Option<W>) -> Message<K, W> {
        Message::<K, W> {
            key: self.key,
            value,
            timestamp: self.timestamp,
        }
    }
}

pub struct Topology {
    inputs: HashMap<&'static str, Sender<StreamMessage<Key, Value>>>,

    flush_needed: Arc<AtomicUsize>,

    flushed_tx: Sender<()>,
    flushed_rx: Receiver<()>,

    writes_tx: Sender<StreamWrite<Key, Value>>,
    writes_rx: Receiver<StreamWrite<Key, Value>>,
}

impl Default for Topology {
    fn default() -> Self {
            let inputs = HashMap::new();
            let flush_needed = Arc::new(AtomicUsize::new(0));
            let (flushed_tx, flushed_rx) = channel(1);
            let (writes_tx, writes_rx) = channel(CHANNEL_BUFFER_SIZE);
    
            Topology {
                inputs,
                flush_needed,
                flushed_tx,
                flushed_rx,
                writes_tx,
                writes_rx,
            }
    }
}

impl<'a> Topology {
    pub fn read_from(&mut self, topic: &'static str) -> Stream<Key, Value> {
        let (tx, rx) = channel(CHANNEL_BUFFER_SIZE);
        self.inputs.insert(topic, tx);

        Stream {
            rx,
            writes: self.writes_tx.clone(),
            flush_needed: self.flush_needed.clone(),
            //flushed: self.flushed_tx.clone(),
        }
    }
}

pub struct Stream<K, V> {
    rx: Receiver<StreamMessage<K, V>>,
    writes: Sender<StreamWrite<Key, Value>>,
    flush_needed: Arc<AtomicUsize>,
    //flushed: Sender<()>,
}

type Key = Vec<u8>;
type Value = Vec<u8>;

impl Stream<Key, Value> {
    pub fn write_to(mut self, topic: &str) {
        self.flush_needed.fetch_add(1, Ordering::Relaxed);
        let topic = topic.to_string();

        tokio::spawn(async move {
            loop {
                match self.rx.recv().await {
                    Some(StreamMessage::Message(message)) => {
                        self.writes
                            .send(StreamWrite::Write(topic.clone(), message))
                            .await
                            .expect("failed to forward message");
                    }
                    Some(StreamMessage::Flush) => {
                        if let Err(e) = self.writes.send(StreamWrite::Flush).await {
                            panic!("failed to forward flush: {}", e);
                        }
                    }
                    None => {
                        log::debug!("write to topic thread stopped");

                        return;
                    }
                };
            }
        });
    }
}
