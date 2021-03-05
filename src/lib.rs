use tokio::sync::mpsc::{channel, Receiver, Sender};

use std::collections::HashMap;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

// use rdkafka::message::{Message, OwnedMessage};
use rdkafka::message::Timestamp;

pub mod driver;
pub mod example_topologies;
pub mod in_memory;
pub mod kafka;
pub mod mapper;
pub mod postgresql;

const CHANNEL_BUFFER_SIZE: usize = 1_000;

#[derive(Debug, Clone)]
enum StreamMessage<K, V> {
    Flush,
    Message(Message<K, V>),
}

#[derive(Debug, Clone)]
pub struct Message<K, V> {
    pub payload: Option<V>,
    pub key: Option<K>,
    pub topic: String,
    pub timestamp: Timestamp,
    pub partition: i32,
    pub offset: i64,
    // headers: Option<OwnedHeaders>,
}

pub fn new_message(
    topic: String,
    key: Option<String>,
    value: Option<String>,
) -> Message<Key, Value> {
    let key = match key {
        None => None,
        Some(key) => Some(key.as_bytes().to_vec()),
    };
    let payload = match value {
        None => None,
        Some(value) => Some(value.as_bytes().to_vec()),
    };

    Message {
        payload,
        key,
        topic,
        timestamp: Timestamp::NotAvailable,
        partition: 0,
        offset: 0,
    }
}

impl<K, V> Message<K, V> {
    /// Creates a new message with the specified content.
    ///
    /// This function is mainly useful in tests of `rust-rdkafka` itself.
    pub fn new(
        payload: Option<V>,
        key: Option<K>,
        topic: String,
        timestamp: Timestamp,
        partition: i32,
        offset: i64,
        // headers: Option<OwnedHeaders>,
    ) -> Message<K, V> {
        Message {
            payload,
            key,
            topic,
            timestamp,
            partition,
            offset,
            // headers,
        }
    }
}

trait WithChange<K, V> {
    fn with_value(self, payload: Option<V>) -> Message<K, V>;
}

trait WithTopic<K, V> {
    fn with_topic(self, topic: String) -> Message<K, V>;
}

impl<K, V1, V2> WithChange<K, V2> for Message<K, V1> {
    fn with_value(self, payload: Option<V2>) -> Message<K, V2> {
        Message::<K, V2> {
            payload: payload,
            key: self.key,
            topic: self.topic,
            timestamp: self.timestamp,
            partition: self.partition,
            offset: self.offset,
            // headers,
        }
    }
}

impl<K, V> WithTopic<K, V> for Message<K, V> {
    fn with_topic(self, topic: String) -> Message<K, V> {
        Message::<K, V> {
            payload: self.payload,
            key: self.key,
            topic: topic,
            timestamp: self.timestamp,
            partition: self.partition,
            offset: self.offset,
            // headers,
        }
    }
}

pub struct Topology {
    inputs: HashMap<&'static str, Sender<StreamMessage<Key, Value>>>,

    flush_needed: Arc<AtomicUsize>,

    flushed_tx: Sender<()>,
    flushed_rx: Receiver<()>,

    writes_tx: Sender<StreamMessage<Key, Value>>,
    writes_rx: Receiver<StreamMessage<Key, Value>>,
}

impl<'a> Topology {
    pub fn new() -> Topology {
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

    pub fn read_from(&mut self, topic: &'static str) -> Stream<Key, Value> {
        let (tx, rx) = channel(CHANNEL_BUFFER_SIZE);
        self.inputs.insert(topic, tx);

        Stream {
            rx,
            appends: self.writes_tx.clone(),
            flush_needed: self.flush_needed.clone(),
            flushed: self.flushed_tx.clone(),
        }
    }
}

pub struct Stream<K, V> {
    rx: Receiver<StreamMessage<K, V>>,
    appends: Sender<StreamMessage<Key, Value>>,
    flush_needed: Arc<AtomicUsize>,
    flushed: Sender<()>,
}

type Key = Vec<u8>;
type Value = Vec<u8>;

impl Stream<Key, Value> {
    pub fn write_to(mut self, topic_name: &'static str) {
        self.flush_needed.fetch_add(1, Ordering::Relaxed);

        tokio::spawn(async move {
            loop {
                match self.rx.recv().await {
                    Some(StreamMessage::Message(message)) => {
                        log::debug!("write message to topic {}", topic_name);

                        let message = message.with_topic(topic_name.to_string());

                        self.appends
                            .send(StreamMessage::Message(message))
                            .await
                            .expect("failed to forward message");
                    }
                    Some(StreamMessage::Flush) => {
                        if let Err(e) = self.appends.send(StreamMessage::Flush).await {
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
