use tokio::sync::mpsc::{channel, Receiver, Sender};

use std::collections::HashMap;

// use rdkafka::message::{Message, OwnedMessage};
use rdkafka::message::Timestamp;

pub mod driver;
pub mod example_topologies;
pub mod in_memory;
pub mod kafka;
pub mod postgresql;

const CHANNEL_BUFFER_SIZE: usize = 1;

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

type Key = Vec<u8>;
type Value = Vec<u8>;

pub struct Topology {
    inputs: HashMap<&'static str, Sender<Message<Key, Value>>>,
    writes_sink: Sender<Message<Key, Value>>,
    writes_source: Receiver<Message<Key, Value>>,
}

pub struct Stream<K, V> {
    rx: Receiver<Message<K, V>>,
    appends: Sender<Message<Key, Value>>,
}

impl<'a> Topology {
    pub fn new() -> Topology {
        let inputs = HashMap::new();
        let (writes_sink, writes_source) = channel(CHANNEL_BUFFER_SIZE);
        Topology {
            inputs,
            writes_sink,
            writes_source,
        }
    }

    pub async fn process_message(self, topic_name: &str, msg: Message<Key, Value>) {
        let topic = self.inputs.get(topic_name);
        match topic {
            None => (),
            Some(sender) => {
                sender.send(msg).await.unwrap();
            }
        }
    }

    pub fn read_from(&mut self, topic: &'static str) -> Stream<Key, Value> {
        let (tx, rx) = channel(CHANNEL_BUFFER_SIZE);
        self.inputs.insert(topic, tx);
        Stream {
            rx,
            appends: self.writes_sink.clone(),
        }
    }
}

impl Stream<Key, Value> {
    pub fn write_to(mut self, topic_name: &'static str) {
        tokio::spawn(async move {
            loop {
                match self.rx.recv().await {
                    Some(message) => {
                        let message = message.with_topic(topic_name.to_string());
                        self.appends.send(message).await.unwrap();
                    }
                    None => {
                        println!("closed 1");
                        return;
                    }
                };
            }
        });
    }
}

/*
pub trait Mapper<'a, V1, V2> {
    fn map(self, m: impl Send + 'static + Fn(V1) -> V2) -> Stream<V2>;
}

impl<'a, V1: Send + 'static, V2: Send + 'static> Mapper<'a, V1, V2> for Stream<V1> {
    fn map(mut self, map: impl Send + 'static + Fn(V1) -> V2) -> Stream<V2> {
        let (tx, rx) = channel::<V2>(1);

        tokio::spawn(async move {
            let ten_millis = std::time::Duration::from_millis(10);
            std::thread::sleep(ten_millis);

            use futures::task::Poll;
            match self.poll_recv() {
                Poll::Pending => {
                    println!("pending")
                }
                Poll::Ready(Some(message)) => {
                    println!("msg");
                    let new_message = map(message);
                    if let Err(e) = tx.send(new_message).await {
                        panic!("failed to send: {}", e);
                    }
                }
                Poll::Ready(None) => {
                    println!("closed")
                }
            };
        });

        Stream { rx }
    }
}
*/
