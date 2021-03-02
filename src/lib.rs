use tokio::sync::mpsc::{channel, Receiver, Sender};

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

// use rdkafka::message::{Message, OwnedMessage};
use rdkafka::message::Timestamp;

pub mod driver;
pub mod example_topologies;
pub mod in_memory;
pub mod kafka;
pub mod postgresql;
pub mod mapper;

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

    flush_tx: Vec<Sender<()>>,
    flush_needed: AtomicUsize,

    flushed_tx: Sender<()>,
    flushed_rx: Receiver<()>,

    writes_tx: Sender<Message<Key, Value>>,
    writes_rx: Receiver<Message<Key, Value>>,
}

pub struct Stream<K, V> {
    rx: Receiver<Message<K, V>>,
    appends: Sender<Message<Key, Value>>,
    flush_needed: AtomicUsize,
    flush: Receiver<()>,
    flushed: Sender<()>,
}

impl<'a> Topology {
    pub fn new() -> Topology {
        let inputs = HashMap::new();
        let flush_tx = Vec::new();
        let flush_needed = AtomicUsize::new(5);
        let (flushed_tx, flushed_rx) = channel(1);
        let (writes_tx, writes_rx) = channel(CHANNEL_BUFFER_SIZE);
        
        Topology {
            inputs,
            flush_tx,
            flush_needed,
            flushed_tx,
            flushed_rx,
            writes_tx,
            writes_rx,
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
        let (flush_tx, flush_rx) = channel(1);
        self.flush_tx.push(flush_tx);

        let (tx, rx) = channel(CHANNEL_BUFFER_SIZE);
        self.inputs.insert(topic, tx);

        Stream {
            rx,
            appends: self.writes_tx.clone(),
            flush_needed: self.flush_needed,
            flush: flush_rx,
            flushed: self.flushed_tx,
        }
    }
}

impl Stream<Key, Value> {
    pub fn write_to(mut self, topic_name: &'static str) {
        self.flush_needed.fetch_add(1, Ordering::Relaxed);

        tokio::spawn(async move {
            'outer: loop {
                tokio::select! {
                    received = self.rx.recv() => {
                        match received {
                            Some(message) => {
                                let message = message.with_topic(topic_name.to_string());
                                self.appends.send(message).await.unwrap();
                            }
                            None => {
                                break 'outer;
                            }
                        };
                    },
                    _ = self.flush.recv() => {
                        loop {
                            use std::task::Poll;
                            match self.poll_recv() {    
                                Poll::Pending => {
                                    self.flushed.send(());
                                    continue 'outer;
                                },
                                Poll::Ready(received) => {
                                    match received {
                                        Some(message) => {
                                            let message = message.with_topic(topic_name.to_string());
                                            self.appends.send(message).await.unwrap();
                                        }
                                        None => {
                                            break 'outer;
                                        }
                                    }
                                }
                            }
                        }
                    },
                }
            }
        });
    }


    fn poll_recv(&mut self) -> futures::task::Poll<Option<Message<Key, Value>>> {
        use futures::task::{noop_waker, Context};
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        self.rx.poll_recv(&mut cx)
    }
}
