use rdkafka::message::{FromBytes, ToBytes};
use tokio::sync::mpsc::{channel, Receiver, Sender};

use std::collections::HashMap;

use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

// use rdkafka::message::{Message, OwnedMessage};
pub use rdkafka::message::Timestamp;

pub mod driver;
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
    Write(&'static str, Message<K, V>),
}

#[derive(Debug, Clone)]
pub struct Message<K, V> {
    pub key: Option<K>,
    pub value: Option<V>,
    pub timestamp: Timestamp,
    // headers: Option<OwnedHeaders>,
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
    inputs: HashMap<&'static str, Vec<Sender<StreamMessage<Vec<u8>, Vec<u8>>>>>,
    flush_needed: Arc<AtomicUsize>,
    writes_tx: Sender<StreamWrite<Vec<u8>, Vec<u8>>>,
    writes_rx: Receiver<StreamWrite<Vec<u8>, Vec<u8>>>,
}

impl Default for Topology {
    fn default() -> Self {
        let inputs = HashMap::new();
        let flush_needed = Arc::new(AtomicUsize::new(0));
        let (writes_tx, writes_rx) = channel(CHANNEL_BUFFER_SIZE);

        Topology {
            inputs,
            flush_needed,
            writes_tx,
            writes_rx,
        }
    }
}

impl Topology {
    pub fn read_from<
        K: 'static + Send + Debug + Clone + FromBytes<Error = &'static dyn Debug>,
        V: 'static + Send + Debug + Clone + FromBytes<Error = &'static dyn Debug>,
    >(
        &mut self,
        topic: &'static str,
    ) -> Stream<K, V> {
        let (tx, mut ri) = channel(CHANNEL_BUFFER_SIZE);

        match self.inputs.get_mut(topic) {
            None => {
                let subscriptions = vec![tx];
                self.inputs.insert(topic, subscriptions);
            }
            Some(subscriptions) => {
                subscriptions.push(tx);
            }
        };

        let (ti, rx) = channel::<StreamMessage<K, V>>(CHANNEL_BUFFER_SIZE);

        tokio::spawn(async move {
            // until shutting down
            while let Some(m) = ri.recv().await {
                match m {
                    StreamMessage::Flush => {
                        ti.send(StreamMessage::Flush)
                            .await
                            .expect("failed to forward flush message");
                    }
                    StreamMessage::Message(m) => {
                        let key = m
                            .key
                            .as_deref()
                            .map(K::from_bytes)
                            .map(|v| v.expect("key failed to decode"))
                            .cloned();
                        let value = m
                            .value
                            .as_deref()
                            .map(V::from_bytes)
                            .map(|v| v.expect("value failed to decode"))
                            .cloned();

                        ti.send(StreamMessage::Message(Message {
                            key,
                            value,
                            timestamp: m.timestamp,
                        }))
                        .await
                        .expect("failed to send decoded message");
                    }
                }
            }
        });

        Stream {
            rx,
            writes: self.writes_tx.clone(),
            flush_needed: self.flush_needed.clone(),
            //flushed: self.flushed_tx.clone(),
        }
    }

    async fn flush(
        &mut self, //         inputs: HashMap<&str, Sender<StreamMessage<Key, Value>>>,
                   //         flush_needed: Arc<AtomicUsize>,
                   //         flushed_rx: &mut Receiver<()>
    ) {
        log::debug!("flushing");

        let mut flush_signals = 0;

        for inputs in self.inputs.iter() {
            log::debug!("request flush");

            for input in inputs.1.iter() {
                flush_signals += 1;
                input
                    .send(StreamMessage::Flush)
                    .await
                    .expect("failed to trigger flush");
            }

            log::debug!("requested flush");
        }

        let expected_acks = flush_signals * self.flush_needed.load(Ordering::Relaxed);

        log::debug!("await {} flushes", expected_acks);

        for _ in 0..expected_acks {
            log::debug!("await flush ack");

            self.flushed_rx
                .recv()
                .await
                .expect("failed to receive flush acknowledge");
        }

        log::debug!("all flushes acked");
    }
}

pub struct Stream<K, V> {
    rx: Receiver<StreamMessage<K, V>>,
    writes: Sender<StreamWrite<Vec<u8>, Vec<u8>>>,
    flush_needed: Arc<AtomicUsize>,
    //flushed: Sender<()>,
}

impl<K: 'static + Send + Debug + Clone + ToBytes, V: 'static + Send + Debug + Clone + ToBytes>
    Stream<K, V>
{
    pub fn write_to(mut self, topic: &'static str) {
        self.flush_needed.fetch_add(1, Ordering::Relaxed);

        tokio::spawn(async move {
            while let Some(m) = self.rx.recv().await {
                match m {
                    StreamMessage::Flush => {
                        self.writes
                            .send(StreamWrite::Flush)
                            .await
                            .expect("failed to forward flush message");
                    }
                    StreamMessage::Message(m) => {
                        let key = m.key.map(|v| v.to_bytes().to_vec());
                        let value = m.value.map(|v| v.to_bytes().to_vec());

                        self.writes
                            .send(StreamWrite::Write(
                                topic,
                                Message {
                                    key,
                                    value,
                                    timestamp: m.timestamp,
                                },
                            ))
                            .await
                            .expect("failed to send encoded message");
                    }
                }
            }
        });
    }
}
