use async_std::{channel::{bounded as channel, Receiver, Sender}, task::spawn};
use std::{collections::HashMap, ops::Deref};
use std::fmt::{Debug, Error};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

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
        K: FromBytes,
        V: FromBytes,
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

        spawn(async move {
            // until shutting down
            while let Ok(m) = ri.recv().await {
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

    async fn flush(&mut self, persist: fn(topic: &str, msg: Message<Vec<u8>, Vec<u8>>) -> Result<(), Error>) {
        log::debug!("flushing");

        for inputs in self.inputs.iter() {            
            for input in inputs.1.iter() {
                log::debug!("send flush message for {}", inputs.0);

                input
                    .send(StreamMessage::Flush)
                    .await
                    .expect("failed to trigger flush");
            }
        }

        let mut expected_acks = self.flush_needed.load(Ordering::SeqCst);

        log::debug!("await {} flushes", expected_acks);

        while let Ok(m) = self.writes_rx.recv().await {
            match m {
                StreamWrite::Flush => {
                    expected_acks -= 1;
                    log::debug!("await {} flushes", expected_acks);
                    if expected_acks == 0 {
                        return
                    }
                },
                StreamWrite::Write(topic, msg) => {
                    persist(topic, msg).expect("failed to persist message")
                },
            }
        }

        panic!("writes channel was closed during flush")
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
        self.flush_needed.fetch_add(1, Ordering::SeqCst);

        spawn(async move {
            while let Ok(m) = self.rx.recv().await {
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


/// A cheap conversion from a byte slice to typed data.
///
/// Given a reference to a byte slice, returns a different view of the same
/// data. No allocation is performed, however the underlying data might be
/// checked for correctness (for example when converting to `str`).
///
/// See also the [`ToBytes`] trait.
pub trait FromBytes {
    /// The error type that will be returned if the conversion fails.
    type Error;
    /// Tries to convert the provided byte slice into a different type.
    fn from_bytes(_: &[u8]) -> Result<&Self, Self::Error>;
}

impl FromBytes for [u8] {
    type Error = ();
    fn from_bytes(bytes: &[u8]) -> Result<&Self, Self::Error> {
        Ok(bytes)
    }
}

impl FromBytes for str {
    type Error = std::str::Utf8Error;
    fn from_bytes(bytes: &[u8]) -> Result<&Self, Self::Error> {
        std::str::from_utf8(bytes)
    }
}

/// A cheap conversion from typed data to a byte slice.
///
/// Given some data, returns the byte representation of that data.
/// No copy of the data should be performed.
///
/// See also the [`FromBytes`] trait.
pub trait ToBytes {
    /// Converts the provided data to bytes.
    fn to_bytes(&self) -> &[u8];
}

impl ToBytes for [u8] {
    fn to_bytes(&self) -> &[u8] {
        self
    }
}

impl ToBytes for str {
    fn to_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl ToBytes for Vec<u8> {
    fn to_bytes(&self) -> &[u8] {
        self.as_slice()
    }
}

impl ToBytes for String {
    fn to_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<'a, T: ToBytes> ToBytes for &'a T {
    fn to_bytes(&self) -> &[u8] {
        (*self).to_bytes()
    }
}

impl ToBytes for () {
    fn to_bytes(&self) -> &[u8] {
        &[]
    }
}

// Implement to_bytes for arrays - https://github.com/rust-lang/rfcs/issues/1038
macro_rules! array_impls {
    ($($N:expr)+) => {
        $(
            impl ToBytes for [u8; $N] {
                fn to_bytes(&self) -> &[u8] { self }
            }
         )+
    }
}

array_impls! {
     0  1  2  3  4  5  6  7  8  9
    10 11 12 13 14 15 16 17 18 19
    20 21 22 23 24 25 26 27 28 29
    30 31 32
}