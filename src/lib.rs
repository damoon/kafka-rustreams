use core::panic;
use std::{borrow::Borrow, option, time::Duration};
use tokio::{task::JoinHandle, time::sleep};

use std::sync::Arc;
use tokio::sync::mpsc::{channel, unbounded_channel, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;

use std::collections::HashMap;

// use rdkafka::message::{Message, OwnedMessage};
use rdkafka::message::Timestamp;

pub mod driver;
pub mod example_topologies;
pub mod in_memory;
pub mod kafka;
pub mod postgresql;

#[derive(Debug, Clone)]
pub struct Message {
    pub payload: Option<Vec<u8>>,
    pub key: Option<Vec<u8>>,
    pub topic: String,
    pub timestamp: Timestamp,
    pub partition: i32,
    pub offset: i64,
    // headers: Option<OwnedHeaders>,
}

impl Message {
    /// Creates a new message with the specified content.
    ///
    /// This function is mainly useful in tests of `rust-rdkafka` itself.
    pub fn new(
        payload: Option<Vec<u8>>,
        key: Option<Vec<u8>>,
        topic: String,
        timestamp: Timestamp,
        partition: i32,
        offset: i64,
        // headers: Option<OwnedHeaders>,
    ) -> Message {
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

pub struct Topology {
    inputs: HashMap<&'static str, Sender<Message>>,
    writes_sink: Sender<Message>,
    writes_source: Receiver<Message>,
}

pub struct Stream {
    rx: Receiver<Message>,
    output: Sender<Message>,
}

impl<'a> Topology {
    pub fn new() -> Topology {
        let inputs = HashMap::new();
        let (writes_sink, writes_source) = channel(1);
        Topology {
            inputs,
            writes_sink,
            writes_source,
        }
    }

    pub async fn process_message(self, topic_name: &str, msg: Message) {
        let topic = self.inputs.get(topic_name);
        match topic {
            None => panic!("topic not registered"),
            Some(sender) => {
                if let Err(e) = sender.send(msg).await {
                    panic!("failed to send: {}", e);
                }
            }
        }
    }

    pub fn read_from(&mut self, topic: &'static str) -> Stream {
        let (tx, rx) = channel(1);
        self.inputs.insert(topic, tx);
        Stream {
            rx,
            output: self.writes_sink.clone(),
        }
    }
}

impl Stream {
    async fn recv(&mut self) -> Option<Message> {
        self.rx.recv().await
    }

    fn poll_recv(&mut self) -> futures::task::Poll<Option<Message>> {
        use futures::task::{noop_waker, Context, Poll};
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        self.rx.poll_recv(&mut cx)
    }

    pub fn write_to(mut self, topic_name: &str) {
        tokio::spawn(async move {
            loop {
                /*
                match self.rx.blocking_recv() {
                    None => {
                        println!("closed 1");
                        return;
                    }
                    Some(msg) => self.output.send(msg).await.unwrap(),
                }
                */

                /*
                use futures::task::Poll;
                match self.poll_recv() {
                    Poll::Pending => {
                        //    println!("pending 1")
                    }
                    Poll::Ready(Some(message)) => {
                        println!("msg 1");
                        self.output
                            .send(message)
                            //.send_timeout(message, Duration::from_millis(200))
                            .await
                            .unwrap();
                    }
                    Poll::Ready(None) => {
                        println!("closed 1");
                        return;
                    }
                };
                */

                match self.recv().await {
                    Some(message) => {
                        println!("msg 1");
                        self.output
                            .send(message)
                            //.send_timeout(message, Duration::from_millis(200))
                            .await
                            .unwrap();
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
