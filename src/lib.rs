use std::{borrow::Borrow, option, time::Duration};
use tokio::{task::JoinHandle, time::sleep};

use std::sync::Arc;
use tokio::sync::mpsc::{channel, unbounded_channel, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;

use std::collections::HashMap;

pub mod in_memory;
pub mod kafka;

use async_trait::async_trait;

#[async_trait]
pub trait Driver<'a> {
    fn publish(&mut self, topic: &str, msg: Option<&'a [u8]>);

    fn await_eot(&mut self);

    async fn start(&mut self);

    async fn stop(self);
}

pub struct Topology<'a> {
    streams: HashMap<&'a str, Sender<Option<&'a [u8]>>>,
}

pub struct Stream<V> {
    rx: Receiver<V>,
}

impl<'a> Topology<'a> {
    pub fn new() -> Topology<'a> {
        let streams = HashMap::new();
        Topology { streams }
    }

    pub fn read_from(&mut self, topic_name: &'a str) -> Stream<Option<&'a [u8]>> {
        let (tx, rx) = channel(1);
        self.streams.insert(topic_name, tx);
        Stream::<Option<&[u8]>> { rx }
    }
}

impl<'a, V> Stream<V> {
    async fn recv(&mut self) -> Option<V> {
        self.rx.recv().await
    }

    fn poll_recv(&mut self) -> futures::task::Poll<Option<V>> {
        use futures::task::{noop_waker, Context, Poll};
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        self.rx.poll_recv(&mut cx)
    }

    pub fn write_to(self, topic_name: &str) {
        // TODO
    }
}

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
