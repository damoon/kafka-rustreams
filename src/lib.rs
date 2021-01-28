#![feature(in_band_lifetimes)]

use std::{borrow::Borrow, time::Duration};
use tokio::{task::JoinHandle, time::sleep};

use std::sync::Arc;
use tokio::sync::mpsc::{channel, unbounded_channel, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;

use std::collections::HashMap;

pub struct Streams<'a> {
    tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
    streams: HashMap<&'a str, Sender<Option<&'a [u8]>>>,
}

pub struct Stream<V> {
    rx: Receiver<V>,
}

impl<'a> Streams<'a> {
    pub fn new() -> Streams<'a> {
        let streams = HashMap::new();
        Streams {
            tx: None,
            task: None,
            streams,
        }
    }

    pub fn read(&mut self, topic_name: &'a str) -> Stream<Option<&'a [u8]>> {
        let (tx, rx) = channel(1);
        self.streams.insert(topic_name, tx);
        Stream::<Option<&[u8]>> { rx }
    }

    pub async fn start(&mut self) {
        use tokio::sync::oneshot::error::TryRecvError;
        // TODO
        // create kafka consumer
        // register topics

        let (tx, mut rx) = oneshot::channel::<()>();
        self.tx = Some(tx);

        self.task = Some(tokio::spawn(async move {
            // until shutting down
            while let Err(TryRecvError::Empty) = rx.try_recv() {
                // begin_transaction
                // read and process messages for 100ms
                // commit
            }
        }));
    }

    pub async fn stop(mut self) {
        if self.task.is_none() || self.tx.is_none() {
            panic!("streams not running")
        }

        println!("shutting down");
        self.tx
            .unwrap()
            .send(())
            .expect("failed to signal shutdown");
        self.task
            .unwrap()
            .await
            .expect("failed to wait for shutdown");
        println!("shut down complete");
    }
}

impl<V> Stream<V> {
    async fn recv(&mut self) -> Option<V> {
        self.rx.recv().await
    }

    fn poll_recv(&mut self) -> futures::task::Poll<Option<V>> {
        use futures::task::{noop_waker, Context, Poll};
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        self.rx.poll_recv(&mut cx)
    }

    fn write(mut self, topic_name: &str) {
        // TODO
    }
}

pub trait Mapper<V1, V2> {
    fn map(self, m: impl Send + 'static + Fn(V1) -> V2) -> Stream<V2>;
}

impl<V1: Send + 'static, V2: Send + 'static> Mapper<V1, V2> for Stream<V1> {
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

            while let Some(message) = self.rx.recv().await {
                let new_message = map(message);
                if let Err(e) = tx.send(new_message).await {
                    panic!("failed to send: {}", e);
                }
            }

            match self.poll_recv() {
                Poll::Pending => {
                    println!("pending")
                }
                Poll::Ready(Some(message)) => {
                    println!("msg")
                }
                Poll::Ready(None) => {
                    println!("closed")
                }
            };
        });

        Stream { rx }
    }
}
