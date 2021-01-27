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

struct Stream<V> {
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

    fn read(mut self, topic_name: &'a str) -> Stream<Option<&[u8]>> {
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

    pub async fn stop(self) {
        match (self.task, self.tx) {
            (Some(task), Some(tx)) => {
                println!("shutting down");
                tx.send(()).expect("failed to signal shutdown");
                task.await.expect("failed to wait for shutdown");
                println!("shut down complete");
            }
            _ => panic!("streams not running"),
        }
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
