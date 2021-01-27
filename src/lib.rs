use std::time::Duration;
use tokio::time::sleep;

use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::{channel, unbounded_channel};
use tokio::sync::Mutex;

use std::collections::HashMap;

struct Streams<'a> {
    streams: HashMap<&'a str, Sender<Option<&'a [u8]>>>,
}

struct Stream<V> {
    rx: Receiver<V>,
}

impl<'a> Streams<'a> {
    fn new() -> Streams<'a> {
        let streams = HashMap::new();
        Streams { streams }
    }

    fn read(mut self, topic_name: &'a str) -> Stream<Option<&[u8]>> {
        let (tx, rx) = channel(1);
        self.streams.insert(topic_name, tx);
        Stream::<Option<&[u8]>> { rx }
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
}
