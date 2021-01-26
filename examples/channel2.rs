use std::time::Duration;
use tokio::time::sleep;

use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::{channel, unbounded_channel};
use tokio::sync::Mutex;

#[tokio::main]
async fn main() {
    let (tx, rx) = channel(1);

    let stream = Stream { rx };

    tokio::spawn(async move {
        for _ in 0..1_000 {
            tx.send("sending from second handle")
                .await
                .expect("sending failed");
        }
    });

    let mut stream: Stream<usize> = stream.map(|v1: &str| -> usize { v1.len() }).map(times_two);

    let mut i = 0;
    while let Some(message) = stream.recv().await {
        // println!("GOT = {}", message);
        i = i + message;
    }
    println!("C = {}", i);
}

fn times_two(i: usize) -> usize {
    i * 2
}

struct Stream<V> {
    rx: Receiver<V>,
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

trait Mapper<V1, V2> {
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
