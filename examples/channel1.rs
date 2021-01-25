use std::time::Duration;
use tokio::time::sleep;

use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::{channel, unbounded_channel};
use tokio::sync::Mutex;

#[tokio::main]
async fn main() {
    //let (sender, receiver) = unbounded_channel();
    let (tx1, mut rx1) = channel(1);

    let s1 = Stream { rx: rx1 };

    tokio::spawn(async move {
        for _ in 0..1000000 {
            tx1.send("sending from second handle")
                .await
                .expect("sending failed");
        }
    });

    let mut s2 = s1.map(Box::new(|v1: &str| -> usize { v1.len() }));

    let mut i = 0;
    while let Some(message) = s2.rx.recv().await {
        // println!("GOT = {}", message);
        i = i + message;
    }
    println!("C = {}", i);
}

struct Stream<V> {
    rx: Receiver<V>,
}

trait Mapper<V1, V2> {
    fn map(self, m: Box<dyn Fn(V1) -> V2 + Send>) -> Stream<V2>;
}

impl<V1: Send + 'static, V2: Send + 'static> Mapper<V1, V2> for Stream<V1> {
    fn map(mut self, m: Box<dyn Fn(V1) -> V2 + Send>) -> Stream<V2> {
        let (tx2, rx2) = channel(1);
        let s: Stream<V2> = Stream { rx: rx2 };

        tokio::spawn(async move {
            while let Some(message) = self.rx.recv().await {
                let m2 = m(message);
                if let Err(e) = tx2.send(m2).await {
                    panic!("failed to send");
                }
            }
        });

        s
    }
}
