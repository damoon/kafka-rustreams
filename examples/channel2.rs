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
trait NewTrait<V1, V2>: Sized + Send + FnMut(V1) -> V2 {}

trait Mapper<V1, V2>: Sized {
    fn map1(&mut self, m: dyn FnMut(V1) -> V2 + Send) -> Stream<V2>;
}

impl<V1: Send + 'static, V2: Send + 'static> Mapper<V1, V2> for Stream<V1> {
    fn map1(&mut self, m: dyn FnMut(V1) -> V2 + Send) -> Stream<V2>
    where
        Self: Sized,
    {
        let (tx2, rx2) = channel(1);
        let s: Stream<V2> = Stream { rx: rx2 };
        let m2 = Box::new(m);

        tokio::spawn(async move {
            while let Some(message) = self.rx.recv().await {
                let m3 = m2(message);
                if let Err(e) = tx2.send(m3).await {
                    panic!("failed to send");
                }
            }
        });

        s
    }
}
