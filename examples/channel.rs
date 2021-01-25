use std::time::Duration;
use tokio::time::sleep;

use std::sync::Arc;
use tokio::sync::mpsc::{channel, unbounded_channel};
use tokio::sync::Mutex;

#[tokio::main]
async fn main() {
    //let (sender, receiver) = unbounded_channel();
    let (tx1, mut rx1) = channel(1);

    tokio::spawn(async move {
        for _ in 0..1000000 {
            tx1.send("sending from second handle")
                .await
                .expect("sending failed");
        }
    });

    let (tx2, mut rx2) = channel(1);

    tokio::spawn(async move {
        while let Some(message) = rx1.recv().await {
            tx2.send(message).await.expect("sending failed");
        }
    });

    let (tx3, mut rx3) = channel(1);

    tokio::spawn(async move {
        while let Some(message) = rx2.recv().await {
            tx3.send(message).await.expect("sending failed");
        }
    });

    let mut i = 0;
    while let Some(message) = rx3.recv().await {
        // println!("GOT = {}", message);
        i = i + 1;
    }
    println!("C = {}", i);
}
