use tokio::sync::mpsc::{Receiver, Sender};

use super::{Key, Message, StreamMessage, Topology, Value};

use std::{collections::HashMap, sync::Mutex};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct Driver {
    pub created_messages: Arc<Mutex<Vec<Message<Key, Value>>>>,

    inputs: HashMap<&'static str, Sender<StreamMessage<Key, Value>>>,
    flush_needed: Arc<AtomicUsize>,
    flushed_rx: Receiver<()>,

    pub writes_counter: Arc<AtomicUsize>,
}

impl Driver {
    pub fn start(topo: Topology) -> Driver {
        let writes_counter = Arc::new(AtomicUsize::new(0));
        let created_messages = Arc::new(Mutex::new(Vec::new()));

        let flushed_tx = topo.flushed_tx;
        let mut writes_rx = topo.writes_rx;
        let writes_counter_clone = writes_counter.clone();
        let created_messages_clone = created_messages.clone();

        tokio::spawn(async move {
            loop {
                match writes_rx.recv().await {
                    Some(StreamMessage::Message(message)) => {
                        writes_counter_clone.fetch_add(1, Ordering::Relaxed);
                        created_messages_clone.lock().unwrap().push(message.clone());
                    }
                    Some(StreamMessage::Flush) => {
                        flushed_tx.send(()).await.expect("failed to ack flush")
                    }
                    None => {
                        // channel closed
                        return;
                    }
                };
            }
        });

        Driver {
            created_messages,

            inputs: topo.inputs,
            flush_needed: topo.flush_needed,
            flushed_rx: topo.flushed_rx,

            writes_counter,
        }
    }

    pub async fn flush(&mut self) {
        println!("flushing");

        let expected_acks = self.inputs.len() * self.flush_needed.load(Ordering::Relaxed);
        //println!("await {} flushes", expected_acks);

        for flusher in self.inputs.iter() {
            //    println!("request flush");
            flusher
                .1
                .send(StreamMessage::Flush)
                .await
                .expect("failed to trigger flush");
            //    println!("requested flush");
        }

        for _ in 0..expected_acks {
            //    println!("await flush ack");
            self.flushed_rx
                .recv()
                .await
                .expect("failed to receive flush acknowledge");
        }
        // println!("all flushes acked");
    }

    pub async fn stop(&mut self) {
        self.flush().await;
    }

    pub async fn write_to(&mut self, msg: Message<Key, Value>) {
        let topic_name = msg.topic.clone();
        self.inputs
            .get(topic_name.as_str())
            .expect(format!("failed to look up input stream {}", topic_name).as_str())
            .send(StreamMessage::Message(msg))
            .await
            .expect("failed to write message")
    }
}
