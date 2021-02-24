use core::panic;
use std::{borrow::Borrow, option, time::Duration};
use tokio::{task::JoinHandle, time::sleep};

use std::sync::Arc;
use tokio::sync::mpsc::{channel, unbounded_channel, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;

use std::collections::HashMap;

use crate::Message;

use super::Topology;

pub struct Driver {
    inputs: HashMap<&'static str, Sender<Message>>,
    writes_sink: Sender<Message>,
}

impl Driver {
    pub fn new(mut topo: Topology) -> Driver {
        use futures::task::{noop_waker, Context, Poll};

        let mut src = topo.writes_source;

        tokio::spawn(async move {
            loop {
                /*
                let waker = noop_waker();
                let mut cx = Context::from_waker(&waker);
                match src.poll_recv(&mut cx) {
                    Poll::Pending => {
                        // println!("pending 2")
                    }
                    Poll::Ready(Some(message)) => {
                        println!("writing {:?}", message);
                    }
                    Poll::Ready(None) => {
                        println!("closed 2");
                        return;
                    }
                };
                */

                match src.recv().await {
                    Some(message) => println!("writing {:?}", message),
                    None => {
                        println!("closed 2");
                        return;
                    }
                };

                /*
                match self.topo.writes_source.blocking_recv() {
                    Some(message) => println!("writing {:?}", message),
                    None => {
                        println!("closed 2");
                        return;
                    }
                };
                */
            }
        });

        Driver {
            inputs: topo.inputs,
            writes_sink: topo.writes_sink,
        }
    }

    pub async fn write_to(&mut self, topic_name: &str, msg: Message) {
        self.inputs
            .get(topic_name)
            .unwrap()
            //.blocking_send(msg)
            .send(msg)
            .await
            .unwrap()
    }
}
