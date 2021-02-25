use tokio::sync::mpsc::{ Sender};

use std::collections::HashMap;

use crate::Message;

use crate::{Key, Value};

use super::Topology;

use async_trait::async_trait;

pub struct Driver {
    inputs: HashMap<&'static str, Sender<Message<Key, Value>>>,
}

impl Driver {
    pub fn new(topo: Topology) -> Driver {
        let mut src = topo.writes_source;

        tokio::spawn(async move {
            loop {
                match src.recv().await {
                    Some(message) => match message.payload {
                        Some(p) => println!("writing {:?}", String::from_utf8(p).unwrap()),
                        None => println!("none"),
                    }
                    None => {
                        println!("closed 2");
                        return;
                    }
                };
            }
        });

        Driver {
            inputs: topo.inputs,
        }
    }
}

#[async_trait]
impl<'a> super::driver::Driver for Driver {
    async fn stop(self) {

    }

    async fn write_to(&mut self, topic_name: &str, msg: Message<Key, Value>) {
        self.inputs
            .get(topic_name)
            .unwrap()
            .send(msg)
            .await
            .unwrap()
    }
}
