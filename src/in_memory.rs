use core::panic;

use super::Topology;

pub struct Driver<'a> {
    topo: Topology<'a>,
}

impl<'a> Driver<'a> {
    pub fn new(topo: Topology) -> Driver {
        Driver {
            topo
        }
    }

    pub async fn write_to(&mut self, topic_name: &str, msg: Option<&'a [u8]>) {
        match self.topo.inputs.get(topic_name) {
            None => {},
            Some(sender) => 
                if let Err(e) = sender.send(msg).await {
                    panic!("failed to send: {}", e);
                }
        }
    }
}
