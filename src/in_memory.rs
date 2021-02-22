use super::Topology;
use tokio::sync::oneshot;
use tokio::{task::JoinHandle, time::sleep};

pub struct Driver {}

impl<'a> Driver {
    pub fn new(topo: Topology) -> Driver {
        Driver {}
    }

    pub fn publish(&mut self, topic: &str, msg: Option<&'a [u8]>) {}

    pub fn await_eot(&mut self) {}

    pub async fn start(&mut self) {
    }
    pub async fn stop(mut self) {
    }
}
