use super::Topology;
use tokio::sync::oneshot;
use tokio::{task::JoinHandle, time::sleep};

use async_trait::async_trait;

pub struct Driver {}

impl Driver {
    pub fn new(topo: Topology) -> Driver {
        Driver {}
    }
}

#[async_trait]
impl<'a> super::Driver<'a> for Driver {
    fn publish(&mut self, topic: &str, msg: Option<&'a [u8]>) {}

    fn await_eot(&mut self) {}

    async fn start(&mut self) {}

    async fn stop(mut self) {}
}
