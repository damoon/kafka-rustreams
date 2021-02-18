use super::Topology;
use tokio::sync::oneshot;
use tokio::{task::JoinHandle, time::sleep};

pub struct TestDriver {}

impl<'a> TestDriver {
    pub fn new() -> TestDriver {
        TestDriver {}
    }

    pub fn addMsg(topic: &str, msg: Option<&'a [u8]>) {}

    pub fn evalute(&mut self, topology: Topology<'a>) {}
}
