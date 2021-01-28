
use super::Topology;
use tokio::sync::oneshot;
use tokio::{task::JoinHandle, time::sleep};

pub struct TestDriver {}

impl<'a> TestDriver {
    pub fn new() -> TestDriver {
        TestDriver {}
    }

    pub fn evalute(&mut self, topology: Topology<'a>) {}
}
