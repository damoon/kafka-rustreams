use async_trait::async_trait;

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::{Key, StreamMessage, Value};
use tokio::sync::mpsc::{Receiver, Sender};

pub mod in_memory;
pub mod kafka;
pub mod postgresql;
pub mod testing;

#[async_trait]
pub trait Driver {
    async fn write(&self, msg: super::Message<super::Key, super::Value>);

    async fn stop(self);
}

pub(crate) async fn flush(
    inputs: HashMap<&str, Sender<StreamMessage<Key, Value>>>,
    flush_needed: Arc<AtomicUsize>,
    flushed_rx: &mut Receiver<()>,
) {
    log::debug!("flushing");

    let expected_acks = inputs.len() * flush_needed.load(Ordering::Relaxed);

    log::debug!("await {} flushes", expected_acks);

    for input in inputs.iter() {
        log::debug!("request flush");

        input
            .1
            .send(StreamMessage::Flush)
            .await
            .expect("failed to trigger flush");

        log::debug!("requested flush");
    }

    for _ in 0..expected_acks {
        log::debug!("await flush ack");

        flushed_rx
            .recv()
            .await
            .expect("failed to receive flush acknowledge");
    }

    log::debug!("all flushes acked");
}
