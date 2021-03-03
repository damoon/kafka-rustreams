use tokio::sync::mpsc::channel;

use super::{Stream, StreamMessage, WithChange};

pub trait Mapper<K, V1, V2> {
    fn map(self, m: impl Send + 'static + Fn(&V1) -> V2) -> Stream<K, V2>;
}

impl<K: Send + 'static, V1: Send + 'static, V2: Send + 'static> Mapper<K, V1, V2>
    for Stream<K, V1>
{
    fn map(self, map: impl Send + 'static + Fn(&V1) -> V2) -> Stream<K, V2> {
        let (tx, rx) = channel::<StreamMessage<K, V2>>(1);
        let mut source = self.rx;

        tokio::spawn(async move {
            loop {
                match source.recv().await {
                    Some(StreamMessage::Message(message)) => {
                        let new_payload: Option<V2> = match &message.payload {
                            Some(value) => Some(map(value)),
                            None => None,
                        };

                        let new_message = message.with_value(new_payload);

                        if let Err(e) = tx.send(StreamMessage::Message(new_message)).await {
                            panic!("failed to forward message: {}", e);
                        }
                    }
                    Some(StreamMessage::Flush) => {
                        if let Err(e) = tx.send(StreamMessage::Flush).await {
                            panic!("failed to forward flush: {}", e);
                        }
                    }
                    None => {
                        println!("closed 2");
                        return;
                    }
                };
            }
        });

        Stream {
            rx,
            flushed: self.flushed,
            flush_needed: self.flush_needed,
            appends: self.appends,
        }
    }
}
