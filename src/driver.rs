
use async_trait::async_trait;

#[async_trait]
pub trait Driver<'a> {
    fn write_to(&mut self, topic: &str, msg: Option<&'a [u8]>);

    fn await_end_of_topic(&mut self);

    async fn start(&mut self);

    async fn stop(self);
}
