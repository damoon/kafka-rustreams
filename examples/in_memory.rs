use rdkafka::message::Timestamp;
use rustreams::driver::Driver;
use rustreams::example_topologies;
use rustreams::in_memory;
use rustreams::Message;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let topology = example_topologies::copy("input_topic", "ouput_topic");

    let mut app = in_memory::Driver::new(topology);

    for n in 1..10 {
        let msg = Message {
            payload: Some(format!("The number is {}", n).as_bytes().to_vec()),
            key: None,
            topic: "topic".to_string(),
            timestamp: Timestamp::NotAvailable,
            partition: 0,
            offset: 0,
        };

        app.write_to("input_topic", msg.clone()).await;
    }

    sleep(Duration::from_millis(200)).await;

    Ok(())
}
