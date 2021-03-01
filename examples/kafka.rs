use rdkafka::message::Timestamp;
use rustreams::driver::Driver;
use rustreams::example_topologies;
use rustreams::kafka;
use rustreams::Message;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let topology = example_topologies::copy("input_topic", "output_topic");

    let mut app = kafka::Driver::new(topology);

    for n in 0..10 {
        let msg = Message {
            payload: Some(format!("hello world {}", n).as_bytes().to_vec()),
            key: None,
            topic: "topic".to_string(),
            timestamp: Timestamp::NotAvailable,
            partition: 0,
            offset: 0,
        };

        app.write_to("input_topic", msg).await;
    }

    app.stop().await;

    Ok(())
}
