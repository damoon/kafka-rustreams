use rustreams::driver::{Driver, kafka};
use rustreams::{example_topologies, Message};
use rustreams::new_message;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .parse_env("RUST_LOG")
        .format_timestamp_millis()
        .init();

    let topology = example_topologies::copy("input_topic", "output_topic");

    let driver = kafka::Driver::new(topology);

    for n in 0..10 {
        let msg = Message{
            key: None,
            value: Some(format!("hello world {}", n)),
            timestamp: rustreams::Timestamp::NotAvailable,
        };

        driver.write("input_topic", msg).await;
    }

    driver.stop().await;

    Ok(())
}
