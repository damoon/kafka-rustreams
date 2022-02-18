use rustreams::driver::{Driver, testing};
use rustreams::{example_topologies, Message};
use rustreams::new_message;

use env_logger;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .parse_env("RUST_LOG")
        .format_timestamp_millis()
        .init();

    let topology = example_topologies::copy("input_topic", "output_topic");

    let mut driver = testing::Driver::start(topology);

    for n in 0..1_000_000 {
        let msg = Message{
            key: None,
            value: Some(format!("hello world {}", n)),
            timestamp: rustreams::Timestamp::NotAvailable,
        };
        driver.write("input_topic", msg).await;
    }

    let messages = driver.created_messages().await;

    assert_eq!(
        1_000_000,
        messages.len(),
        "found {} created messages instead of 1.000.000 messages",
        messages.len()
    );

    driver.stop().await;

    Ok(())
}
