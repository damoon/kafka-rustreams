use rustreams::example_topologies;
use rustreams::new_message;
use rustreams::testing::Driver;

use env_logger;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .parse_env("RUST_LOG")
        .format_timestamp_millis()
        .init();

    let topology = example_topologies::copy("input_topic", "output_topic");

    let mut driver = Driver::start(topology);

    for n in 0..1_000_000 {
        let msg = new_message(
            "input_topic".to_string(),
            None,
            Some(format!("hello world {}", n)),
        );
        driver.write_to(msg).await;
    }

    let messages = driver.stop().await;

    assert_eq!(
        1_000_000,
        messages.len(),
        "found {} created messages instead of 1.000.000 messages",
        messages.len()
    );

    Ok(())
}
