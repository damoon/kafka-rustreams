use rustreams::driver::Driver;
use rustreams::example_topologies;
use rustreams::kafka;
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
        let msg = new_message(
            "input_topic".to_string(),
            None,
            Some(format!("hello world {}", n)),
        );

        driver.write(msg).await;
    }

    driver.stop().await;

    Ok(())
}
