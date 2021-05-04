use rustreams::driver::{Driver, in_memory};
use rustreams::example_topologies;
use rustreams::new_message;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .parse_env("RUST_LOG")
        .format_timestamp_millis()
        .init();

    let topology = example_topologies::copy("input_topic", "output_topic");

    let driver = in_memory::Driver::start(topology);

    for n in 0..1_000_000 {
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
