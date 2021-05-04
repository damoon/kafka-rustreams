use rustreams::driver::{postgresql, Driver};
use rustreams::example_topologies;
use rustreams::new_message;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .parse_env("RUST_LOG")
        .format_timestamp_millis()
        .init();

    let topology = example_topologies::copy("input_topic", "output_topic");

    let driver = postgresql::Driver::new(
        "host=localhost user=postgres password=password123 dbname=postgres connect_timeout=5 application_name=rustreams",
        topology,
    )
    .await?;

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
