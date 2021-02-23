use rustreams::kafka;
use rustreams::driver::Driver;
use rustreams::example_topologies;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let topology = example_topologies::copy("input_topic", "ouput_topic");

    let mut app = kafka::Driver::new(topology);

    app.start().await;

    app.write_to("input_topic", Some("test1".as_bytes()));

    app.await_end_of_topic();

    app.stop().await;

    Ok(())
}
