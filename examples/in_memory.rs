use rustreams::in_memory;
use rustreams::driver::Driver;
use rustreams::example_topologies;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let topology = example_topologies::copy("input_topic", "ouput_topic");

    let mut app = in_memory::Driver::new(topology);

    app.write_to("input_topic", Some("test1".as_bytes())).await;

    Ok(())
}
