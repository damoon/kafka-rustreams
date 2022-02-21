use rdkafka::message::Timestamp;
use rustreams::driver::{kafka, Driver};
use rustreams::Message;

use tokio::signal;

use rustreams::mapper::Mapper;
use rustreams::Topology;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .parse_env("RUST_LOG")
        .format_timestamp_millis()
        .init();

    let mut topology = Topology::default();

    let input1 = topology.read_from::<String, String>("input1"); // TODO only allow one stream per topic
    let input2 = topology.read_from::<String, String>("input2");

    let length2 = input2
        .map(|s: &Vec<u8>| -> usize { s.len() })
        .map(usize_ser);

    input1.write_to("output1");
    length2.write_to("output2");

    let app = kafka::Driver::new(topology);

    let msg = Message {
        key: None,
        value: Some("hello world".as_bytes().to_vec()),
        timestamp: Timestamp::NotAvailable,
    };

    app.write("input1", msg).await;
    app.write("input2", msg).await;

    signal::ctrl_c().await?;

    app.stop().await;

    Ok(())
}

fn usize_ser(i: &usize) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}
