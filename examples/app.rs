use std::borrow::{Borrow, BorrowMut};

use rdkafka::message::Timestamp;
use rustreams::driver::Driver;
use rustreams::Message;

use tokio::signal;

use rustreams::kafka;
use rustreams::mapper::Mapper;
use rustreams::Topology;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut topology = Topology::new();

    let input1 = topology.read_from("input1"); // TODO only allow one stream per topic
    let input2 = topology.read_from("input2");

    let length2 = input2
        .map(|s: &Vec<u8>| -> usize { s.len() })
        //.map(len)
        .map(usize_ser);
        //.map(|i| { i.to_be_bytes().to_vec() });

    let a3 = input2.map(len); // TODO not allowed

    input1.write_to("output1");
    length2.write_to("output2");

    let mut app = kafka::Driver::new(topology);

    let msg = Message {
        payload: Some("hello world".as_bytes().to_vec()),
        key: None,
        topic: "topic".to_string(),
        timestamp: Timestamp::NotAvailable,
        partition: 0,
        offset: 0,
    };

    app.write_to("input1", msg).await;

    // signal::ctrl_c().await?;

    app.stop().await;

    Ok(())
}

fn len (s: &Vec<u8>) -> usize {
    s.len()
}

fn usize_ser (i: &usize) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}
