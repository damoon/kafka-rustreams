use std::borrow::Borrow;

use tokio::signal;

use rustreams::in_memory;
use rustreams::Driver;
use rustreams::Mapper;
use rustreams::Topology;

use async_trait::async_trait;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut topology = Topology::new();

    let input1 = topology.read_from("input1");
    let input2 = topology.read_from("input2");

    let length2 = input2.map(|s: Option<&[u8]>| -> usize { s.unwrap().len() });
    //    input1.map(|s: Option<&[u8]>| -> usize { s.unwrap().len() });

    input1.write_to("output1");
    length2.write_to("output2");

    let mut app = in_memory::Driver::new(topology);

    app.start().await;

    app.publish("input1", Some("test1".as_bytes()));

    // signal::ctrl_c().await?;

    app.await_eot();

    app.stop().await;

    Ok(())
}
