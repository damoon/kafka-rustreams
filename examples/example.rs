use std::borrow::Borrow;

use tokio::signal;

use rustreams::testing::TestDriver;
use rustreams::Mapper;
use rustreams::Topology;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut topology = Topology::new();

    //    let input1 = streams.read("input1");
    //    let input2 = streams.read("input2");
    //    let length1 = input1.map(|s: Option<&[u8]>| -> usize { s.unwrap().len() });
    //    input1.map(|s: Option<&[u8]>| -> usize { s.unwrap().len() });

    topology.readFrom("input1").writeTo("output1");

    let mut test_env = TestDriver::new();
    topology.applyTo(test_env);

    //    streams.start().await;
    //    signal::ctrl_c().await?;
    //    streams.stop().await;
    Ok(())
}
