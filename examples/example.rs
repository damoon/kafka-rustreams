use std::borrow::Borrow;

use tokio::signal;

use rustreams::testing::TestDriver;
use rustreams::Mapper;
use rustreams::Topology;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut topology = Topology::new();

    let input1 = topology.read_from("input1");
    let input2 = topology.read_from("input2");

    let length2 = input2.map(|s: Option<&[u8]>| -> usize { s.unwrap().len() });
    //    input1.map(|s: Option<&[u8]>| -> usize { s.unwrap().len() });

    input1.write_to("output1");
    length2.write_to("output2");

    let mut test_env = TestDriver::new();
    test_env.evalute(topology);

    //    streams.start().await;
    //    signal::ctrl_c().await?;
    //    streams.stop().await;
    Ok(())
}
