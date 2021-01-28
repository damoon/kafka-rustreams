use tokio::signal;

use rustreams::Mapper;
use rustreams::Streams;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut streams = Streams::new();

    let input1 = streams.read("input1");
    let input2 = streams.read("input2");
    let length1 = input1.map(|s: Option<&[u8]>| -> usize { s.unwrap().len() });
    let length1 = input1.map(|s: usize| -> &[u8] { i.to_be_bytes() });

    streams.start().await;
    signal::ctrl_c().await?;
    streams.stop().await;
    Ok(())
}
