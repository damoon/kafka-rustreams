use tokio::signal;

use rustreams::Streams;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut streams = Streams::new();
    streams.start().await;
    signal::ctrl_c().await?;
    streams.stop().await;
    Ok(())
}
