use rdkafka::message::{Timestamp};
use rustreams::driver::{kafka, Driver};
use rustreams::{Message, Stream};
use rustreams::Topology;

fn abc() {
    let sparkle_heart = vec![240, 159, 146, 150];
    let sparkle_heart = std::str::from_utf8(&sparkle_heart).unwrap().to_string();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .parse_env("RUST_LOG")
        .format_timestamp_millis()
        .init();
    
    let mut topology = Topology::default();

    let input1 = topology.read_from::<str, str>("input1"); // TODO only allow one stream per topic
    let input2 = topology.read_from::<str, str>("input2");

    let length2: Stream<String, usize> = input2
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

    let (tx, rx) = channel();
    ctrlc::set_handler(move || tx.send(()).expect("Could not send signal on channel."))
        .expect("Error setting Ctrl-C handler");
    println!("Waiting for Ctrl-C...");
    rx.recv().expect("Could not receive from channel.");

    app.stop().await;

    Ok(())
}

fn usize_ser(i: &usize) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}
