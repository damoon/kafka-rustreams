use rdkafka::message::Timestamp;
use rustreams::example_topologies;
use rustreams::in_memory;
use rustreams::Message;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let topology = example_topologies::copy("input_topic", "output_topic");

    let driver = in_memory::Driver::new();
    let mut app = driver.start(topology);

    for n in 0..1_000_000 {
        let msg = Message {
            payload: Some(format!("hello world {}", n).as_bytes().to_vec()),
            key: None,
            topic: "topic".to_string(),
            timestamp: Timestamp::NotAvailable,
            partition: 0,
            offset: 0,
        };

        app.write_to("input_topic", msg).await;
    }

    println!("writes {:?}", app.writes_counter);

    app.flush().await;

    for n in 10..20 {
        let msg = Message {
            payload: Some(format!("Hello, world {}.", n).as_bytes().to_vec()),
            key: None,
            topic: "topic".to_string(),
            timestamp: Timestamp::NotAvailable,
            partition: 0,
            offset: 0,
        };

        app.write_to("input_topic", msg).await;
    }
 
    println!("writes {:?}", app.writes_counter);

    app.stop().await;

    println!("writes {:?}", app.writes_counter);

    Ok(())
}
