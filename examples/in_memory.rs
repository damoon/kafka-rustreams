use rdkafka::message::Timestamp;
use rustreams::example_topologies;
use rustreams::in_memory;
use rustreams::Message;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let topology = example_topologies::copy("input_topic", "output_topic");

    let mut driver = in_memory::Driver::start(topology);

    for n in 0..1_000_000 {
        let msg = Message {
            payload: Some(format!("hello world {}", n).as_bytes().to_vec()),
            key: None,
            topic: "input_topic".to_string(),
            timestamp: Timestamp::NotAvailable,
            partition: 0,
            offset: 0,
        };

        driver.write_to(msg).await;
    }

    driver.flush().await;

    assert_eq!(
        1_000_000,
        driver.created_messages.lock().unwrap().len(),
        "found {} created messages instead of 1.000.000 messages",
        driver.created_messages.lock().unwrap().len()
    );

    for n in 10..20 {
        let msg = Message {
            payload: Some(format!("Hello, world {}.", n).as_bytes().to_vec()),
            key: None,
            topic: "input_topic".to_string(),
            timestamp: Timestamp::NotAvailable,
            partition: 0,
            offset: 0,
        };

        driver.write_to(msg).await;
    }

    driver.flush().await;

    assert_eq!(
        1_000_010,
        driver.created_messages.lock().unwrap().len(),
        "found {} created messages instead of 1.000.010 messages",
        driver.created_messages.lock().unwrap().len()
    );

    Ok(())
}
