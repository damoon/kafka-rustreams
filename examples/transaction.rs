use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::time::Duration;

use rand::Rng;

use env_logger::{self};
use maplit::hashmap;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::ToBytes;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::statistics::Statistics;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;

const ROUNDS: usize = 10;
const MESSAGES: usize = 100000;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::Builder::new()
        .parse_env("RUST_LOG")
        .format_timestamp_millis()
        .init();
    test_transaction_commit().await
}

pub fn rand_test_topic() -> String {
    let id = rand::thread_rng()
        .gen_ascii_chars()
        .take(10)
        .collect::<String>();
    format!("__test_{}", id)
}

pub fn rand_test_group() -> String {
    let id = rand::thread_rng()
        .gen_ascii_chars()
        .take(10)
        .collect::<String>();
    format!("__test_{}", id)
}

pub fn rand_test_transactional_id() -> String {
    let id = rand::thread_rng()
        .gen_ascii_chars()
        .take(10)
        .collect::<String>();
    format!("__test_{}", id)
}

pub fn get_bootstrap_server() -> String {
    // env::var("KAFKA_HOST").unwrap_or_else(|_| "localhost:9092".to_owned())
    env::var("KAFKA_HOST").expect("Environment variable KAFKA_HOST is missing")
}

pub fn consumer_config(
    group_id: &str,
    config_overrides: Option<HashMap<&str, &str>>,
) -> ClientConfig {
    let mut config = ClientConfig::new();

    config.set("group.id", group_id);
    config.set("client.id", "rdkafka_integration_test_client");
    config.set("group.instance.id", "testing-node");
    config.set("bootstrap.servers", get_bootstrap_server().as_str());
    config.set("enable.partition.eof", "false");
    config.set("session.timeout.ms", "6000");
    config.set("enable.auto.commit", "false");
    config.set("statistics.interval.ms", "500");
    config.set("api.version.request", "true");
    config.set("debug", "all");
    config.set("auto.offset.reset", "earliest");

    if let Some(overrides) = config_overrides {
        for (key, value) in overrides {
            config.set(key, value);
        }
    }

    config
}

fn create_consumer(
    config_overrides: Option<HashMap<&str, &str>>,
) -> Result<BaseConsumer, KafkaError> {
    consumer_config(&rand_test_group(), config_overrides).create()
}

fn create_producer() -> Result<BaseProducer, KafkaError> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", &get_bootstrap_server())
        .set("message.timeout.ms", "5000")
        .set("enable.idempotence", "true")
        .set("transactional.id", &rand_test_transactional_id())
        .set("debug", "eos");
    config.set_log_level(RDKafkaLogLevel::Debug);
    config.create()
}

enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
}

impl IsolationLevel {
    fn to_string(&self) -> &str {
        match self {
            IsolationLevel::ReadUncommitted => "read_uncommitted",
            IsolationLevel::ReadCommitted => "read_committed",
        }
    }
}

fn count_records(consumer: &BaseConsumer, topic: &str) -> Result<usize, KafkaError> {
    log::info!("Seek to beginning.");
    consumer.poll(Timeout::Never);
    let offset = consumer
        .position()?
        .find_partition(topic, 0)
        .unwrap()
        .offset();
    if offset != rdkafka::Offset::Beginning {
        consumer.seek(topic, 0, rdkafka::Offset::Beginning, Timeout::Never)?;
    }

    let mut count = 0;
    for message in consumer.iter() {
        match message {
            Ok(_m) => {
                //let s = m.payload_view::<str>().expect("not a string");
                //println!("message: {:?}", s);

                // match m.payload() {
                //     Some(p) => println!("message: {:?}", std::str::from_utf8(p)),
                //     None => println!("no message found"),
                // };
                count += 1
            }
            Err(KafkaError::PartitionEOF(_)) => break,
            Err(e) => {
                println!("error: {:?}", e);
                return Err(e);
            }
        }
    }
    Ok(count)
}

pub async fn create_topic(name: &str, partitions: i32) {
    let client: AdminClient<_> = admin_config(None).create().unwrap();
    client
        .create_topics(
            &[NewTopic::new(name, partitions, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .unwrap();
}

pub fn admin_config(config_overrides: Option<HashMap<&str, &str>>) -> ClientConfig {
    let mut config = ClientConfig::new();

    config.set("client.id", "rdkafka_integration_test_client");
    config.set("bootstrap.servers", get_bootstrap_server().as_str());
    config.set("statistics.interval.ms", "500");
    config.set("api.version.request", "true");
    config.set("debug", "all");

    if let Some(overrides) = config_overrides {
        for (key, value) in overrides {
            config.set(key, value);
        }
    }

    config
}

pub struct ProducerTestContext {
    _some_data: i64, // Add some data so that valgrind can check proper allocation
}

impl ClientContext for ProducerTestContext {
    fn stats(&self, _: Statistics) {} // Don't print stats
}

/// Produce the specified count of messages to the topic and partition specified. A map
/// of (partition, offset) -> message id will be returned. It panics if any error is encountered
/// while populating the topic.
pub async fn populate_topic<P, K, J, Q>(
    topic_name: &str,
    count: i32,
    value_fn: &P,
    key_fn: &K,
    partition: Option<i32>,
    timestamp: Option<i64>,
) -> HashMap<(i32, i64), i32>
where
    P: Fn(i32) -> J,
    K: Fn(i32) -> Q,
    J: ToBytes,
    Q: ToBytes,
{
    let prod_context = ProducerTestContext { _some_data: 1234 };

    // Produce some messages
    log::info!("Produce producer");
    let producer = &ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_server().as_str())
        .set("statistics.interval.ms", "500")
        .set("api.version.request", "true")
        .set("debug", "all")
        .set("message.timeout.ms", "30000")
        .create_with_context::<ProducerTestContext, FutureProducer<_>>(prod_context)
        .expect("Producer creation error");

    log::info!("Produce some messages");
    let futures = (0..count)
        .map(|id| {
            let future = async move {
                producer
                    .send(
                        FutureRecord {
                            topic: topic_name,
                            payload: Some(&value_fn(id)),
                            key: Some(&key_fn(id)),
                            partition,
                            timestamp,
                            headers: None,
                        },
                        Duration::from_secs(1),
                    )
                    .await
            };
            (id, future)
        })
        .collect::<Vec<_>>();

    log::info!("Await message ACK");
    let mut message_map = HashMap::new();
    for (id, future) in futures {
        match future.await {
            Ok((partition, offset)) => message_map.insert((partition, offset), id),
            Err((kafka_error, _message)) => panic!("Delivery failed: {}", kafka_error),
        };
    }

    log::info!("Messages produced.");
    message_map
}

pub fn value_fn(id: i32) -> String {
    format!("Message {}", id)
}

pub fn key_fn(id: i32) -> String {
    format!("Key {}", id)
}

async fn test_transaction_commit() -> Result<(), Box<dyn Error>> {
    log::info!("init");
    let consume_topic = rand_test_topic();
    let produce_topic = rand_test_topic();
    //    let consume_topic = String::from("consume_topic");
    //    let produce_topic = String::from("produce_topic");

    log::info!("Create `consume_topic`.");
    create_topic(consume_topic.as_str(), 1).await;
    log::info!("Create `produce_topic`.");
    create_topic(produce_topic.as_str(), 1).await;

    log::info!("populate_topic");
    populate_topic(&consume_topic, 30, &value_fn, &key_fn, Some(0), None).await;

    // Create consumer and subscribe to `consume_topic`.
    log::info!("Create consumer and subscribe to `consume_topic`.");
    let consumer = create_consumer(None)?;
    consumer.subscribe(&[&consume_topic])?;
    consumer.poll(Timeout::Never).unwrap()?;

    log::info!("Create consumer for counting committed messages.");
    let consumer_counting_committed = create_consumer(Some(hashmap! {
        "isolation.level" => IsolationLevel::ReadCommitted.to_string(),
        "enable.partition.eof" => "true"
    }))?;
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(produce_topic.as_str(), 0);
    consumer_counting_committed.assign(&tpl)?;

    log::info!("Count comitted messages.");
    let commited_count = count_records(&consumer_counting_committed, &produce_topic)?;

    log::info!("Create consumer for counting uncommitted messages.");
    let consumer_counting_uncommitted = create_consumer(Some(hashmap! {
        "isolation.level" => IsolationLevel::ReadUncommitted.to_string(),
        "enable.partition.eof" => "true"
    }))?;
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(produce_topic.as_str(), 0);
    consumer_counting_uncommitted.assign(&tpl)?;

    log::info!("Count uncomitted messages.");
    let uncommited_count = count_records(&consumer_counting_uncommitted, &produce_topic)?;

    // Commit the first 10 messages.
    log::info!("Commit the first 10 messages.");
    let mut commit_tpl = TopicPartitionList::new();
    commit_tpl.add_partition_offset(&consume_topic, 0, Offset::Offset(10))?;
    consumer.commit(&commit_tpl, CommitMode::Sync).unwrap();

    // Create a producer and start a transaction.
    log::info!("Create a producer and clean up incomplete transactions.");
    let producer = create_producer()?;
    producer.init_transactions(Timeout::Never)?;

    for _ in 0..ROUNDS {
        log::info!("Start a transaction.");
        producer.begin_transaction()?;

        // Produce 10 records in the transaction.
        log::info!("Produce 10 records in the transaction.");
        for _ in 0..MESSAGES {
            producer
                .send(
                    BaseRecord::to(&produce_topic)
                        .payload("A")
                        .key("B")
                        .partition(0),
                )
                .unwrap();
        }

        // Tie the commit of offset 20 to the transaction.
        log::info!("Tie the commit of offset 20 to the transaction.");
        let cgm = consumer.group_metadata().unwrap();
        let mut txn_tpl = TopicPartitionList::new();
        txn_tpl.add_partition_offset(&consume_topic, 0, Offset::Offset(20))?;
        producer.send_offsets_to_transaction(&txn_tpl, &cgm, Timeout::Never)?;

        // Commit the transaction.
        log::info!("Commit the transaction.");
        producer.commit_transaction(Timeout::Never)?;
    }

    // Check that 10 records were produced.
    log::info!(
        "Check that {} uncommited records were produced.",
        ROUNDS * MESSAGES
    );
    assert_eq!(
        count_records(&consumer_counting_uncommitted, &produce_topic)? - uncommited_count,
        ROUNDS * MESSAGES,
    );
    log::info!(
        "Check that {} commited records were produced.",
        ROUNDS * MESSAGES
    );
    assert_eq!(
        count_records(&consumer_counting_committed, &produce_topic)? - commited_count,
        ROUNDS * MESSAGES,
    );

    // Check that the consumer's committed offset is now 20.
    log::info!("Check that the consumer's committed offset is now 20.");
    let committed = consumer.committed(Timeout::Never)?;
    assert_eq!(
        committed
            .find_partition(&consume_topic, 0)
            .unwrap()
            .offset(),
        Offset::Offset(20)
    );

    log::info!("Done");
    Ok(())
}
