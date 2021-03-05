# Kafka rustreams

Kafka streams in rust.

`docker-compose up`

`KAFKA_HOST=127.0.0.1:9092 RUST_LOG=warn,kafka_rustreams=info cargo run`

`cargo run --example generate_random`

`cargo run --example print`

`cargo run --example insert`

`cargo run --example copy`

`time taskset 0x1 cargo run --release --example channel3`

`RUSTFLAGS="-C target-cpu=native" cargo build --release --example in_memory && /usr/bin/time -v target/release/examples/in_memory`


## Learnings

Seeking to offset 0 is not allowed. Use Offset::Beginning instead.

The current offset is only available once the consumer was polled.

The first transaction commit of a producer is slower, even when the topic exists and has messages.

Keeping the process pinned to a CPU core improves single threaded performance a lot.

Run `cargo fmt`.

Time as an shell alias vs the binary command line tool. https://stackoverflow.com/questions/774556/peak-memory-usage-of-a-linux-unix-process
