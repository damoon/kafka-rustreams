use crate::{Key, Message, Value};

use super::super::Topology;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use async_trait::async_trait;

use tokio_postgres::{Client, Error, NoTls};

pub struct Driver {
    tx: oneshot::Sender<()>,
    task: JoinHandle<()>,
    client: Client,
}

impl Driver {
    pub async fn new(config: &str, _topo: Topology) -> Result<Driver, Error> {

        let (client, connection) =
            tokio_postgres::connect(config, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        client.execute("CREATE TABLE offsets (
                application VARCHAR(255),
                topic       VARCHAR(255),
                offset      BIGINT,
                PRIMARY KEY(application, topic)
            )", &[]).await?;

        use tokio::sync::oneshot::error::TryRecvError;
        // TODO: create postgresql client

        // TODO: register topics

        let (tx, mut rx) = oneshot::channel::<()>();

        let task = tokio::spawn(async move {
            // until shutting down
            while let Err(TryRecvError::Empty) = rx.try_recv() {
                // TODO: begin_transaction

                // TODO: for 100ms or
                // TODO: until all streams reached eof
                // TODO: read and process messages

                // TODO: commit
            }
        });

        Ok(Driver { tx, task, client })
    }
}

#[async_trait]
impl super::Driver for Driver {
    async fn write(&self, _msg: Message<Key, Value>) {
        // TODO
    }

    async fn stop(mut self) {
        println!("shutting down");

        self.tx.send(()).expect("failed to signal shutdown");

        self.task.await.expect("failed to wait for shutdown");

        println!("shut down complete");
    }
}
