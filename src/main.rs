mod models;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use models::QdrantPoint;
use qdrant_client::client::{Payload, QdrantClient};
use qdrant_client::qdrant::{PointId, PointStruct};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::Message;
use serde::Serialize;

pub type Result<T> = core::result::Result<T, ErrorType>;

#[derive(Debug, Clone, Serialize)]
pub enum ErrorType {
    GenericError { e: String },
}

#[tokio::main]
async fn main() {
    println!("Hello, world!");

    (0..2)
        .map(|_| {
            tokio::spawn(run_async_ingestor(
                "localhost:9092".to_string(),
                "test-1".to_string(),
                "tester".to_string(),
            ))
        })
        .collect::<FuturesUnordered<_>>()
        .for_each(|_| async { () })
        .await
}

async fn run_async_ingestor(brokers: String, group_id: String, input_topic: String) {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &group_id)
        .set("bootstrap.servers", &brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Consumer creation failed!");

    consumer
        .subscribe(&[&input_topic])
        .expect("Failed to subscribe to the topic");

    let mut stream = consumer.stream();

    // construct a queue and a timer;
    // push to queue till threshold
    // if queue reaches threshold or timer > time threshold: batch upsert -> reset queue, reset timer
    // else keep pushing

    while let Some(result) = stream.next().await {
        match result {
            Ok(message) => {
                println!(
                    "Message offset: {}, Message partition: {}, Message key: {:?}, Message body: {:?}",
                    message.offset(),
                    message.partition(),
                    message.key_view::<str>(),
                    message.payload_view::<str>()
                );

                if let Err(e) = consumer.store_offset_from_message(&message) {
                    eprintln!("Error storing offset: {:?}", e);
                }
            }

            Err(e) => eprintln!("Kafka error: {}", e),
        }
    }
}

async fn upload_to_qdrant(
    collection_name: String,
    client: QdrantClient,
    points: Vec<QdrantPoint>,
) -> Result<()> {
    let points = points
        .into_iter()
        .map(|point| {
            let id = match point.id {
                serde_json::Value::Number(n) => Ok(n.to_string()),
                serde_json::Value::String(s) => Ok(s),
                _ => Err(ErrorType::GenericError {
                    e: String::from("Unknown PointID type"),
                }),
            }?;
            Ok(PointStruct::new(id, point.vector, point.payload))
        })
        .collect::<Result<Vec<PointStruct>>>()?;

    let result = client
        .upsert_points_batch_blocking(collection_name, None, points, None, 100)
        .await
        .map_err(|e| ErrorType::GenericError { e: e.to_string() })?;

    println!("{:?}", result.result);
    Ok(())
}
