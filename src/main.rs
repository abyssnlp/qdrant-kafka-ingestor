mod models;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use models::QdrantPoint;
use qdrant_client::client::{Payload, QdrantClient};
use qdrant_client::qdrant::PointStruct;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::Message;
use serde::Serialize;
use std::sync::Arc;
use tokio::task::JoinHandle;

pub type Result<T> = core::result::Result<T, ErrorType>;

#[derive(Debug, Clone, Serialize)]
pub enum ErrorType {
    GenericError { e: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    let qdrant_client = Arc::new(
        QdrantClient::from_url("http://localhost:6333")
            .build()
            .map_err(|e| ErrorType::GenericError {
                e: format!("Failed to create Qdrant Client: {}", e.to_string()),
            })?,
    );

    let mut tasks = Vec::new();

    for _ in 0..2 {
        let handle = tokio::spawn(run_async_ingestor(
            "localhost:9092".to_string(),
            "test-1".to_string(),
            "tester".to_string(),
            qdrant_client.clone(),
            "tester",
        ));
        tasks.push(handle);
    }

    for task in tasks {
        let result = task
            .await
            .map_err(|e| ErrorType::GenericError { e: e.to_string() })?;

        match result {
            Ok(_) => println!("Successfully uploaded to qdrant!"),
            Err(e) => println!("{:?}", e),
        }
    }

    Ok(())
}

async fn run_async_ingestor(
    brokers: String,
    group_id: String,
    input_topic: String,
    client: Arc<QdrantClient>,
    collection_name: &str,
) -> Result<()> {
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

                let owned_message = message.detach();

                // process here
                upload_to_qdrant(
                    collection_name,
                    client.clone(),
                    vec![QdrantPoint {
                        id: owned_message.offset().into(),
                        vector: (1..5)
                            .map(|_| owned_message.offset() as f32)
                            .collect::<Vec<f32>>(),
                        payload: serde_json::from_str(
                            message
                                .payload_view::<str>()
                                .unwrap()
                                .map_err(|e| ErrorType::GenericError { e: e.to_string() })?,
                        )
                        .map_err(|e| ErrorType::GenericError { e: e.to_string() })?,
                    }],
                )
                .await?;
            }

            Err(e) => println!("Kafka error: {}", e),
        }
    }
    Ok(())
}

async fn upload_to_qdrant(
    collection_name: &str,
    client: Arc<QdrantClient>,
    points: Vec<QdrantPoint>,
) -> Result<()> {
    println!("{:?}", &points);
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
            Ok(PointStruct::new(
                id,
                point.vector,
                match point.payload {
                    Some(val) => serde_json::from_value::<Payload>(val)
                        .map_err(|e| ErrorType::GenericError { e: e.to_string() })?,
                    None => Payload::new(),
                }
                .into(),
            ))
        })
        .collect::<Result<Vec<PointStruct>>>()?;

    let result = client
        .upsert_points_batch_blocking(collection_name, None, points, None, 100)
        .await
        .map_err(|e| ErrorType::GenericError { e: e.to_string() })?;

    println!("{:?}", result.result);
    Ok(())
}
