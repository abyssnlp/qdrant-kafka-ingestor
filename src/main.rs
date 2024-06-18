mod models;
use futures::StreamExt;
use log::{debug, error, info};
use models::QdrantPoint;
use qdrant_client::client::{Payload, QdrantClient};
use qdrant_client::qdrant::{PointId, PointStruct};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::message::BorrowedMessage;
use rdkafka::Message;
use serde::Serialize;
use std::sync::Arc;

pub type Result<T> = core::result::Result<T, ErrorType>;
const FLUSH_TIME: u64 = 4;
const FLUSH_THRESHOLD: usize = 100;

#[derive(Debug, Clone, Serialize)]
pub enum ErrorType {
    GenericError { e: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let qdrant_client = Arc::new(
        QdrantClient::from_url("http://localhost:6334")
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
        let result = task.await.map_err(|e| ErrorType::GenericError {
            e: format!("Failed to run async ingestor: {}", e),
        })?;

        match result {
            Ok(_) => info!("Successfully uploaded to qdrant!"),
            Err(e) => error!("{:?}", e),
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

    // TODO: Keep a single queue of borrowed messages; Convert message to QdrantPoint when ingesting
    let mut queue = Vec::new();
    let mut message_queue: Vec<BorrowedMessage<'_>> = Vec::new();
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(FLUSH_TIME));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                if !queue.is_empty() {
                    info!("Flushing time-based queue of size: {}", queue.len());
                    let upload_result = upload_to_qdrant(collection_name, client.clone(), queue.clone()).await?;
                    if upload_result == 1 || upload_result == 2 {
                        commit_offset(&consumer, &message_queue).await?;
                        message_queue.clear();
                    }
                    queue.clear();
                }
            }

            Some(result) = stream.next() => {
                match result {
                    Ok(message) => {
                        debug!(
                            "Message offset: {}, Message partition: {}, Message key: {:?}, Message body: {:?}",
                            message.offset(),
                            message.partition(),
                            message.key_view::<str>(),
                            message.payload_view::<str>()
                        );

                        let owned_message = message.detach();

                        let point = QdrantPoint {
                            id: owned_message.offset().into(),
                            vector: (1..6)
                                .map(|_| owned_message.offset() as f32)
                                .collect::<Vec<f32>>(),
                            payload: serde_json::from_str(
                                message
                                    .payload_view::<str>()
                                    .unwrap()
                                    .map_err(|e| ErrorType::GenericError {
                                        e: format!("Failed to convert payload to qdrant payload: {}", e),
                                    })?,
                            )
                            .map_err(|e| ErrorType::GenericError {
                                e: format!("Failed to convert payload to qdrant payload (JSON): {}", e),
                            })?,
                        };

                        queue.push(point);
                        message_queue.push(message);

                        if queue.len() > FLUSH_THRESHOLD {
                            info!("Flushing size-based queue of size: {}", queue.len());
                            let upload_result = upload_to_qdrant(collection_name, client.clone(), queue.clone()).await?;
                            if upload_result == 1 || upload_result == 2 {
                                commit_offset(&consumer, &message_queue).await?;
                                message_queue.clear();
                            }
                            queue.clear();
                        }
                    }
                    Err(e) => error!("Kafka error: {}", e),
                }
            }
        }
    }

    // Ok(())
}

async fn upload_to_qdrant(
    collection_name: &str,
    client: Arc<QdrantClient>,
    points: Vec<QdrantPoint>,
) -> Result<i32> {
    debug!("{:?}", &points);
    let points = points
        .into_iter()
        .map(|point| {
            let id: PointId = match point.id {
                serde_json::Value::Number(n) => {
                    if let Some(u) = n.as_u64() {
                        PointId::from(u)
                    } else {
                        return Err(ErrorType::GenericError {
                            e: String::from("Point ID number is not a valid u64"),
                        });
                    }
                }
                serde_json::Value::String(s) => PointId::from(s),
                _ => {
                    return Err(ErrorType::GenericError {
                        e: String::from("Unknown PointID type"),
                    });
                }
            };
            Ok(PointStruct::new(
                id,
                point.vector,
                match point.payload {
                    Some(val) => serde_json::from_value::<Payload>(val).map_err(|e| {
                        ErrorType::GenericError {
                            e: format!("Failed to convert payload to qdrant payload: {}", e),
                        }
                    })?,
                    None => Payload::new(),
                }
                .into(),
            ))
        })
        .collect::<Result<Vec<PointStruct>>>()?;

    let result = client
        .upsert_points_batch_blocking(collection_name, None, points, None, 100)
        .await
        .map_err(|e| ErrorType::GenericError {
            e: format!("Failed to upload to qdrant: {}", e),
        })?;

    debug!("{:?}", result.result);
    match result.result {
        Some(res) => Ok(res.status),
        None => Err(ErrorType::GenericError {
            e: String::from("Failed to get status from qdrant.UpdateResult"),
        }),
    }
}

async fn commit_offset<'a>(
    consumer: &'a StreamConsumer,
    messages: &'a Vec<BorrowedMessage<'a>>,
) -> Result<bool> {
    info!("Committing offsets: {:?}", messages.len());
    if messages.is_empty() {
        return Ok(true);
    }

    for message in messages {
        let result = consumer.commit_message(message, CommitMode::Async);
        match result {
            Ok(_) => (),
            Err(e) => {
                return Err(ErrorType::GenericError {
                    e: format!("Failed to commit offset: {}", e),
                })
            }
        }
    }

    Ok(true)
}
