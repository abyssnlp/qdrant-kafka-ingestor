mod models;
use clap::{Arg, Command};
use futures::StreamExt;
use log::{debug, error, info};
use models::{Config, QdrantPoint};
use qdrant_client::client::{Payload, QdrantClient};
use qdrant_client::qdrant::{PointId, PointStruct};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::message::BorrowedMessage;
use rdkafka::Message;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub type Result<T> = core::result::Result<T, ErrorType>;

#[derive(Debug, Clone, Serialize)]
pub enum ErrorType {
    GenericError { e: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    /*
       Args required:
           - brokers: String
           - group_id: String
           - input_topic: String
           - collection_name: String
           - qdrant_url: String
           - flush_time: u64
           - flush_threshold: usize
    */

    env_logger::init();

    let matches = Command::new("Qdrant Kafka Ingestor")
        .version("0.1.0")
        .about("Qdrant Kafka Ingestor")
        .arg(
            Arg::new("brokers")
                .short('b')
                .long("brokers")
                .help("Kafka brokers")
                .required(true),
        )
        .arg(
            Arg::new("group_id")
                .short('g')
                .long("group_id")
                .help("Kafka group id")
                .required(true),
        )
        .arg(
            Arg::new("input_topic")
                .short('t')
                .long("input_topic")
                .help("Kafka input topic")
                .required(true),
        )
        .arg(
            Arg::new("collection_name")
                .short('c')
                .long("collection_name")
                .help("Qdrant collection name")
                .required(true),
        )
        .arg(
            Arg::new("qdrant_url")
                .short('q')
                .long("qdrant_url")
                .help("Qdrant URL")
                .required(true),
        )
        .arg(
            Arg::new("flush_time")
                .short('f')
                .long("flush_time")
                .help("Flush time in seconds")
                .required(false)
                .default_value("4"),
        )
        .arg(
            Arg::new("flush_threshold")
                .short('s')
                .long("flush_threshold")
                .help("Flush threshold")
                .required(false)
                .default_value("100"),
        )
        .arg(
            Arg::new("num_consumers")
                .short('n')
                .long("num_consumers")
                .help("Number of consumers")
                .required(false)
                .default_value("2"),
        )
        .get_matches();

    let brokers = matches.get_one::<String>("brokers").unwrap();
    let group_id = matches.get_one::<String>("group_id").unwrap();
    let input_topic = matches.get_one::<String>("input_topic").unwrap();
    let collection_name = matches
        .get_one::<String>("collection_name")
        .unwrap()
        .to_string();
    let qdrant_url = matches.get_one::<String>("qdrant_url").unwrap().to_string();
    let flush_time: u64 = matches
        .get_one::<String>("flush_time")
        .unwrap()
        .parse()
        .unwrap();
    let flush_threshold: usize = matches
        .get_one::<String>("flush_threshold")
        .unwrap()
        .parse()
        .unwrap();
    let num_consumers: usize = matches
        .get_one::<String>("num_consumers")
        .unwrap()
        .parse()
        .unwrap();

    let config = Config {
        brokers: brokers.to_string(),
        group_id: group_id.to_string(),
        input_topic: input_topic.to_string(),
        collection_name: collection_name.to_string(),
        qdrant_url: qdrant_url.to_string(),
        flush_time,
        flush_threshold,
    };

    let running = Arc::new(AtomicBool::new(true));

    let qdrant_client = Arc::new(QdrantClient::from_url(&qdrant_url).build().map_err(|e| {
        ErrorType::GenericError {
            e: format!("Failed to create Qdrant Client: {}", e),
        }
    })?);

    let mut tasks = Vec::new();

    for _ in 0..num_consumers {
        let handle = tokio::spawn(run_async_ingestor(
            qdrant_client.clone(),
            running.clone(),
            config.clone(),
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
    client: Arc<QdrantClient>,
    running: Arc<AtomicBool>,
    config: Config,
) -> Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &config.group_id)
        .set("bootstrap.servers", &config.brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Consumer creation failed!");

    consumer
        .subscribe(&[&config.input_topic])
        .expect("Failed to subscribe to the topic");

    let mut stream = consumer.stream();

    // construct a queue and a timer;
    // push to queue till threshold
    // if queue reaches threshold or timer > time threshold: batch upsert -> reset queue, reset timer
    // else keep pushing

    // TODO: Keep a single queue of borrowed messages; Convert message to QdrantPoint when ingesting
    let mut queue = Vec::new();
    let mut message_queue: Vec<BorrowedMessage<'_>> = Vec::new();
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(config.flush_time));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut retry_delay = 2;

    loop {
        if running.load(Ordering::SeqCst) {
            tokio::select! {
                    _ = interval.tick() => {
                    if !queue.is_empty() {
                        info!("Flushing time-based queue of size: {}", queue.len());
                        let upload_result = upload_to_qdrant(&config.collection_name, client.clone(), queue.clone()).await?;
                        if upload_result == 1 || upload_result == 2 {
                            let is_committed = commit_offset(&consumer, &message_queue).await?;
                            if !is_committed {
                                error!("Failed to commit offsets to Kafka");
                                running.store(false, Ordering::SeqCst);
                            } else {
                                message_queue.clear();
                                queue.clear();
                            }
                        } else {
                            error!("Failed to upload to qdrant");
                            running.store(false, Ordering::SeqCst);
                        }
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

                            let point: QdrantPoint = serde_json::from_str(
                                message.payload_view::<str>()
                                    .unwrap()
                                    .map_err(|e| ErrorType::GenericError {
                                        e: format!("Failed to extract QdrantPoint from Kafka message: {}", e),
                                    })?,
                            ).map_err(|e| ErrorType::GenericError {
                                e: format!("Could not parse QdrantPoint with id, vector and payload (JSON): {}", e),
                            })?;

                            queue.push(point);
                            message_queue.push(message);

                            if queue.len() > config.flush_threshold {
                                info!("Flushing size-based queue of size: {}", queue.len());
                                let upload_result = upload_to_qdrant(&config.collection_name, client.clone(), queue.clone()).await?;
                                if upload_result == 1 || upload_result == 2 {
                                    let is_committed = commit_offset(&consumer, &message_queue).await?;
                                    if !is_committed {
                                        error!("Failed to commit offsets to Kafka");
                                        running.store(false, Ordering::SeqCst);
                                    } else {
                                        message_queue.clear();
                                        queue.clear();
                                    }
                                } else {
                                    error!("Failed to upload to qdrant");
                                    running.store(false, Ordering::SeqCst);
                                }
                            }
                        }
                        Err(e) => error!("Kafka error: {}", e),
                    }
                }
            }
        } else {
            // TODO: Separate running for qdrant and kafka
            tokio::time::sleep(tokio::time::Duration::from_secs(retry_delay)).await;
            retry_delay *= 2; // Exponential backoff
            running.store(true, Ordering::SeqCst);
        }
    }
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
                },
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
