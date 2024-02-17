use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use qdrant_client::client::QdrantClient;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::Message;

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

    // let stream = consumer.stream().try_for_each(|message| {
    //     async move {
    //         println!(
    //             "Message offset: {}, Message partition: {}, Message key: {:?}, Message body: {:?}",
    //             message.offset(),
    //             message.partition(),
    //             message.key_view::<str>(),
    //             message.payload_view::<str>()
    //         );

    //         // more computation

    //         Ok(())
    //     }
    // });

    // stream.await.expect("Stream processing failed!")
}

async fn upload_to_qdrant(collection_name: String, client: QdrantClient) {
    let result = client.upsert_points_batch_blocking(collection_name, None, vec![], None, 100);
}
