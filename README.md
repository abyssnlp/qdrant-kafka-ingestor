# Qdrant Kafka Ingestor


## Overview

The Qdrant Kafka Ingestor is a Rust-based CLI application designed to ingest data from Kafka and upload it to a Qdrant database. This tool is particularly useful for integrating streaming data pipelines with vector search engines.



## CLI Arguments

The application accepts several command-line arguments to configure its behavior:

- `--brokers` or `-b`: Specifies the Kafka brokers (required).

- `--group_id` or `-g`: Specifies the Kafka consumer group ID (required).

- `--input_topic` or `-t`: Specifies the Kafka topic from which to consume data (required).

- `--collection_name` or `-c`: Specifies the Qdrant collection name to which data will be uploaded (required).

- `--qdrant_url` or `-q`: Specifies the URL of the Qdrant server (required).

- `--flush_time` or `-f`: Specifies the time interval (in seconds) after which the data is flushed to Qdrant (default: 4).

- `--flush_threshold` or `-s`: Specifies the size threshold after which the data is flushed to Qdrant (default: 100).

- `--num_consumers` or `-n`: Specifies the number of consumers to use (default: 2).

## Example Usage

To run the application with the required arguments, use the following command:

```shell
./qdrant_kafka_ingestor --brokers localhost:9092 --group_id my_group --input_topic my_topic --collection_name my_collection --qdrant_url http://localhost:6333 --num_consumers 2
```


This command configures the ingestor to connect to Kafka running on `localhost:9092`, using the consumer group `my_group`, and consuming messages from the topic `my_topic`. It will upload the ingested data to the Qdrant collection `my_collection` at `http://localhost:6333`.

## Building

To build the application, use the following command:

```shell
git clone https://github.com/abyssnlp/qdrant-queue-ingestor.git
cd qdrant_kafka_ingestor
cargo build --release
```


The compiled binary will be available in `./target/release/`.


## Reporting Issues

If you encounter any problems while using the Qdrant Kafka Ingestor, please open an issue in the GitHub repository. Provide detailed information about the issue, including steps to reproduce, error messages, and environment details, to help us resolve the issue efficiently.