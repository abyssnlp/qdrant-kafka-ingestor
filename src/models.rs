use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QdrantPoint {
    pub id: Value,
    pub vector: Vec<f32>,
    pub payload: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub brokers: String,
    pub group_id: String,
    pub input_topic: String,
    pub collection_name: String,
    pub qdrant_url: String,
    pub flush_time: u64,
    pub flush_threshold: usize,
}
