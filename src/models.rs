use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Debug)]
pub struct QdrantPoint {
    pub id: Value,
    pub vector: Vec<f32>,
    pub payload: qdrant_client::client::Payload,
}
