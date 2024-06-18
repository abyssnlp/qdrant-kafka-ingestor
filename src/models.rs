use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QdrantPoint {
    pub id: Value,
    pub vector: Vec<f32>,
    pub payload: Option<Value>,
}
