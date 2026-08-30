use uuid::Uuid;

use crate::model::MessageRequest;

/// One in-flight HTTP turn bound to one CLI slot.
#[derive(Clone, Debug)]
pub struct Job {
    pub job_id: String,
    pub slot_id: String,
    pub request: MessageRequest,
}

pub fn new_id(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple())
}
