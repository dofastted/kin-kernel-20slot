use std::{collections::HashMap, time::Instant};

use serde_json::{Value, json};
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::{error::KernelError, model::MessageRequest};

#[derive(Clone, Debug)]
pub struct Job {
    pub job_id: String,
    pub tenant_id: String,
    pub session_id: String,
    pub slot_id: String,
    pub generation: u64,
    pub request: MessageRequest,
}

#[derive(Debug)]
pub enum SlotWaitPayload {
    Job(Job),
    Retire,
}

#[derive(Default)]
pub struct PendingCalls {
    slot_wait: HashMap<String, oneshot::Sender<SlotWaitPayload>>,
    client_tool: HashMap<String, oneshot::Sender<Value>>,
    done: HashMap<String, oneshot::Sender<JobOutcome>>,
}

#[derive(Clone, Debug)]
pub struct JobOutcome {
    pub text: String,
    pub is_error: bool,
}

impl PendingCalls {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_slot_wait(&mut self, slot_id: &str) -> oneshot::Receiver<SlotWaitPayload> {
        let (tx, rx) = oneshot::channel();
        if let Some(prev) = self.slot_wait.insert(slot_id.to_string(), tx) {
            let _ = prev.send(SlotWaitPayload::Retire);
        }
        rx
    }

    pub fn wake_slot(
        &mut self,
        slot_id: &str,
        payload: SlotWaitPayload,
    ) -> Result<(), KernelError> {
        self.slot_wait
            .remove(slot_id)
            .ok_or(KernelError::NoCapacity)?
            .send(payload)
            .map_err(|_| KernelError::Provider("slot waiter dropped".into()))
    }

    pub fn register_client_tool(
        &mut self,
        job_id: &str,
        tool_id: Option<&str>,
    ) -> oneshot::Receiver<Value> {
        let (tx, rx) = oneshot::channel();
        self.client_tool.insert(client_key(job_id, tool_id), tx);
        rx
    }

    pub fn complete_client_tool(
        &mut self,
        job_id: &str,
        tool_id: Option<&str>,
        result: Value,
    ) -> Result<(), KernelError> {
        let key = client_key(job_id, tool_id);
        if let Some(tx) = self.client_tool.remove(&key) {
            return tx.send(result).map_err(|_| KernelError::ContinuationLost);
        }
        if let Some(tx) = self.client_tool.remove(job_id) {
            return tx.send(result).map_err(|_| KernelError::ContinuationLost);
        }
        Err(KernelError::ContinuationLost)
    }

    pub fn complete_client_tools(
        &mut self,
        job_id: &str,
        results: Vec<(String, Value)>,
    ) -> Result<(), KernelError> {
        if results.is_empty() {
            return self.complete_client_tool(job_id, None, json!({ "content": "" }));
        }
        for (tool_id, result) in results {
            self.complete_client_tool(job_id, Some(&tool_id), result)?;
        }
        Ok(())
    }

    pub fn register_done(&mut self, job_id: &str) -> oneshot::Receiver<JobOutcome> {
        let (tx, rx) = oneshot::channel();
        self.done.insert(job_id.to_string(), tx);
        rx
    }

    pub fn finish_job(&mut self, job_id: &str, outcome: JobOutcome) -> Result<(), KernelError> {
        if let Some(tx) = self.done.remove(job_id) {
            let _ = tx.send(outcome);
        }
        Ok(())
    }

    pub fn drop_job(&mut self, job_id: &str) {
        self.client_tool
            .retain(|key, _| key != job_id && !key.starts_with(&format!("{job_id}:")));
        self.done.remove(job_id);
    }

    pub fn abort_client_tools(&mut self, job_id: &str, message: &str) {
        let prefix = format!("{job_id}:");
        let keys: Vec<String> = self
            .client_tool
            .keys()
            .filter(|key| key.as_str() == job_id || key.starts_with(&prefix))
            .cloned()
            .collect();
        for key in keys {
            if let Some(tx) = self.client_tool.remove(&key) {
                let tool_id = key
                    .strip_prefix(&prefix)
                    .filter(|id| !id.is_empty())
                    .unwrap_or("");
                let _ = tx.send(json!({
                    "tool_use_id": tool_id,
                    "content": message,
                    "is_error": true
                }));
            }
        }
    }
}

fn client_key(job_id: &str, tool_id: Option<&str>) -> String {
    match tool_id {
        Some(id) if !id.is_empty() => format!("{job_id}:{id}"),
        _ => job_id.to_string(),
    }
}

pub fn new_id(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple())
}

pub fn now() -> Instant {
    Instant::now()
}
