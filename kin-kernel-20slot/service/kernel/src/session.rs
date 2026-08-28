use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

use uuid::Uuid;

use crate::{error::KernelError, model::MessageRequest};

#[derive(Clone, Debug, PartialEq, Eq)]
enum Phase {
    Ready,
    WaitingTool,
}

#[derive(Clone, Debug)]
struct SessionRecord {
    worker_index: usize,
    worker_generation: u64,
    phase: Phase,
    expected_tool_use_ids: Vec<String>,
    continuation_token: Option<String>,
    pending_request: Option<MessageRequest>,
    reserved_worker: bool,
    expires_at: Instant,
}

#[derive(Clone, Debug)]
pub struct ResumeBinding {
    pub worker_index: usize,
    pub worker_generation: u64,
    pub pending_request: Option<MessageRequest>,
    pub reserved_worker: bool,
}

#[derive(Clone, Debug)]
pub struct ExpiredReservation {
    pub worker_index: usize,
    pub worker_generation: u64,
}

pub struct SessionDirectory {
    records: Mutex<HashMap<String, SessionRecord>>,
    session_ttl: Duration,
    continuation_ttl: Duration,
    max_session_bytes: usize,
}

impl SessionDirectory {
    pub fn new(
        session_ttl: Duration,
        continuation_ttl: Duration,
        max_session_bytes: usize,
    ) -> Self {
        Self {
            records: Mutex::new(HashMap::new()),
            session_ttl,
            continuation_ttl,
            max_session_bytes,
        }
    }

    pub fn sticky_worker(&self, tenant_id: &str, session_id: &str) -> Option<usize> {
        let key = key(tenant_id, session_id);
        let mut records = self.records.lock().expect("session directory poisoned");
        let expired = records
            .get(&key)
            .map(|record| record.expires_at <= Instant::now())
            .unwrap_or(false);
        if expired {
            let waiting = records
                .get(&key)
                .map(|record| record.phase == Phase::WaitingTool)
                .unwrap_or(false);
            if !waiting {
                records.remove(&key);
            }
            return None;
        }

        records
            .get(&key)
            .and_then(|record| (record.phase == Phase::Ready).then_some(record.worker_index))
    }

    pub fn mark_ready(
        &self,
        tenant_id: &str,
        session_id: &str,
        worker_index: usize,
        worker_generation: u64,
    ) {
        self.records
            .lock()
            .expect("session directory poisoned")
            .insert(
                key(tenant_id, session_id),
                SessionRecord {
                    worker_index,
                    worker_generation,
                    phase: Phase::Ready,
                    expected_tool_use_ids: Vec::new(),
                    continuation_token: None,
                    pending_request: None,
                    reserved_worker: false,
                    expires_at: Instant::now() + self.session_ttl,
                },
            );
    }

    #[allow(clippy::too_many_arguments)]
    pub fn mark_waiting(
        &self,
        tenant_id: &str,
        session_id: &str,
        worker_index: usize,
        worker_generation: u64,
        tool_use_ids: Vec<String>,
        pending_request: MessageRequest,
        reserved_worker: bool,
    ) -> Result<String, KernelError> {
        let pending_size = serde_json::to_vec(&pending_request)
            .map_err(|_| KernelError::Internal)?
            .len();
        if pending_size > self.max_session_bytes {
            return Err(KernelError::InvalidRequest(
                "continuation transcript exceeds the configured session limit".to_string(),
            ));
        }
        let token = format!("cont_{}", Uuid::new_v4().simple());
        self.records
            .lock()
            .expect("session directory poisoned")
            .insert(
                key(tenant_id, session_id),
                SessionRecord {
                    worker_index,
                    worker_generation,
                    phase: Phase::WaitingTool,
                    expected_tool_use_ids: normalized_ids(tool_use_ids),
                    continuation_token: Some(token.clone()),
                    pending_request: Some(pending_request),
                    reserved_worker,
                    expires_at: Instant::now() + self.continuation_ttl,
                },
            );
        Ok(token)
    }

    pub fn resume(
        &self,
        tenant_id: &str,
        session_id: &str,
        continuation_token: &str,
        tool_use_ids: &[String],
    ) -> Result<ResumeBinding, KernelError> {
        let key = key(tenant_id, session_id);
        let mut records = self.records.lock().expect("session directory poisoned");
        if records
            .get(&key)
            .map(|record| record.expires_at <= Instant::now())
            .unwrap_or(false)
        {
            return Err(KernelError::ContinuationMismatch(
                "continuation has expired".to_string(),
            ));
        }
        let record = records
            .get_mut(&key)
            .ok_or_else(|| KernelError::ContinuationMismatch("unknown session".to_string()))?;
        if record.phase != Phase::WaitingTool {
            return Err(KernelError::ContinuationMismatch(
                "session is not waiting for a tool result".to_string(),
            ));
        }
        if record.continuation_token.as_deref() != Some(continuation_token) {
            return Err(KernelError::ContinuationMismatch(
                "continuation token does not match".to_string(),
            ));
        }
        let normalized = normalized_ids(tool_use_ids.to_vec());
        if normalized.len() != tool_use_ids.len() {
            return Err(KernelError::ContinuationMismatch(
                "duplicate tool_result ids are not allowed".to_string(),
            ));
        }
        if record.expected_tool_use_ids != normalized {
            return Err(KernelError::ContinuationMismatch(
                "tool_result ids do not match the pending tool calls".to_string(),
            ));
        }

        let binding = ResumeBinding {
            worker_index: record.worker_index,
            worker_generation: record.worker_generation,
            pending_request: record.pending_request.take(),
            reserved_worker: record.reserved_worker,
        };
        record.phase = Phase::Ready;
        record.continuation_token = None;
        record.expected_tool_use_ids.clear();
        record.expires_at = Instant::now() + self.session_ttl;
        Ok(binding)
    }

    pub fn sweep_expired(&self) -> Vec<ExpiredReservation> {
        let now = Instant::now();
        let mut expired = Vec::new();
        self.records
            .lock()
            .expect("session directory poisoned")
            .retain(|_, record| {
                if record.expires_at > now {
                    return true;
                }
                if record.phase == Phase::WaitingTool && record.reserved_worker {
                    expired.push(ExpiredReservation {
                        worker_index: record.worker_index,
                        worker_generation: record.worker_generation,
                    });
                }
                false
            });
        expired
    }
}

fn key(tenant_id: &str, session_id: &str) -> String {
    format!("{tenant_id}\u{1f}{session_id}")
}

fn normalized_ids(mut ids: Vec<String>) -> Vec<String> {
    ids.sort();
    ids.dedup();
    ids
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::model::{Message, MessageContent, MessageRequest};

    use super::SessionDirectory;

    #[test]
    fn continuation_is_single_use_and_tool_bound() {
        let sessions = SessionDirectory::new(
            Duration::from_secs(60),
            Duration::from_secs(60),
            1024 * 1024,
        );
        let pending = MessageRequest {
            model: "mock-agent".to_string(),
            messages: vec![Message {
                role: "user".to_string(),
                content: MessageContent::Text("hello".to_string()),
                tool_call_id: None,
                tool_calls: Vec::new(),
            }],
            max_tokens: 128,
            stream: false,
            ..MessageRequest::default()
        };
        let token = sessions
            .mark_waiting("t", "s", 1, 7, vec!["tool-a".to_string()], pending, true)
            .expect("store continuation");

        assert!(
            sessions
                .resume("t", "s", &token, &["tool-b".to_string()])
                .is_err()
        );
        let binding = sessions
            .resume("t", "s", &token, &["tool-a".to_string()])
            .expect("valid continuation");
        assert_eq!(binding.worker_index, 1);
        assert_eq!(binding.worker_generation, 7);
        assert!(
            sessions
                .resume("t", "s", &token, &["tool-a".to_string()])
                .is_err()
        );
    }

    #[test]
    fn expired_native_wait_returns_reservation() {
        let sessions = SessionDirectory::new(Duration::from_secs(60), Duration::ZERO, 1024 * 1024);
        let pending = MessageRequest {
            model: "mock-agent".to_string(),
            messages: vec![Message {
                role: "user".to_string(),
                content: MessageContent::Text("hello".to_string()),
                tool_call_id: None,
                tool_calls: Vec::new(),
            }],
            max_tokens: 128,
            stream: false,
            ..MessageRequest::default()
        };
        sessions
            .mark_waiting("t", "s", 2, 9, vec!["tool-a".to_string()], pending, true)
            .expect("store continuation");
        let expired = sessions.sweep_expired();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].worker_index, 2);
        assert_eq!(expired[0].worker_generation, 9);
    }
}
