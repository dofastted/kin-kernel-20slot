use axum::body::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::KernelError;

use super::super::{
    Runtime,
    signing::{self, KRC_DOMAIN},
};

pub const KRC_PREFIX: &str = "krc_";
const MAX_TOKEN_BYTES: usize = 2 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RelayContextToken {
    pub job_id: String,
    pub slot_id: String,
    pub generation: u64,
    pub nonce: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CorrelatedJob {
    pub job_id: String,
    pub slot_id: String,
    pub generation: u64,
}

impl RelayContextToken {
    pub fn issue(
        job_id: impl Into<String>,
        slot_id: impl Into<String>,
        generation: u64,
        secret: &[u8],
    ) -> Result<String, KernelError> {
        let token = Self {
            job_id: job_id.into(),
            slot_id: slot_id.into(),
            generation,
            nonce: Uuid::new_v4().to_string(),
        };
        token.encode(secret)
    }

    pub fn encode(&self, secret: &[u8]) -> Result<String, KernelError> {
        let payload = serde_json::to_vec(self).unwrap_or_default();
        let mac = signing::sign(KRC_DOMAIN, &payload, secret)
            .map_err(|err| KernelError::Provider(err.to_string()))?;
        Ok(format!("{KRC_PREFIX}{}.{}", hex(&payload), hex(&mac)))
    }

    pub fn decode(raw: &str, secret: &[u8]) -> Result<Self, KernelError> {
        if raw.len() > MAX_TOKEN_BYTES {
            return Err(KernelError::ContinuationMismatch(
                "relay context too long".into(),
            ));
        }
        let trimmed = raw.strip_prefix(KRC_PREFIX).unwrap_or(raw);
        let (payload_hex, sig_hex) = trimmed
            .split_once('.')
            .ok_or_else(|| KernelError::ContinuationMismatch("malformed relay context".into()))?;
        let payload = unhex(payload_hex)
            .map_err(|_| KernelError::ContinuationMismatch("relay context payload".into()))?;
        let sig = unhex(sig_hex)
            .map_err(|_| KernelError::ContinuationMismatch("relay context mac".into()))?;
        if !signing::verify(KRC_DOMAIN, &payload, &sig, secret) {
            return Err(KernelError::ContinuationMismatch(
                "relay context signature".into(),
            ));
        }
        serde_json::from_slice(&payload)
            .map_err(|_| KernelError::ContinuationMismatch("relay context json".into()))
    }
}

#[derive(Debug, Default)]
pub struct ContextScanner {
    buf: Vec<u8>,
    candidates: Vec<String>,
}

impl ContextScanner {
    pub fn push(&mut self, bytes: &Bytes) {
        self.buf.extend_from_slice(bytes);
        self.scan(false);
    }

    pub fn finish(&mut self) {
        self.scan(true);
        self.buf.clear();
    }

    pub async fn last_valid(&self, runtime: &Runtime) -> Option<CorrelatedJob> {
        last_valid_candidate(&self.candidates, runtime).await
    }

    pub(crate) fn candidates(&self) -> &[String] {
        &self.candidates
    }

    fn scan(&mut self, final_chunk: bool) {
        let mut index = 0;
        let mut keep_from = None;
        while index + KRC_PREFIX.len() <= self.buf.len() {
            if &self.buf[index..index + KRC_PREFIX.len()] != KRC_PREFIX.as_bytes() {
                index += 1;
                continue;
            }
            let start = index;
            let mut cursor = index + KRC_PREFIX.len();
            while cursor < self.buf.len() && is_lower_hex(self.buf[cursor]) {
                cursor += 1;
                if cursor - start > MAX_TOKEN_BYTES {
                    index = cursor;
                    continue;
                }
            }
            if cursor == self.buf.len() && !final_chunk {
                keep_from = Some(start);
                break;
            }
            if cursor == index + KRC_PREFIX.len() || self.buf.get(cursor) != Some(&b'.') {
                index += 1;
                continue;
            }
            cursor += 1;
            let mac_start = cursor;
            while cursor < self.buf.len() && is_lower_hex(self.buf[cursor]) {
                cursor += 1;
                if cursor - start > MAX_TOKEN_BYTES {
                    index = cursor;
                    continue;
                }
            }
            if cursor == self.buf.len() && !final_chunk {
                keep_from = Some(start);
                break;
            }
            if cursor - start <= MAX_TOKEN_BYTES
                && cursor > mac_start
                && let Ok(candidate) = std::str::from_utf8(&self.buf[start..cursor])
            {
                self.candidates.push(candidate.to_string());
            }
            index = cursor.max(index + 1);
        }
        if let Some(start) = keep_from {
            self.buf.drain(..start);
            if self.buf.len() > MAX_TOKEN_BYTES {
                let drop = self.buf.len() - KRC_PREFIX.len().saturating_sub(1);
                self.buf.drain(..drop);
            }
        } else {
            let keep = partial_prefix_len(&self.buf);
            if keep == 0 {
                self.buf.clear();
            } else {
                let split = self.buf.len() - keep;
                self.buf.drain(..split);
            }
        }
    }
}

pub async fn last_valid_candidate(
    candidates: &[String],
    runtime: &Runtime,
) -> Option<CorrelatedJob> {
    for raw in candidates.iter().rev() {
        let Ok(token) = RelayContextToken::decode(raw, runtime.secret()) else {
            continue;
        };
        if let Some(job) = runtime.correlate_lookup(&token).await {
            return Some(job);
        }
    }
    None
}

fn is_lower_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')
}

fn partial_prefix_len(buf: &[u8]) -> usize {
    let prefix = KRC_PREFIX.as_bytes();
    let max = buf.len().min(prefix.len() - 1);
    for len in (1..=max).rev() {
        if buf[buf.len() - len..] == prefix[..len] {
            return len;
        }
    }
    0
}

fn hex(data: &[u8]) -> String {
    const H: &[u8] = b"0123456789abcdef";
    let mut out = String::with_capacity(data.len() * 2);
    for byte in data {
        out.push(H[(byte >> 4) as usize] as char);
        out.push(H[(byte & 0x0f) as usize] as char);
    }
    out
}

fn unhex(text: &str) -> Result<Vec<u8>, ()> {
    if !text.len().is_multiple_of(2) {
        return Err(());
    }
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let nibble = |c: u8| match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        _ => Err(()),
    };
    for chunk in bytes.as_chunks::<2>().0 {
        out.push((nibble(chunk[0])? << 4) | nibble(chunk[1])?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc, time::Duration};

    use crate::{
        config::RelayMode,
        model::MessageRequest,
        provider::multiplex_cli::{
            MultiplexConfig,
            pending_call::Job,
            slot::{Slot, SlotPhase},
        },
    };

    use super::*;

    fn test_cfg() -> MultiplexConfig {
        MultiplexConfig {
            slot_count: 1,
            simulate: true,
            bin: PathBuf::from("simulated"),
            mock_bin: true,
            model: "claude-sonnet-5".into(),
            retire_after_turn: false,
            max_jobs_per_slot: 32,
            slot_max_lifetime: Duration::from_secs(1800),
            session_idle_ttl: Duration::from_secs(600),
            simulate_latency: Duration::from_millis(1),
            continuation_ttl_secs: 600,
            client_stall_timeout: Duration::from_secs(30),
            relay_mode: RelayMode::Observe,
            relay_addr: "127.0.0.1:0".parse().unwrap(),
            relay_upstream: "https://api.anthropic.com".into(),
        }
    }

    async fn runtime_with_job(job_id: &str, slot_id: &str, generation: u64) -> Arc<Runtime> {
        let runtime = Runtime::new(test_cfg());
        runtime
            .process_generation
            .store(generation, std::sync::atomic::Ordering::Relaxed);
        runtime.jobs.lock().await.insert(
            job_id.to_string(),
            Job {
                job_id: job_id.to_string(),
                tenant_id: "tenant".into(),
                session_id: "session".into(),
                slot_id: slot_id.to_string(),
                generation,
                request: MessageRequest::default(),
            },
        );
        let mut slot = Slot::new(slot_id);
        slot.phase = SlotPhase::Running;
        slot.job_id = Some(job_id.to_string());
        runtime.slots.lock().await.push(slot);
        runtime
    }

    #[test]
    fn scanner_handles_split_token_and_ignores_fakes() {
        let secret = b"kin-test-secret-32-bytes-pad!!!!";
        let token = RelayContextToken {
            job_id: "job-1".into(),
            slot_id: "slot-1".into(),
            generation: 7,
            nonce: "nonce".into(),
        }
        .encode(secret)
        .unwrap();
        let split = token.len() / 2;
        let mut scanner = ContextScanner::default();
        scanner.push(&Bytes::from(format!("fake krc_bad {}", &token[..split])));
        scanner.push(&Bytes::from(format!("{} end", &token[split..])));
        scanner.finish();
        assert_eq!(scanner.candidates(), &[token]);
    }

    #[tokio::test]
    async fn multiple_tokens_take_last_valid() {
        let runtime = runtime_with_job("job-good", "slot-1", 7).await;
        let stale = RelayContextToken {
            job_id: "job-old".into(),
            slot_id: "slot-1".into(),
            generation: 7,
            nonce: "n1".into(),
        }
        .encode(runtime.secret())
        .unwrap();
        let good = RelayContextToken {
            job_id: "job-good".into(),
            slot_id: "slot-1".into(),
            generation: 7,
            nonce: "n2".into(),
        }
        .encode(runtime.secret())
        .unwrap();
        let mut scanner = ContextScanner::default();
        scanner.push(&Bytes::from(format!("{stale} {good}")));
        scanner.finish();
        assert_eq!(
            scanner.last_valid(&runtime).await.unwrap().job_id,
            "job-good"
        );
    }

    #[tokio::test]
    async fn rejects_old_generation_missing_job_and_moved_slot() {
        let runtime = runtime_with_job("job-good", "slot-1", 7).await;
        let old_generation = RelayContextToken {
            job_id: "job-good".into(),
            slot_id: "slot-1".into(),
            generation: 6,
            nonce: "n".into(),
        }
        .encode(runtime.secret())
        .unwrap();
        let missing_job = RelayContextToken {
            job_id: "job-missing".into(),
            slot_id: "slot-1".into(),
            generation: 7,
            nonce: "n".into(),
        }
        .encode(runtime.secret())
        .unwrap();
        {
            let mut scanner = ContextScanner::default();
            scanner.push(&Bytes::from(format!("{old_generation} {missing_job}")));
            scanner.finish();
            assert!(scanner.last_valid(&runtime).await.is_none());
        }
        runtime.slots.lock().await[0].job_id = Some("other-job".into());
        let moved = RelayContextToken {
            job_id: "job-good".into(),
            slot_id: "slot-1".into(),
            generation: 7,
            nonce: "n".into(),
        }
        .encode(runtime.secret())
        .unwrap();
        let mut scanner = ContextScanner::default();
        scanner.push(&Bytes::from(moved));
        scanner.finish();
        assert!(scanner.last_valid(&runtime).await.is_none());
    }

    #[test]
    fn overlong_and_pseudo_tokens_are_ignored() {
        let mut scanner = ContextScanner::default();
        scanner.push(&Bytes::from(format!("krc_{}.abcd", "a".repeat(3000))));
        scanner.push(&Bytes::from(" krc_zz.11 krc_abc."));
        scanner.finish();
        assert!(
            scanner.candidates().is_empty(),
            "{:?}",
            scanner.candidates()
        );
        assert!(RelayContextToken::decode("krc_7b7d.0000", b"secret").is_err());
    }
}
