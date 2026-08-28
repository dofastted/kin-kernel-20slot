use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::KernelError;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContinuationToken {
    pub process_generation: u64,
    pub slot_id: String,
    pub job_id: String,
    pub logical_session_id: String,
    pub tool_call_id: String,
    pub expires_at: i64,
    pub nonce: String,
}

impl ContinuationToken {
    pub fn issue(
        process_generation: u64,
        slot_id: impl Into<String>,
        job_id: impl Into<String>,
        logical_session_id: impl Into<String>,
        tool_call_id: impl Into<String>,
        ttl_secs: i64,
        secret: &[u8],
    ) -> (Self, String) {
        let token = Self {
            process_generation,
            slot_id: slot_id.into(),
            job_id: job_id.into(),
            logical_session_id: logical_session_id.into(),
            tool_call_id: tool_call_id.into(),
            expires_at: now_secs().saturating_add(ttl_secs),
            nonce: Uuid::new_v4().to_string(),
        };
        let encoded = token.encode(secret);
        (token, encoded)
    }

    pub fn encode(&self, secret: &[u8]) -> String {
        let payload = serde_json::to_vec(self).unwrap_or_default();
        format!("kct_{}.{}", hex(&payload), hex(&mac(secret, &payload)))
    }

    pub fn decode(raw: &str, secret: &[u8]) -> Result<Self, KernelError> {
        let trimmed = raw.strip_prefix("kct_").unwrap_or(raw);
        let (payload_hex, sig_hex) = trimmed
            .split_once('.')
            .ok_or_else(|| KernelError::ContinuationMismatch("malformed continuation".into()))?;
        let payload = unhex(payload_hex)
            .map_err(|_| KernelError::ContinuationMismatch("continuation payload".into()))?;
        let sig = unhex(sig_hex)
            .map_err(|_| KernelError::ContinuationMismatch("continuation mac".into()))?;
        if sig != mac(secret, &payload) {
            return Err(KernelError::ContinuationMismatch(
                "continuation signature".into(),
            ));
        }
        let token: Self = serde_json::from_slice(&payload)
            .map_err(|_| KernelError::ContinuationMismatch("continuation json".into()))?;
        if token.expires_at <= now_secs() {
            return Err(KernelError::ContinuationLost);
        }
        Ok(token)
    }

    pub fn matches_runtime(&self, process_generation: u64) -> Result<(), KernelError> {
        if self.process_generation != process_generation {
            return Err(KernelError::ContinuationLost);
        }
        Ok(())
    }
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
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
    if text.len() % 2 != 0 {
        return Err(());
    }
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let nibble = |c: u8| match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(()),
    };
    for chunk in bytes.chunks_exact(2) {
        out.push((nibble(chunk[0])? << 4) | nibble(chunk[1])?);
    }
    Ok(out)
}

fn mac(secret: &[u8], payload: &[u8]) -> [u8; 32] {
    let mut out = [0u8; 32];
    if secret.is_empty() {
        return out;
    }
    for (i, byte) in secret.iter().cycle().take(32).enumerate() {
        out[i] = *byte;
    }
    for (i, byte) in payload.iter().enumerate() {
        let idx = i % 32;
        out[idx] ^= byte.wrapping_add(i as u8);
        out[(idx + 7) % 32] = out[(idx + 7) % 32].wrapping_add(out[idx].rotate_left(3));
        out[(idx + 13) % 32] ^= out[idx].wrapping_mul(31);
    }
    for _ in 0..4 {
        for i in 0..32 {
            out[i] = out[i]
                .rotate_left(5)
                .wrapping_add(out[(i + 11) % 32])
                ^ secret[i % secret.len()];
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_tamper() {
        let secret = b"kin-test-secret-32-bytes-pad!!!!";
        let (token, encoded) =
            ContinuationToken::issue(3, "s1", "j1", "sess", "toolu_1", 60, secret);
        let parsed = ContinuationToken::decode(&encoded, secret).unwrap();
        assert_eq!(parsed, token);
        let mut bad = encoded;
        bad.replace_range(8..9, "a");
        assert!(ContinuationToken::decode(&bad, secret).is_err());
        assert!(token.matches_runtime(4).is_err());
    }
}
