use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::KernelError;

use super::signing::{self, KCT_DOMAIN};

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
    ) -> Result<(Self, String), KernelError> {
        let token = Self {
            process_generation,
            slot_id: slot_id.into(),
            job_id: job_id.into(),
            logical_session_id: logical_session_id.into(),
            tool_call_id: tool_call_id.into(),
            expires_at: now_secs().saturating_add(ttl_secs),
            nonce: Uuid::new_v4().to_string(),
        };
        let encoded = token.encode(secret)?;
        Ok((token, encoded))
    }

    pub fn encode(&self, secret: &[u8]) -> Result<String, KernelError> {
        let payload = serde_json::to_vec(self).unwrap_or_default();
        let mac = signing::sign(KCT_DOMAIN, &payload, secret)
            .map_err(|err| KernelError::Provider(err.to_string()))?;
        Ok(format!("kct_{}.{}", hex(&payload), hex(&mac)))
    }

    #[cfg(test)]
    pub fn decode(raw: &str, secret: &[u8]) -> Result<Self, KernelError> {
        let trimmed = raw.strip_prefix("kct_").unwrap_or(raw);
        let (payload_hex, sig_hex) = trimmed
            .split_once('.')
            .ok_or_else(|| KernelError::ContinuationMismatch("malformed continuation".into()))?;
        let payload = unhex(payload_hex)
            .map_err(|_| KernelError::ContinuationMismatch("continuation payload".into()))?;
        let sig = unhex(sig_hex)
            .map_err(|_| KernelError::ContinuationMismatch("continuation mac".into()))?;
        if !signing::verify(KCT_DOMAIN, &payload, &sig, secret) {
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

    #[cfg(test)]
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

#[cfg(test)]
fn unhex(text: &str) -> Result<Vec<u8>, ()> {
    if !text.len().is_multiple_of(2) {
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
    for chunk in bytes.as_chunks::<2>().0 {
        out.push((nibble(chunk[0])? << 4) | nibble(chunk[1])?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_tamper() {
        let secret = b"kin-test-secret-32-bytes-pad!!!!";
        let (token, encoded) =
            ContinuationToken::issue(3, "s1", "j1", "sess", "toolu_1", 60, secret).unwrap();
        let parsed = ContinuationToken::decode(&encoded, secret).unwrap();
        assert_eq!(parsed, token);
        let mut bad = encoded;
        bad.replace_range(8..9, "a");
        assert!(ContinuationToken::decode(&bad, secret).is_err());
        assert!(token.matches_runtime(4).is_err());
    }

    #[test]
    fn empty_secret_is_rejected() {
        let token = ContinuationToken {
            process_generation: 3,
            slot_id: "s1".into(),
            job_id: "j1".into(),
            logical_session_id: "sess".into(),
            tool_call_id: "toolu_1".into(),
            expires_at: now_secs().saturating_add(60),
            nonce: "nonce".into(),
        };
        assert!(token.encode(b"").is_err());
        assert!(ContinuationToken::decode("kct_7b7d.0000", b"").is_err());
    }
}
