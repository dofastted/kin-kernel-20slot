use std::fmt;

use hmac::{Hmac, Mac};
use sha2::Sha256;

pub const KCT_DOMAIN: &str = "kin/kct/v1";

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SigningError {
    EmptySecret,
}

impl fmt::Display for SigningError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptySecret => f.write_str("signing secret must not be empty"),
        }
    }
}

impl std::error::Error for SigningError {}

pub fn sign(domain: &str, payload: &[u8], secret: &[u8]) -> Result<[u8; 32], SigningError> {
    if secret.is_empty() {
        return Err(SigningError::EmptySecret);
    }
    let mut mac = HmacSha256::new_from_slice(secret).map_err(|_| SigningError::EmptySecret)?;
    mac.update(domain.as_bytes());
    mac.update(b"\0");
    mac.update(payload);
    let bytes = mac.finalize().into_bytes();
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

/// Counterpart of `sign`, exercised by `ContinuationToken::decode`; the
/// kernel authenticates continuations by server-side lookup, so verification
/// has no production caller.
#[cfg(test)]
pub fn verify(domain: &str, payload: &[u8], signature: &[u8], secret: &[u8]) -> bool {
    if secret.is_empty() {
        return false;
    }
    let Ok(mut mac) = HmacSha256::new_from_slice(secret) else {
        return false;
    };
    mac.update(domain.as_bytes());
    mac.update(b"\0");
    mac.update(payload);
    mac.verify_slice(signature).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const OTHER_DOMAIN: &str = "kin/other/v1";

    #[test]
    fn domains_are_isolated() {
        let secret = b"kin-test-secret-32-bytes-pad!!!!";
        let payload = br#"{"job_id":"j1"}"#;
        let sig = sign(KCT_DOMAIN, payload, secret).unwrap();

        assert!(verify(KCT_DOMAIN, payload, &sig, secret));
        assert!(!verify(OTHER_DOMAIN, payload, &sig, secret));
    }

    #[test]
    fn empty_secret_is_rejected() {
        let payload = b"payload";

        assert_eq!(
            sign(KCT_DOMAIN, payload, b"").unwrap_err(),
            SigningError::EmptySecret
        );
        assert!(!verify(KCT_DOMAIN, payload, &[0; 32], b""));
    }

    #[test]
    fn tampering_is_detected() {
        let secret = b"kin-test-secret-32-bytes-pad!!!!";
        let payload = b"payload";
        let sig = sign(OTHER_DOMAIN, payload, secret).unwrap();

        assert!(!verify(OTHER_DOMAIN, b"tampered", &sig, secret));

        let mut bad_sig = sig;
        bad_sig[0] ^= 0x01;
        assert!(!verify(OTHER_DOMAIN, payload, &bad_sig, secret));
    }
}
