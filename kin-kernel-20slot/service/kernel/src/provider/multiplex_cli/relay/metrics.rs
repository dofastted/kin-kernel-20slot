#![allow(dead_code)] // consumed by the relay data plane in later stages

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct RelayMetrics {
    pub tap_dropped: AtomicU64,
    pub digest_mismatch: AtomicU64,
    pub relay_requests: AtomicU64,
}

impl RelayMetrics {
    pub fn inc_tap_dropped(&self) {
        self.tap_dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_digest_mismatch(&self) {
        self.digest_mismatch.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_relay_requests(&self) {
        self.relay_requests.fetch_add(1, Ordering::Relaxed);
    }
}
