use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct RelayMetrics {
    pub tap_dropped: AtomicU64,
    pub digest_mismatch: AtomicU64,
    pub relay_requests: AtomicU64,
    pub correlate_hit: AtomicU64,
    pub correlate_miss: AtomicU64,
    pub correlate_ambiguous: AtomicU64,
    pub tap_response_started: AtomicU64,
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

    pub fn inc_correlate_hit(&self) {
        self.correlate_hit.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_correlate_miss(&self) {
        self.correlate_miss.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_correlate_ambiguous(&self) {
        self.correlate_ambiguous.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_tap_response_started(&self) {
        self.tap_response_started.fetch_add(1, Ordering::Relaxed);
    }
}
