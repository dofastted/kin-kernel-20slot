//! Memory-aware admission for the 20-slot single-process runtime.
//!
//! Targets (kernel + one Claude OS process):
//!   < 3 GiB        admit all 20 slots
//!   3.0–3.5 GiB    keep in-flight work, refuse large requests
//!   3.5–3.75 GiB   drain: no new requests
//!   > 3.75 GiB     503 overloaded
//!
//! 4 GiB is the cgroup hard cap. 20 concurrent 1M-token / huge tool_result
//! payloads cannot be guaranteed inside 4 GiB.
//!
//! Admission uses kernel+Claude RSS by default. The sandbox cgroup often
//! includes unrelated processes (builds, traces) and would false-drain.
//! Set KIN_MEM_OBSERVED=cgroup to classify on memory.current instead.

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};

use serde::Serialize;

use crate::error::KernelError;

pub const MIB: u64 = 1024 * 1024;
pub const GIB: u64 = 1024 * MIB;

#[derive(Clone, Copy, Debug)]
pub struct MemoryLimits {
    pub soft: u64,
    pub drain: u64,
    pub reject: u64,
    pub max_inflight_payload: usize,
    pub max_pending: usize,
    pub large_request_bytes: usize,
}

impl MemoryLimits {
    pub fn production() -> Self {
        Self {
            soft: 3 * GIB,
            drain: 3500 * MIB,
            reject: 3750 * MIB,
            max_inflight_payload: (256 * MIB) as usize,
            max_pending: 100,
            large_request_bytes: (256 * MIB) as usize,
        }
    }

    pub fn from_env() -> Self {
        let mut limits = Self::production();
        if let Ok(value) = std::env::var("KIN_MEM_SOFT_BYTES")
            && let Ok(parsed) = value.parse()
        {
            limits.soft = parsed;
        }
        if let Ok(value) = std::env::var("KIN_MEM_DRAIN_BYTES")
            && let Ok(parsed) = value.parse()
        {
            limits.drain = parsed;
        }
        if let Ok(value) = std::env::var("KIN_MEM_REJECT_BYTES")
            && let Ok(parsed) = value.parse()
        {
            limits.reject = parsed;
        }
        if let Ok(value) = std::env::var("KIN_MAX_INFLIGHT_PAYLOAD")
            && let Ok(parsed) = value.parse()
        {
            limits.max_inflight_payload = parsed;
        }
        if let Ok(value) = std::env::var("KIN_MAX_PENDING")
            && let Ok(parsed) = value.parse()
        {
            limits.max_pending = parsed;
        }
        limits
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Admission {
    Allow,
    AllowSmall,
    Drain,
    Reject,
}

#[derive(Clone, Debug, Serialize)]
pub struct MemorySnapshot {
    pub kernel_rss_bytes: u64,
    pub claude_rss_bytes: u64,
    pub cgroup_bytes: u64,
    pub service_rss_bytes: u64,
    pub observed_bytes: u64,
    pub inflight_payload: usize,
    pub pending: usize,
    pub admission: Admission,
}

pub struct MemoryGuard {
    limits: MemoryLimits,
    inflight_payload: AtomicUsize,
    pending: AtomicUsize,
    claude_pid: AtomicU32,
    rss_override: AtomicU64,
}

impl MemoryGuard {
    pub fn new(limits: MemoryLimits) -> Self {
        Self {
            limits,
            inflight_payload: AtomicUsize::new(0),
            pending: AtomicUsize::new(0),
            claude_pid: AtomicU32::new(0),
            rss_override: AtomicU64::new(0),
        }
    }

    pub fn from_env() -> Self {
        Self::new(MemoryLimits::from_env())
    }

    pub fn set_claude_pid(&self, pid: u32) {
        self.claude_pid.store(pid, Ordering::Relaxed);
    }

    pub fn set_rss_override(&self, bytes: u64) {
        self.rss_override.store(bytes, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MemorySnapshot {
        let kernel = rss_of(std::process::id()).unwrap_or(0);
        let claude = rss_of(self.claude_pid.load(Ordering::Relaxed)).unwrap_or(0);
        let service = kernel.saturating_add(claude);
        let cgroup = cgroup_current().unwrap_or(0);
        let mut observed = if observe_cgroup() && cgroup > 0 {
            service.max(cgroup)
        } else {
            service
        };
        let override_rss = self.rss_override.load(Ordering::Relaxed);
        if override_rss > 0 {
            observed = override_rss;
        }
        MemorySnapshot {
            kernel_rss_bytes: kernel,
            claude_rss_bytes: claude,
            cgroup_bytes: cgroup,
            service_rss_bytes: service,
            observed_bytes: observed,
            inflight_payload: self.inflight_payload.load(Ordering::Relaxed),
            pending: self.pending.load(Ordering::Relaxed),
            admission: classify(observed, &self.limits),
        }
    }

    pub fn admit(&self, request_bytes: usize) -> Result<(), KernelError> {
        let snap = self.snapshot();
        let pending = self.pending.load(Ordering::Relaxed);
        if pending >= self.limits.max_pending {
            return Err(KernelError::Overloaded {
                retry_after: Some("1".into()),
            });
        }
        let inflight = self.inflight_payload.load(Ordering::Relaxed);
        if inflight.saturating_add(request_bytes) > self.limits.max_inflight_payload {
            return Err(KernelError::Overloaded {
                retry_after: Some("1".into()),
            });
        }
        let large = request_bytes >= self.limits.large_request_bytes;
        match snap.admission {
            Admission::Allow => Ok(()),
            Admission::AllowSmall if !large => Ok(()),
            Admission::AllowSmall | Admission::Drain | Admission::Reject => {
                Err(KernelError::Overloaded {
                    retry_after: Some("2".into()),
                })
            }
        }
    }

    pub fn begin(&self, request_bytes: usize) {
        self.pending.fetch_add(1, Ordering::Relaxed);
        self.inflight_payload
            .fetch_add(request_bytes, Ordering::Relaxed);
    }

    pub fn end(&self, request_bytes: usize) {
        self.pending.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
            Some(n.saturating_sub(1))
        }).ok();
        self.inflight_payload
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(request_bytes))
            })
            .ok();
    }
}

fn observe_cgroup() -> bool {
    matches!(
        std::env::var("KIN_MEM_OBSERVED").as_deref(),
        Ok("cgroup") | Ok("Cgroup") | Ok("CGROUP")
    )
}

fn classify(observed: u64, limits: &MemoryLimits) -> Admission {
    if observed >= limits.reject {
        Admission::Reject
    } else if observed >= limits.drain {
        Admission::Drain
    } else if observed >= limits.soft {
        Admission::AllowSmall
    } else {
        Admission::Allow
    }
}

fn rss_of(pid: u32) -> Option<u64> {
    if pid == 0 {
        return None;
    }
    let text = std::fs::read_to_string(format!("/proc/{pid}/statm")).ok()?;
    let pages: u64 = text.split_whitespace().nth(1)?.parse().ok()?;
    Some(pages.saturating_mul(page_size()))
}

fn page_size() -> u64 {
    4096
}

fn cgroup_current() -> Option<u64> {
    std::fs::read_to_string("/sys/fs/cgroup/memory.current")
        .ok()
        .and_then(|text| text.trim().parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bands() {
        let limits = MemoryLimits::production();
        assert_eq!(classify(2 * GIB, &limits), Admission::Allow);
        assert_eq!(classify(3 * GIB, &limits), Admission::AllowSmall);
        assert_eq!(classify(3500 * MIB, &limits), Admission::Drain);
        assert_eq!(classify(3750 * MIB, &limits), Admission::Reject);
    }

    #[test]
    fn reject_when_override_high() {
        let guard = MemoryGuard::new(MemoryLimits::production());
        guard.set_rss_override(4 * GIB);
        let err = guard.admit(128).unwrap_err();
        assert!(matches!(err, KernelError::Overloaded { .. }));
    }

    #[test]
    fn drain_blocks_large_only() {
        let mut limits = MemoryLimits::production();
        limits.large_request_bytes = 1024;
        let guard = MemoryGuard::new(limits);
        guard.set_rss_override(3 * GIB + 1);
        assert!(guard.admit(512).is_ok());
        assert!(guard.admit(2048).is_err());
    }
}
