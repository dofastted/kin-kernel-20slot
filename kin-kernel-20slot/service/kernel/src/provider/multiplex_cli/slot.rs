use std::time::{Duration, Instant};

use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SlotPhase {
    Booting,
    /// Idle and registered with the scheduler, waiting for a job.
    ReadyBlocked,
    Running,
    Dead,
}

#[derive(Debug)]
pub struct Slot {
    pub id: String,
    pub phase: SlotPhase,
    pub tenant_id: Option<String>,
    pub session_id: Option<String>,
    pub job_id: Option<String>,
    pub jobs_completed: u32,
    pub created_at: Instant,
    pub last_change: Instant,
}

impl Slot {
    pub fn new(id: impl Into<String>) -> Self {
        let now = Instant::now();
        Self {
            id: id.into(),
            phase: SlotPhase::Booting,
            tenant_id: None,
            session_id: None,
            job_id: None,
            jobs_completed: 0,
            created_at: now,
            last_change: now,
        }
    }

    pub fn bind_job(&mut self, tenant: &str, session: &str, job_id: &str) -> bool {
        if self.phase != SlotPhase::ReadyBlocked {
            return false;
        }
        if let Some(existing) = self.tenant_id.as_deref()
            && existing != tenant
        {
            return false;
        }
        self.tenant_id = Some(tenant.to_string());
        self.session_id = Some(session.to_string());
        self.job_id = Some(job_id.to_string());
        self.phase = SlotPhase::Running;
        self.last_change = Instant::now();
        true
    }

    /// Keep tenant+session sticky. Clearing them would let another tenant
    /// inherit leftover subagent context.
    pub fn unbind_ready(&mut self) {
        self.job_id = None;
        self.phase = SlotPhase::ReadyBlocked;
        self.last_change = Instant::now();
    }

    pub fn should_retire(&self, max_jobs: u32, max_lifetime: Duration, idle: Duration) -> bool {
        if self.jobs_completed >= max_jobs {
            return true;
        }
        if self.created_at.elapsed() >= max_lifetime {
            return true;
        }
        if self.phase == SlotPhase::ReadyBlocked
            && self.tenant_id.is_some()
            && self.last_change.elapsed() >= idle
        {
            return true;
        }
        false
    }

    pub fn retire(&mut self) {
        self.phase = SlotPhase::Dead;
        self.job_id = None;
        self.last_change = Instant::now();
    }
}

#[cfg(test)]
#[derive(Clone, Debug, Serialize)]
pub struct SlotSnapshot {
    pub id: String,
    pub phase: SlotPhase,
    pub session_id: Option<String>,
    pub tenant_id: Option<String>,
    pub jobs_completed: u32,
}
