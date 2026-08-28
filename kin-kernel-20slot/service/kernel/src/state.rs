use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};

use crate::{config::Config, provider::Provider, scheduler::Scheduler, session::SessionDirectory};

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderBootStatus {
    Booting = 0,
    Ready = 1,
    Failed = 2,
}

impl ProviderBootStatus {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Ready,
            2 => Self::Failed,
            _ => Self::Booting,
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub scheduler: Arc<Scheduler>,
    pub sessions: Arc<SessionDirectory>,
    pub provider: Arc<dyn Provider>,
    provider_boot: Arc<AtomicU8>,
}

impl AppState {
    pub fn new(
        config: Config,
        scheduler: Arc<Scheduler>,
        sessions: Arc<SessionDirectory>,
        provider: Arc<dyn Provider>,
    ) -> Self {
        Self {
            config,
            scheduler,
            sessions,
            provider,
            provider_boot: Arc::new(AtomicU8::new(ProviderBootStatus::Booting as u8)),
        }
    }

    pub fn provider_boot_status(&self) -> ProviderBootStatus {
        ProviderBootStatus::from_u8(self.provider_boot.load(Ordering::Acquire))
    }

    pub fn mark_provider_booting(&self) {
        self.provider_boot
            .store(ProviderBootStatus::Booting as u8, Ordering::Release);
    }

    pub fn mark_provider_ready(&self) {
        self.provider_boot
            .store(ProviderBootStatus::Ready as u8, Ordering::Release);
    }

    pub fn mark_provider_failed(&self) {
        self.provider_boot
            .store(ProviderBootStatus::Failed as u8, Ordering::Release);
    }
}
