use std::sync::Arc;

use crate::{config::Config, provider::Provider, scheduler::Scheduler, session::SessionDirectory};

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub scheduler: Arc<Scheduler>,
    pub sessions: Arc<SessionDirectory>,
    pub provider: Arc<dyn Provider>,
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
        }
    }
}
