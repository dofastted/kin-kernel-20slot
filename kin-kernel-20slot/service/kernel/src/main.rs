mod api;
mod config;
mod error;
mod model;
mod provider;
mod scheduler;
mod session;
mod state;
mod stream;

use std::sync::Arc;

use tokio::{net::TcpListener, time::Duration};
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::{
    config::Config,
    provider::{
        Provider, anthropic::AnthropicProvider, local_cli::LocalCliProvider, mock::MockProvider,
    },
    scheduler::Scheduler,
    session::SessionDirectory,
    state::AppState,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("kin_kernel=info,tower_http=info")),
        )
        .json()
        .init();

    let config = Config::from_env()?;
    let scheduler = Arc::new(Scheduler::new(config.worker_count, config.slots_per_worker));
    let sessions = Arc::new(SessionDirectory::new(
        config.session_ttl,
        config.continuation_ttl,
        config.max_session_bytes,
    ));
    let provider: Arc<dyn Provider> = match config.provider.as_str() {
        "mock" => Arc::new(MockProvider),
        "anthropic_api" => Arc::new(AnthropicProvider::from_env()?),
        "local_cli" => match config.isolation {
            crate::config::IsolationMode::Multiplexed => {
                Arc::new(provider::multiplex_cli::MultiplexCliProvider::from_env()?)
            }
            _ => Arc::new(LocalCliProvider::from_env(config.isolation)?),
        },
        other => return Err(format!("unsupported KIN_PROVIDER: {other}").into()),
    };
    let cleanup_scheduler = Arc::clone(&scheduler);
    let cleanup_sessions = Arc::clone(&sessions);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            for reservation in cleanup_sessions.sweep_expired() {
                cleanup_scheduler
                    .expire_waiting(reservation.worker_index, reservation.worker_generation);
            }
        }
    });
    let state = AppState::new(config.clone(), scheduler, sessions, provider);
    let boot_state = state.clone();
    let app = api::router(state);

    let listener = TcpListener::bind(config.listen_addr).await?;
    info!(
        address = %config.listen_addr,
        workers = config.worker_count,
        slots_per_worker = config.slots_per_worker,
        isolation = %config.isolation,
        provider = %config.provider,
        "kin-kernel listening"
    );
    tokio::spawn(async move {
        boot_state.mark_provider_booting();
        match boot_state.provider.boot().await {
            Ok(()) => {
                boot_state.mark_provider_ready();
                tracing::info!("provider boot complete");
            }
            Err(err) => {
                // A kernel whose CLI/relay never came up must not accept
                // traffic: flip every worker unhealthy so /readyz returns 503
                // and the control plane routes around us.
                tracing::error!(%err, "provider boot failed; marking kernel not ready");
                boot_state.mark_provider_failed();
                boot_state.scheduler.mark_all_unhealthy();
            }
        }
    });

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
