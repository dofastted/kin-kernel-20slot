use std::time::Duration;

use tokio::time::timeout;

use super::Runtime;
use crate::error::KernelError;

pub async fn wait_ready(
    runtime: &Runtime,
    n: usize,
    max_wait: Duration,
) -> Result<(), KernelError> {
    timeout(max_wait, async {
        loop {
            if runtime.ready_slots() >= n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| KernelError::Provider(format!("timed out waiting for {n} ready slots")))
}
