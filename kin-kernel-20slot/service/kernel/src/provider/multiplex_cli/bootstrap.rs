use std::time::Duration;

use serde_json::json;
use tokio::io::AsyncWriteExt;
use tokio::process::ChildStdin;
use tokio::time::timeout;

use super::{Runtime, supervisor::bootstrap_prompt};
use crate::error::KernelError;

pub async fn write_root_prompt(stdin: &mut ChildStdin, n: usize) -> Result<(), KernelError> {
    let frame = json!({
        "type": "user",
        "message": {
            "role": "user",
            "content": [{ "type": "text", "text": bootstrap_prompt(n) }]
        }
    });
    let mut line = serde_json::to_vec(&frame).unwrap();
    line.push(b'\n');
    stdin
        .write_all(&line)
        .await
        .map_err(|err| KernelError::Provider(format!("bootstrap stdin: {err}")))?;
    stdin
        .flush()
        .await
        .map_err(|err| KernelError::Provider(err.to_string()))?;
    Ok(())
}

pub async fn wait_ready(runtime: &Runtime, n: usize, max_wait: Duration) -> Result<(), KernelError> {
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
