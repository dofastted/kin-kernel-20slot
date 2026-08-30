mod config;
mod credential;
mod error;
mod hop;
mod server;
mod sse;

use std::path::PathBuf;

pub use server::run;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LaunchMode {
    PublicHttp,
    GatewayWorker { config: PathBuf },
}

pub fn parse_args(args: &[String]) -> Result<LaunchMode, String> {
    let mut worker = false;
    let mut config = None;
    let mut iter = args.iter().skip(1);
    while let Some(arg) = iter.next() {
        if arg == "--gateway-worker" {
            worker = true;
            continue;
        }
        if let Some(path) = arg.strip_prefix("--config=") {
            config = Some(PathBuf::from(path));
            continue;
        }
        if arg == "--config" {
            let path = iter.next().ok_or("--config requires a path")?;
            config = Some(PathBuf::from(path));
            continue;
        }
        return Err(format!("unknown argument: {arg}"));
    }
    if worker {
        let config = config.ok_or_else(|| "--gateway-worker requires --config".to_string())?;
        return Ok(LaunchMode::GatewayWorker { config });
    }
    if config.is_some() {
        return Err("--config is only valid with --gateway-worker".into());
    }
    Ok(LaunchMode::PublicHttp)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(raw: &[&str]) -> Vec<String> {
        raw.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn default_is_public_http() {
        assert_eq!(
            parse_args(&args(&["kin-kernel"])).unwrap(),
            LaunchMode::PublicHttp
        );
    }

    #[test]
    fn gateway_worker_requires_config() {
        let err = parse_args(&args(&["kin-kernel", "--gateway-worker"])).unwrap_err();
        assert!(err.contains("--config"));
    }

    #[test]
    fn gateway_worker_reads_config_path() {
        let mode = parse_args(&args(&[
            "kin-kernel",
            "--gateway-worker",
            "--config",
            "/tmp/worker.json",
        ]))
        .unwrap();
        assert_eq!(
            mode,
            LaunchMode::GatewayWorker {
                config: PathBuf::from("/tmp/worker.json")
            }
        );
    }
}
