use std::path::Path;

use serde::Serialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

const SCHEMA_VERSION: &str = "1";

#[derive(Debug, Serialize)]
pub struct GuestIdentity {
    pub schema_version: &'static str,
    pub runtime_kind: String,
    pub hostname: String,
    pub os_id: String,
    pub os_pretty: String,
    pub kernel_release: String,
    pub arch: &'static str,
    pub goos: &'static str,
    pub machine_id: String,
    pub timezone: String,
    pub locale: String,
    pub collected_at: String,
    pub worker_version: &'static str,
}

pub fn collect(runtime_kind: &str) -> GuestIdentity {
    let (os_id, os_pretty) = read_os_release();
    GuestIdentity {
        schema_version: SCHEMA_VERSION,
        runtime_kind: value_or(runtime_kind, "docker"),
        hostname: read_first(&["/etc/hostname"]),
        os_id,
        os_pretty,
        kernel_release: read_first(&["/proc/sys/kernel/osrelease"]),
        arch: std::env::consts::ARCH,
        goos: std::env::consts::OS,
        machine_id: read_first(&["/etc/machine-id", "/var/lib/dbus/machine-id"]),
        timezone: env_or("TZ", "UTC"),
        locale: std::env::var("LC_ALL")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| env_or("LANG", "en_US.UTF-8")),
        collected_at: OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .unwrap_or_default(),
        worker_version: env!("CARGO_PKG_VERSION"),
    }
}

fn read_os_release() -> (String, String) {
    let raw = read_first(&["/etc/os-release", "/usr/lib/os-release"]);
    parse_os_release(&raw)
}

fn parse_os_release(raw: &str) -> (String, String) {
    let mut id = String::new();
    let mut pretty = String::new();
    for line in raw.lines().map(str::trim) {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let value = value.trim_matches(['"', '\'']);
        match key {
            "ID" => id = value.to_string(),
            "PRETTY_NAME" => pretty = value.to_string(),
            _ => {}
        }
    }
    (id, pretty)
}

fn read_first(paths: &[&str]) -> String {
    paths
        .iter()
        .find_map(|path| std::fs::read_to_string(Path::new(path)).ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_default()
}

fn env_or(key: &str, fallback: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

fn value_or(value: &str, fallback: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        fallback.to_string()
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_os_release() {
        let (id, pretty) = parse_os_release("ID=debian\nPRETTY_NAME=\"Debian GNU/Linux\"\n");
        assert_eq!(id, "debian");
        assert_eq!(pretty, "Debian GNU/Linux");
    }

    #[test]
    fn collected_contract_has_runtime_and_version() {
        let identity = collect("docker");
        assert_eq!(identity.schema_version, "1");
        assert_eq!(identity.runtime_kind, "docker");
        assert!(!identity.worker_version.is_empty());
    }
}
