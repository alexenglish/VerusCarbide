use super::{Endpoint, Settings, Topic};
use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, SocketAddr, TcpListener};
use std::path::{Path, PathBuf};

pub(crate) fn local_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .trim_matches(['[', ']'])
            .parse::<IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

fn endpoint(value: &str) -> Option<SocketAddr> {
    let address = value.strip_prefix("tcp://")?;
    let parsed = address.strip_prefix("localhost:").map_or_else(
        || address.parse::<SocketAddr>().ok(),
        |port| {
            port.parse()
                .ok()
                .map(|port| SocketAddr::from(([127, 0, 0, 1], port)))
        },
    )?;
    (parsed.ip().is_loopback() && parsed.port() != 0).then_some(parsed)
}

fn values(path: &Path) -> (BTreeMap<Topic, SocketAddr>, Vec<String>) {
    let mut endpoints = BTreeMap::new();
    let mut errors = Vec::new();
    match crate::config::read_key_value_file(path) {
        Ok(values) => {
            for topic in Topic::ALL {
                let name = topic.setting();
                if let Some(value) = values.get(&name) {
                    // Verus uses Boost's config parser, including trailing comments.
                    let value = value.split('#').next().unwrap_or_default().trim();
                    if let Some(address) = endpoint(value) {
                        endpoints.insert(topic, address);
                    } else {
                        errors.push(format!("{name} requires a TCP loopback address and port."));
                    }
                }
            }
        }
        Err(error)
            if !error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
        {
            errors.push(format!("Could not read daemon config: {error}"));
        }
        Err(_) => {}
    }
    (endpoints, errors)
}

/// Include both networks without enabling sources or probing their RPC ports.
pub(crate) fn known_paths(
    configs: &[crate::config::Config],
    saved_path: &Path,
) -> BTreeSet<PathBuf> {
    let mut paths: BTreeSet<_> = configs
        .iter()
        .filter_map(|config| config.daemon.config_file.clone())
        .collect();
    if let Ok(text) = std::fs::read_to_string(saved_path)
        && let Ok(saved) = toml::from_str::<crate::sources::Settings>(&text)
    {
        paths.extend(saved.connections.into_iter().map(|entry| entry.config_file));
    }
    if let Some(dirs) = directories::BaseDirs::new() {
        let home = dirs.home_dir();
        for (network, directory) in [
            (crate::world::Network::Mainnet, ".verus"),
            (crate::world::Network::Testnet, ".verustest"),
        ] {
            if let Ok(path) = crate::config::standard_config_path(home, "VRSC", network) {
                paths.insert(path);
            }
            if let Ok(entries) = std::fs::read_dir(home.join(directory).join("pbaas")) {
                for entry in entries.flatten() {
                    let name = entry.file_name();
                    let Some(name) = name.to_str() else { continue };
                    if name.len() == 40 && name.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                        paths.insert(entry.path().join(format!("{name}.conf")));
                    }
                }
            }
        }
    }
    paths
}

pub(crate) struct Survey {
    by_file: BTreeMap<PathBuf, (BTreeMap<Topic, SocketAddr>, Vec<String>)>,
    claims: BTreeMap<SocketAddr, BTreeSet<PathBuf>>,
}

impl Survey {
    pub(crate) fn read(paths: BTreeSet<PathBuf>) -> Self {
        let mut by_file = BTreeMap::new();
        let mut claims = BTreeMap::<_, BTreeSet<_>>::new();
        for path in paths {
            let path = path.canonicalize().unwrap_or(path);
            if by_file.contains_key(&path) {
                continue;
            }
            let (configured, errors) = values(&path);
            for address in configured.values() {
                claims.entry(*address).or_default().insert(path.clone());
            }
            by_file.insert(path, (configured, errors));
        }
        Self { by_file, claims }
    }

    fn file(&self, path: &Path) -> (BTreeMap<Topic, SocketAddr>, Vec<String>) {
        let path = path.canonicalize().unwrap_or_else(|_| path.to_owned());
        self.by_file.get(&path).cloned().unwrap_or_default()
    }

    pub(crate) fn settings(&self, path: &Path) -> Settings {
        let (configured, mut errors) = self.file(path);
        let current = path.canonicalize().unwrap_or_else(|_| path.to_owned());
        let mut allowed = BTreeMap::<_, Vec<_>>::new();
        for (topic, address) in &configured {
            if let Some(other) = self
                .claims
                .get(address)
                .and_then(|claims| claims.iter().find(|other| **other != current))
            {
                let message = format!(
                    "Port {} also configured in {}",
                    address.port(),
                    other.display()
                );
                if !errors.contains(&message) {
                    errors.push(message);
                }
            } else {
                allowed.entry(*address).or_default().push(*topic);
            }
        }
        Settings {
            configured: configured
                .into_iter()
                .map(|(topic, address)| (topic, format!("tcp://{address}")))
                .collect(),
            endpoints: allowed
                .into_iter()
                .map(|(address, topics)| Endpoint {
                    url: format!("tcp://{address}"),
                    topics,
                })
                .collect(),
            errors,
        }
    }

    pub(crate) fn suggestion(&self, path: &Path, input: &str) -> Result<String, String> {
        let settings = self.settings(path);
        if !settings.errors.is_empty() {
            return Err("Resolve the config warnings, then Recheck.".into());
        }
        let missing: Vec<_> = Topic::RECOMMENDED
            .into_iter()
            .filter(|topic| !settings.configured.contains_key(topic))
            .collect();
        if missing.is_empty() {
            return Ok(String::new());
        }
        let occupied = |port| {
            self.claims.keys().any(|address| address.port() == port)
                || TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], port))).is_err()
        };
        let address = if !input.trim().is_empty() {
            let port = input
                .trim()
                .parse::<u16>()
                .ok()
                .filter(|&port| port != 0)
                .ok_or("Enter a port from 1 through 65535.")?;
            let address = format!("tcp://127.0.0.1:{port}");
            if !settings.configured.values().any(|value| value == &address) && occupied(port) {
                return Err(format!("Port {port} is already configured or in use."));
            }
            address
        } else if let Some(value) = Topic::RECOMMENDED
            .iter()
            .find_map(|topic| settings.configured.get(topic))
            .or_else(|| settings.configured.values().next())
        {
            value.clone()
        } else {
            let port = (28340..=28439)
                .find(|&port| !occupied(port))
                .ok_or("No available port in 28340–28439. Enter a different port.")?;
            format!("tcp://127.0.0.1:{port}")
        };
        Ok(missing
            .into_iter()
            .map(|topic| format!("{}={address}", topic.setting()))
            .collect::<Vec<_>>()
            .join("\n"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claims_are_per_daemon_and_preserve_split_endpoints() {
        let dir = std::env::temp_dir().join(format!(
            "carbide-zmq-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir(&dir).unwrap();
        let a = dir.join("main.conf");
        let b = dir.join("test.conf");
        std::fs::write(&a, "zmqpubrawblock=tcp://127.0.0.1:28340\nzmqpubhashblock=tcp://127.0.0.1:28340\nzmqpubrawtx=tcp://127.0.0.1:28341\n").unwrap();
        std::fs::write(&b, "").unwrap();
        let keys = BTreeSet::from([a.clone(), b.clone()]);
        let survey = Survey::read(keys.clone());
        let first = survey.settings(&a);
        assert_eq!(first.configured.len(), 3);
        assert_eq!(first.endpoints.len(), 2);
        assert!(survey.suggestion(&a, "").unwrap().is_empty());
        assert!(survey.suggestion(&b, "28340").is_err());
        assert!(survey.suggestion(&b, "70000").is_err());
        std::fs::write(&b, "zmqpubhashtx=tcp://localhost:28340\n").unwrap();
        let survey = Survey::read(keys);
        let first = survey.settings(&a);
        assert_eq!(first.endpoints.len(), 1);
        assert!(first.errors[0].contains("test.conf"));
        assert!(survey.settings(&b).endpoints.is_empty());
        std::fs::write(&a, "zmqpubhashblock=tcp://127.0.0.1:28341\n").unwrap();
        let survey = Survey::read(BTreeSet::from([a.clone(), b]));
        assert_eq!(
            survey.suggestion(&a, "").unwrap(),
            "zmqpubrawblock=tcp://127.0.0.1:28341\nzmqpubrawtx=tcp://127.0.0.1:28341"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
}
