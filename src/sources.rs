//! Connection permissions and discovery. Discovery reads only known config files.
use crate::{
    config::{Config, DaemonConfig},
    world::Network,
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[repr(u8)]
pub enum ConnectionState {
    #[default]
    Disabled,
    Enabled,
    Monitored,
}

/// Session observations of actual endpoint traffic, independent of permission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommunicationOutcome {
    Responding,
    Failed,
}

#[derive(Debug, Clone, Default)]
pub struct Communication {
    pub in_flight: std::collections::BTreeSet<u64>,
    pub last_success: Option<std::time::Instant>,
    pub last_contact: Option<std::time::Instant>,
    pub outcome: Option<CommunicationOutcome>,
    order: u64,
}

#[derive(Debug, Clone)]
pub enum ChainCheck {
    Checking,
    Finished {
        at: std::time::Instant,
        error: Option<String>,
    },
}

pub struct RequestActivity {
    control: Arc<Control>,
    generation: u64,
    order: u64,
    outcome: CommunicationOutcome,
    success: bool,
}
impl RequestActivity {
    pub fn responding(&mut self, success: bool) {
        self.outcome = CommunicationOutcome::Responding;
        self.success = success;
    }
}
impl Drop for RequestActivity {
    fn drop(&mut self) {
        let mut status = self
            .control
            .communication
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        status.in_flight.remove(&self.order);
        if self.control.generation() != self.generation {
            return;
        }
        let now = std::time::Instant::now();
        if self.success {
            status.last_success = Some(now);
        }
        if self.outcome == CommunicationOutcome::Responding {
            status.last_contact = Some(now);
        }
        if self.order >= status.order {
            status.order = self.order;
            status.outcome = Some(self.outcome);
        }
    }
}

#[derive(Debug)]
pub struct Control {
    communication: std::sync::Mutex<Communication>,
    chain_check: std::sync::Mutex<Option<(u64, ChainCheck)>>,
    pub branch_epoch: AtomicU64,
    pub zmq: crate::zmq::Hub,
    // 0 unknown, 1 available, 2 unavailable; shared by clients for this source.
    identity_index: AtomicU64,
    unsupported_methods: std::sync::Mutex<std::collections::HashSet<String>>,
    pub reconciliation_heights: std::sync::Mutex<std::collections::BTreeSet<u64>>,
    // State and generation change atomically so revocation cannot issue a fresh ticket.
    authorization: AtomicU64,
    pub visible: std::sync::atomic::AtomicBool,
    pub expected: std::sync::OnceLock<(crate::world::SystemId, Network)>,
    pub permits: tokio::sync::Semaphore,
    pub verified: tokio::sync::Mutex<bool>,
}
impl Default for Control {
    fn default() -> Self {
        Self {
            communication: Default::default(),
            chain_check: Default::default(),
            branch_epoch: AtomicU64::new(0),
            zmq: Default::default(),
            identity_index: AtomicU64::new(0),
            unsupported_methods: Default::default(),
            reconciliation_heights: Default::default(),
            authorization: AtomicU64::new(1),
            visible: false.into(),
            expected: Default::default(),
            verified: Default::default(),
            permits: tokio::sync::Semaphore::new(2),
        }
    }
}
impl Control {
    pub fn record_chain_check(&self, generation: u64, check: ChainCheck) {
        let mut status = self.chain_check.lock().unwrap_or_else(|e| e.into_inner());
        if self.accepts(generation) {
            *status = Some((generation, check));
        }
    }

    pub fn chain_check(&self) -> Option<ChainCheck> {
        self.chain_check
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .filter(|(generation, _)| self.accepts(*generation))
            .map(|(_, check)| check.clone())
    }

    pub fn communication(&self) -> Communication {
        self.communication
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    pub fn begin_request(self: &Arc<Self>, generation: u64, order: u64) -> RequestActivity {
        self.communication
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .in_flight
            .insert(order);
        RequestActivity {
            control: self.clone(),
            generation,
            order,
            outcome: CommunicationOutcome::Failed,
            success: false,
        }
    }

    pub fn method_available(&self, method: &str) -> bool {
        !self
            .unsupported_methods
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(method)
    }
    pub fn unsupported_method(&self, method: &str) {
        self.unsupported_methods
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(method.into());
    }

    pub fn identity_index_unknown(&self) -> bool {
        self.identity_index.load(Ordering::Acquire) == 0
    }
    pub fn identity_index_enabled(&self) -> bool {
        self.identity_index.load(Ordering::Acquire) == 1
    }
    pub fn set_identity_index(&self, enabled: bool) {
        self.identity_index
            .store(if enabled { 1 } else { 2 }, Ordering::Release);
    }

    pub fn state(&self) -> ConnectionState {
        match self.authorization.load(Ordering::Acquire) & 3 {
            0 => ConnectionState::Disabled,
            2 => ConnectionState::Monitored,
            _ => ConnectionState::Enabled,
        }
    }
    pub fn set(&self, state: ConnectionState) {
        let _ = self
            .authorization
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| {
                Some(((old & !3).wrapping_add(4)) | state as u64)
            });
    }
    pub fn ticket(&self) -> Result<u64> {
        let generation = self.authorization.load(Ordering::Acquire);
        ensure!(
            generation & 3 != 0,
            "connection disabled; enable it in Sources (F2)"
        );
        Ok(generation)
    }
    pub fn generation(&self) -> u64 {
        self.authorization.load(Ordering::Acquire)
    }
    pub fn accepts(&self, generation: u64) -> bool {
        self.authorization.load(Ordering::Acquire) == generation
            && self.state() != ConnectionState::Disabled
    }
    pub fn polling(&self) -> bool {
        self.state() == ConnectionState::Monitored
            || (self.state() == ConnectionState::Enabled && self.visible.load(Ordering::Acquire))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SavedConnection {
    pub network: Network,
    pub chain: String,
    pub config_file: PathBuf,
    #[serde(default)]
    pub state: ConnectionState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SavedApi {
    pub network: Network,
    pub chain: String,
    pub endpoint: String,
    #[serde(default)]
    pub enabled: bool,
}

#[derive(Debug)]
pub struct ApiEndpoint {
    pub url: String,
    pub control: Arc<Control>,
    pub retry_after: std::sync::Mutex<Option<std::time::Instant>>,
}
#[derive(Debug)]
pub struct ApiConsent {
    pub local_failure: String,
    pub sources: Vec<String>,
    pub method: String,
    pub chain: String,
    pub reply: tokio::sync::oneshot::Sender<ApiDecision>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiDecision {
    Retry,
    Enable(usize),
    Cancel,
}

#[derive(Debug)]
pub struct ApiCancelled;

impl std::fmt::Display for ApiCancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("API action cancelled")
    }
}

impl std::error::Error for ApiCancelled {}

#[derive(Debug, Default)]
pub struct ApiPool {
    pub endpoints: Vec<ApiEndpoint>,
    pub consent: Option<tokio::sync::mpsc::UnboundedSender<ApiConsent>>,
    pub chain: String,
    cursor: std::sync::atomic::AtomicUsize,
}
impl ApiPool {
    pub fn new(
        chain: String,
        endpoints: Vec<ApiEndpoint>,
        consent: Option<tokio::sync::mpsc::UnboundedSender<ApiConsent>>,
    ) -> Self {
        Self {
            chain,
            endpoints,
            consent,
            cursor: Default::default(),
        }
    }

    pub fn candidates(&self) -> Vec<&ApiEndpoint> {
        let len = self.endpoints.len();
        if len == 0 {
            return vec![];
        }
        let start = self.cursor.fetch_add(1, Ordering::Relaxed) % len;
        let mut candidates: Vec<_> = (0..len)
            .map(|i| &self.endpoints[(start + i) % len])
            .filter(|endpoint| {
                endpoint
                    .retry_after
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .is_none_or(|until| until <= std::time::Instant::now())
            })
            .collect();
        candidates.sort_by_key(|endpoint| endpoint.control.state() == ConnectionState::Disabled);
        candidates
    }
    pub async fn permission(
        &self,
        sources: &[&ApiEndpoint],
        method: &str,
        local_failure: &str,
    ) -> Result<ApiDecision> {
        let sender = self
            .consent
            .as_ref()
            .context("API disabled; interactive consent is unavailable")?;
        let (reply, response) = tokio::sync::oneshot::channel();
        sender
            .send(ApiConsent {
                local_failure: local_failure.into(),
                sources: sources
                    .iter()
                    .map(|endpoint| {
                        reqwest::Url::parse(&endpoint.url)
                            .map(|url| url.origin().ascii_serialization())
                            .unwrap_or_else(|_| "Configured API".into())
                    })
                    .collect(),
                method: method.into(),
                chain: self.chain.clone(),
                reply,
            })
            .map_err(|_| anyhow::anyhow!("API consent interface closed"))?;
        response.await.map_err(|_| ApiCancelled.into())
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    #[serde(default)]
    pub bookmarks: crate::bookmarks::Options,
    #[serde(default)]
    pub connections: Vec<SavedConnection>,
    #[serde(default)]
    pub apis: Vec<SavedApi>,
}

pub struct Registry {
    pub configs: Vec<Config>,
    pub settings_path: PathBuf,
    saved: Settings,
    pub consent_rx: tokio::sync::mpsc::UnboundedReceiver<ApiConsent>,
}

impl Registry {
    pub fn bookmark_options(&self) -> crate::bookmarks::Options {
        self.saved.bookmarks.clone()
    }
    pub fn load(invocation: Config) -> Result<Self> {
        let dirs = directories::BaseDirs::new().context("home directory unavailable")?;
        let settings_path = dirs.config_dir().join("verus-explorer-tui/config.toml");
        let saved: Settings = if settings_path.exists() {
            toml::from_str(&std::fs::read_to_string(&settings_path)?)
                .context("invalid explorer config.toml")?
        } else {
            Settings::default()
        };
        let network = Network::from_testnet(invocation.chain.testnet);
        let mut configs = vec![invocation.clone()];
        for entry in saved
            .connections
            .iter()
            .filter(|entry| entry.network == network)
        {
            if invocation.daemon.config_file.as_ref() == Some(&entry.config_file) {
                // Invocation enables only this session; preserve monitored when saved.
                invocation
                    .daemon
                    .control
                    .set(if entry.state == ConnectionState::Monitored {
                        entry.state
                    } else {
                        ConnectionState::Enabled
                    });
                continue;
            }
            configs.push(config_from_file(
                &invocation,
                &entry.chain,
                &entry.config_file,
                entry.state,
            )?);
        }
        let primary = crate::config::standard_config_path(dirs.home_dir(), "VRSC", network)?;
        let mut candidates = vec![(
            if invocation.chain.testnet {
                "vrsctest".into()
            } else {
                "VRSC".into()
            },
            primary,
        )];
        let root = dirs
            .home_dir()
            .join(if invocation.chain.testnet {
                ".verustest"
            } else {
                ".verus"
            })
            .join("pbaas");
        match std::fs::read_dir(root) {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry?;
                    let name = entry.file_name().to_string_lossy().into_owned();
                    if entry.file_type()?.is_dir()
                        && name.len() == 40
                        && name.bytes().all(|b| b.is_ascii_hexdigit())
                    {
                        candidates.push((name.clone(), entry.path().join(format!("{name}.conf"))));
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        for (name, path) in candidates {
            if !path.is_file()
                || configs
                    .iter()
                    .any(|c| c.daemon.config_file.as_ref() == Some(&path))
            {
                continue;
            }
            let config = config_from_file(&invocation, &name, &path, ConnectionState::Disabled)?;
            if config.daemon.user.is_some() && config.daemon.password.is_some() {
                configs.push(config);
            }
        }
        let mut seen = std::collections::HashSet::new();
        configs.retain(|config| seen.insert(config.chain_cache_key()));
        let (consent_tx, consent_rx) = tokio::sync::mpsc::unbounded_channel();
        for api in saved.apis.iter().filter(|api| api.network == network) {
            if !configs.iter().any(|config| {
                crate::world::system_id(&api.chain, network).ok().as_ref()
                    == config.chain.system_id.get()
            }) {
                let mut config = invocation.clone();
                config.chain.name = api.chain.clone();
                config.chain.system_id =
                    Arc::new(std::sync::OnceLock::from(crate::world::system_id(
                        &config.chain.name,
                        Network::from_testnet(config.chain.testnet),
                    )?));
                config.daemon.control = Arc::default();
                config.daemon.control.set(ConnectionState::Disabled);
                config.daemon.config_file = None;
                config.daemon.user = None;
                config.daemon.password = None;
                config.daemon.port = 0;
                configs.push(config);
            }
        }
        for config in &mut configs {
            if let Some(id) = config.chain.system_id.get() {
                let _ = config.daemon.control.expected.set((id.clone(), network));
            }
            let apis = saved.apis.iter().filter(|api| {
                api.network == network
                    && crate::world::system_id(&api.chain, network).ok().as_ref()
                        == config.chain.system_id.get()
            });
            let mut endpoints = Vec::new();
            for api in apis {
                let parsed = reqwest::Url::parse(&api.endpoint).context("invalid API URL")?;
                ensure!(
                    matches!(parsed.scheme(), "http" | "https")
                        && parsed.username().is_empty()
                        && parsed.password().is_none(),
                    "API endpoints require HTTP(S) and no embedded credentials"
                );
                let control = Arc::new(Control::default());
                if let Some(id) = config.chain.system_id.get() {
                    let _ = control.expected.set((id.clone(), network));
                }
                control.set(if api.enabled {
                    ConnectionState::Enabled
                } else {
                    ConnectionState::Disabled
                });
                endpoints.push(ApiEndpoint {
                    url: api.endpoint.clone(),
                    control,
                    retry_after: Default::default(),
                });
            }
            config.daemon.api_pool = Arc::new(ApiPool::new(
                config.chain.name.clone(),
                endpoints,
                Some(consent_tx.clone()),
            ));
        }
        Ok(Self {
            configs,
            settings_path,
            saved,
            consent_rx,
        })
    }

    pub fn rows(&self) -> Vec<(usize, Option<usize>)> {
        self.configs
            .iter()
            .enumerate()
            .flat_map(|(chain, config)| {
                std::iter::once((chain, None)).chain(
                    (0..config.daemon.api_pool.endpoints.len()).map(move |api| (chain, Some(api))),
                )
            })
            .collect()
    }

    pub fn save(&mut self) -> Result<()> {
        let network = Network::from_testnet(self.configs[0].chain.testnet);
        self.saved.connections.retain(|c| c.network != network);
        self.saved
            .connections
            .extend(self.configs.iter().filter_map(|config| {
                Some(SavedConnection {
                    network,
                    chain: config.chain.name.clone(),
                    config_file: config.daemon.config_file.clone()?,
                    state: config.daemon.control.state(),
                })
            }));
        self.saved.apis.retain(|api| api.network != network);
        for config in &self.configs {
            for endpoint in &config.daemon.api_pool.endpoints {
                self.saved.apis.push(SavedApi {
                    network,
                    chain: config.chain.name.clone(),
                    endpoint: endpoint.url.clone(),
                    enabled: endpoint.control.state() != ConnectionState::Disabled,
                });
            }
        }
        let parent = self
            .settings_path
            .parent()
            .context("settings path has no parent")?;
        std::fs::create_dir_all(parent)?;
        let temporary = self.settings_path.with_extension("toml.tmp");
        std::fs::write(&temporary, toml::to_string_pretty(&self.saved)?)?;
        std::fs::rename(temporary, &self.settings_path)?;
        Ok(())
    }
}

fn config_from_file(
    base: &Config,
    name: &str,
    path: &Path,
    state: ConnectionState,
) -> Result<Config> {
    let values = if path.exists() {
        crate::config::read_key_value_file(path)?
    } else {
        Default::default()
    };
    let mut config = base.clone();
    config.chain.name = name.into();
    config.chain.system_id = Arc::new(std::sync::OnceLock::from(crate::world::system_id(
        &config.chain.name,
        Network::from_testnet(config.chain.testnet),
    )?));
    config.daemon = DaemonConfig {
        host: values
            .get("rpcconnect")
            .cloned()
            .unwrap_or_else(|| "127.0.0.1".into()),
        port: values
            .get("rpcport")
            .map(|port| port.parse::<u16>())
            .transpose()
            .context("invalid rpcport")?
            .unwrap_or_else(|| {
                if name.eq_ignore_ascii_case("vrsc") || name.eq_ignore_ascii_case("vrsctest") {
                    if base.chain.testnet { 27484 } else { 27486 }
                } else {
                    0
                }
            }),
        user: values.get("rpcuser").cloned(),
        password: values.get("rpcpassword").cloned(),
        config_file: Some(path.into()),
        observations: None,
        control: Arc::default(),
        api_pool: Arc::default(),
        background: false,
    };
    if let Some(value) = values.get("idindex") {
        config
            .daemon
            .control
            .set_identity_index(value == "1" || value.eq_ignore_ascii_case("true"));
    }
    config.daemon.control.set(state);
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn communication_tracks_real_requests_and_rejects_obsolete_results() {
        let control = Arc::new(Control::default());
        assert!(control.communication().outcome.is_none());
        let mut success = control.begin_request(control.generation(), 2);
        assert_eq!(control.communication().in_flight.len(), 1);
        success.responding(true);
        drop(success);
        assert!(control.communication().last_success.is_some());
        assert!(control.communication().in_flight.is_empty());
        let late_failure = control.begin_request(control.generation(), 1);
        drop(late_failure);
        assert_eq!(
            control.communication().outcome,
            Some(CommunicationOutcome::Responding)
        );
        drop(control.begin_request(control.generation(), 3));
        assert_eq!(
            control.communication().outcome,
            Some(CommunicationOutcome::Failed)
        );
        assert!(control.communication().last_success.is_some());
        let mut obsolete = control.begin_request(control.generation(), 4);
        control.set(ConnectionState::Disabled);
        obsolete.responding(true);
        drop(obsolete);
        assert_eq!(
            control.communication().outcome,
            Some(CommunicationOutcome::Failed)
        );
        assert!(control.communication().in_flight.is_empty());
    }

    #[test]
    fn disable_and_reenable_cannot_accept_old_results() {
        let control = Control::default();
        let ticket = control.ticket().unwrap();
        control.set(ConnectionState::Disabled);
        assert!(control.ticket().is_err());
        control.set(ConnectionState::Enabled);
        assert!(!control.accepts(ticket));
        assert!(control.accepts(control.ticket().unwrap()));
        assert!(!control.polling());
        control.set(ConnectionState::Monitored);
        assert!(control.polling());
    }

    #[test]
    fn chain_checks_clear_failures_and_reject_previous_connection_generations() {
        let control = Control::default();
        let generation = control.generation();
        control.record_chain_check(
            generation,
            ChainCheck::Finished {
                at: std::time::Instant::now(),
                error: Some("missing header".into()),
            },
        );
        assert!(matches!(
            control.chain_check(),
            Some(ChainCheck::Finished { error: Some(_), .. })
        ));
        control.record_chain_check(
            generation,
            ChainCheck::Finished {
                at: std::time::Instant::now(),
                error: None,
            },
        );
        assert!(matches!(
            control.chain_check(),
            Some(ChainCheck::Finished { error: None, .. })
        ));
        control.set(ConnectionState::Disabled);
        control.set(ConnectionState::Enabled);
        control.record_chain_check(generation, ChainCheck::Checking);
        assert!(control.chain_check().is_none());
    }
}
