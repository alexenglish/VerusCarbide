pub(crate) mod setup;
mod transport;
mod wire;

use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Topic {
    HashBlock,
    RawBlock,
    HashTx,
    RawTx,
    CheckedBlock,
}

impl Topic {
    pub(crate) const RECOMMENDED: [Self; 2] = [Self::RawBlock, Self::RawTx];
    pub(crate) const ALL: [Self; 5] = [
        Self::HashBlock,
        Self::RawBlock,
        Self::HashTx,
        Self::RawTx,
        Self::CheckedBlock,
    ];

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::HashBlock => "hashblock",
            Self::RawBlock => "rawblock",
            Self::HashTx => "hashtx",
            Self::RawTx => "rawtx",
            Self::CheckedBlock => "checkedblock",
        }
    }
    pub(crate) fn setting(self) -> String {
        format!("zmqpub{}", self.name())
    }
    fn from_frame(frame: &[u8]) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|topic| topic.name().as_bytes() == frame)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Endpoint {
    url: String,
    topics: Vec<Topic>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct Settings {
    pub configured: BTreeMap<Topic, String>,
    endpoints: Vec<Endpoint>,
    pub errors: Vec<String>,
}

impl Settings {
    pub(crate) fn active(&self, url: &str) -> bool {
        self.endpoints.iter().any(|endpoint| endpoint.url == url)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
enum Status {
    #[default]
    Connecting,
    Connected,
    Unreachable,
    Error(String),
}

#[derive(Debug)]
pub(crate) enum FeedStatus {
    Connecting,
    Connected,
    Receiving(Instant),
    Unreachable,
    Error(String),
    Conflict,
}

impl FeedStatus {
    pub(crate) fn label(&self) -> String {
        match self {
            Self::Connecting => "Waiting for connection".into(),
            Self::Connected => "Connected · waiting for events".into(),
            Self::Receiving(last) => format!(
                "Receiving notifications · last {}s ago",
                last.elapsed().as_secs()
            ),
            Self::Unreachable => "Unreachable · retrying".into(),
            Self::Error(error) => error.clone(),
            Self::Conflict => "Config conflict".into(),
        }
    }
}

#[derive(Debug, Default)]
struct Data {
    revision: u64,
    settings: Settings,
    statuses: BTreeMap<String, Status>,
    last_notifications: BTreeMap<Topic, Instant>,
    chain_revision: u64,
    tx_revision: u64,
    tx_last: Option<Instant>,
    tx_first: Option<Instant>,
    blocks: VecDeque<Arc<wire::Block>>,
    transactions: VecDeque<(String, Arc<Value>)>,
    transaction_bytes: u64,
}

#[derive(Debug, Default)]
pub(crate) struct Hub {
    data: Mutex<Data>,
    pub chain_wake: tokio::sync::Notify,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Signal {
    pub version: u64,
    pub connected: bool,
    pub ready: bool,
}

impl Hub {
    fn data(&self) -> std::sync::MutexGuard<'_, Data> {
        self.data.lock().unwrap_or_else(|error| error.into_inner())
    }

    pub(crate) fn settings(&self) -> Settings {
        self.data().settings.clone()
    }

    pub(crate) fn configure(&self, settings: Settings) {
        let mut data = self.data();
        let changed = data.settings.endpoints != settings.endpoints
            || data
                .statuses
                .values()
                .any(|status| matches!(status, Status::Error(_)));
        if changed {
            data.reset();
        }
        data.settings = settings;
        drop(data);
        if changed {
            self.chain_wake.notify_one();
        }
    }

    pub(crate) fn header_status(&self, enabled: bool, polling: bool) -> &'static str {
        let data = self.data();
        if !enabled || (data.settings.configured.is_empty() && data.settings.errors.is_empty()) {
            "zmq off"
        } else if !polling {
            "zmq idle"
        } else if !data.last_notifications.is_empty() {
            "zmq live"
        } else if data
            .statuses
            .values()
            .any(|status| matches!(status, Status::Connected))
        {
            "zmq on"
        } else if !data.settings.errors.is_empty()
            || data
                .statuses
                .values()
                .any(|status| matches!(status, Status::Unreachable | Status::Error(_)))
        {
            "zmq down"
        } else {
            "zmq wait"
        }
    }

    pub(crate) fn feeds(&self) -> Vec<(Topic, String, FeedStatus)> {
        let data = self.data();
        data.settings
            .configured
            .iter()
            .map(|(&topic, endpoint)| {
                let status = if !data.settings.active(endpoint) {
                    FeedStatus::Conflict
                } else {
                    match data.statuses.get(endpoint) {
                        Some(Status::Connected) => data
                            .last_notifications
                            .get(&topic)
                            .copied()
                            .map_or(FeedStatus::Connected, FeedStatus::Receiving),
                        Some(Status::Unreachable) => FeedStatus::Unreachable,
                        Some(Status::Error(error)) => FeedStatus::Error(error.clone()),
                        _ => FeedStatus::Connecting,
                    }
                };
                (topic, endpoint.clone(), status)
            })
            .collect()
    }

    pub(crate) fn refresh_chain(&self) {
        self.data().changed(true, false, true);
        self.chain_wake.notify_one();
    }

    pub(crate) fn chain_signal(&self) -> Signal {
        let data = self.data();
        Signal {
            version: data.chain_revision,
            connected: data.connected(Topic::HashBlock, Topic::RawBlock),
            ready: true,
        }
    }

    pub(crate) fn mempool_connected(&self) -> bool {
        self.data().connected(Topic::HashTx, Topic::RawTx)
    }

    pub(crate) fn mempool_signal(&self, seen: u64) -> (Signal, Option<Instant>) {
        let mut data = self.data();
        if data.tx_revision == seen {
            data.tx_first = None;
        }
        let last_event = [Topic::HashTx, Topic::RawTx]
            .into_iter()
            .filter_map(|topic| data.last_notifications.get(&topic).copied())
            .max();
        let signal = Signal {
            version: data.tx_revision,
            connected: data.connected(Topic::HashTx, Topic::RawTx),
            ready: data
                .tx_last
                .is_none_or(|last| last.elapsed() >= Duration::from_millis(50))
                || data
                    .tx_first
                    .is_some_and(|first| first.elapsed() >= Duration::from_millis(250)),
        };
        (signal, last_event)
    }

    pub(crate) fn transaction(&self, txid: &str) -> Option<Arc<Value>> {
        self.data()
            .transactions
            .iter()
            .find(|(hash, _)| hash == txid)
            .map(|(_, tx)| tx.clone())
    }

    pub(crate) fn has_block_for_parent(&self, parent: Option<&str>) -> bool {
        self.data().blocks.iter().any(|block| {
            parent.map_or(block.parent == [0; 32], |parent| {
                wire::hash_hex(block.parent) == parent
            })
        })
    }

    pub(crate) fn block(
        &self,
        header: &Value,
        serialized_header: Option<&str>,
    ) -> Option<(Vec<Value>, usize, i32, String)> {
        let candidates = self.data().blocks.clone();
        let block = candidates.iter().rev().find(|item| {
            serialized_header.map_or_else(
                || item.matches_rpc(header),
                |expected| wire::hex(&item.header) == expected,
            )
        })?;
        Some((
            block.transactions.clone(),
            block.size,
            block.version,
            block.solution.clone(),
        ))
    }
}

impl Data {
    fn changed(&mut self, chain: bool, mempool: bool, urgent: bool) {
        if chain {
            self.chain_revision = self.chain_revision.wrapping_add(1);
        }
        if mempool {
            self.tx_revision = self.tx_revision.wrapping_add(1);
            let now = Instant::now();
            self.tx_last = (!urgent).then_some(now);
            self.tx_first.get_or_insert(now);
        }
    }

    fn reset(&mut self) {
        self.revision = self.revision.wrapping_add(1);
        self.clear_observations();
        self.changed(true, true, true);
    }

    fn clear_observations(&mut self) {
        self.statuses.clear();
        self.last_notifications.clear();
        self.blocks.clear();
        self.transactions.clear();
        self.transaction_bytes = 0;
    }

    fn connected(&self, hash: Topic, raw: Topic) -> bool {
        self.settings.endpoints.iter().any(|endpoint| {
            endpoint
                .topics
                .iter()
                .any(|topic| *topic == hash || *topic == raw)
                && matches!(self.statuses.get(&endpoint.url), Some(Status::Connected))
        })
    }
}

struct Worker(Arc<AtomicBool>);

impl Drop for Worker {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

pub(crate) fn spawn<E: Send + 'static>(
    config: crate::config::Config,
    events: tokio::sync::mpsc::UnboundedSender<E>,
) {
    if config.daemon.config_file.is_none() || !setup::local_host(&config.daemon.host) {
        return;
    }
    tokio::spawn(async move {
        let control = config.daemon.control.clone();
        let hub = &control.zmq;
        let mut epoch = None;
        let mut workers = Vec::new();
        while !events.is_closed() {
            let active = control.polling();
            let generation = control.generation();
            let revision = hub.data().revision;
            let current = (active, generation, revision);
            if epoch != Some(current) {
                workers.clear();
                if epoch.is_some() && active {
                    hub.refresh_chain();
                }
                let endpoints = {
                    let mut data = hub.data();
                    data.clear_observations();
                    data.settings.endpoints.clone()
                };
                if active {
                    for endpoint in endpoints {
                        let stop = Arc::new(AtomicBool::new(false));
                        let cancel = stop.clone();
                        let control = control.clone();
                        let testnet = config.chain.testnet;
                        workers.push(Worker(stop));
                        let _ = std::thread::Builder::new()
                            .name("carbide-zmq".into())
                            .spawn(move || {
                                let mut sequences = BTreeMap::new();
                                transport::subscribe(
                                    &endpoint.url,
                                    &endpoint.topics,
                                    &cancel,
                                    |event| {
                                        if !cancel.load(Ordering::Relaxed)
                                            && control.accepts(generation)
                                            && control.polling()
                                        {
                                            accept(
                                                &control.zmq,
                                                revision,
                                                &endpoint,
                                                &mut sequences,
                                                testnet,
                                                event,
                                                &cancel,
                                            );
                                        }
                                    },
                                );
                            });
                    }
                }
                epoch = Some(current);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
}

enum Body {
    Hash,
    Transaction(String, Arc<Value>),
    Block(Arc<wire::Block>),
    Unsupported,
}

fn accept(
    hub: &Hub,
    revision: u64,
    endpoint: &Endpoint,
    sequences: &mut BTreeMap<Topic, u32>,
    testnet: bool,
    event: transport::Event,
    cancelled: &AtomicBool,
) {
    use transport::Event;
    let decoded = match &event {
        Event::Data(topic, payload, sequence) if endpoint.topics.contains(topic) => {
            let body = match topic {
                Topic::HashBlock | Topic::HashTx if payload.len() == 32 => Body::Hash,
                Topic::RawBlock | Topic::CheckedBlock => wire::block(payload, testnet)
                    .map_or(Body::Unsupported, |block| Body::Block(Arc::new(block))),
                Topic::RawTx => wire::transaction(payload, testnet)
                    .map_or(Body::Unsupported, |(hash, tx)| {
                        Body::Transaction(hash, Arc::new(tx))
                    }),
                _ => Body::Unsupported,
            };
            Some((*topic, body, *sequence))
        }
        _ => None,
    };
    // The transport state and messages are committed only for the current config.
    let mut data = hub.data();
    if data.revision != revision || cancelled.load(Ordering::Relaxed) {
        return;
    }
    let chain_topic = endpoint
        .topics
        .iter()
        .any(|topic| matches!(topic, Topic::HashBlock | Topic::RawBlock));
    let tx_topic = endpoint
        .topics
        .iter()
        .any(|topic| matches!(topic, Topic::HashTx | Topic::RawTx));
    let wake;
    if let Some((topic, body, sequence)) = decoded {
        let gap = sequences
            .insert(topic, sequence)
            .is_some_and(|previous| previous.wrapping_add(1) != sequence);
        if !matches!(body, Body::Unsupported) {
            data.statuses
                .insert(endpoint.url.clone(), Status::Connected);
            data.last_notifications.insert(topic, Instant::now());
        }
        match body {
            Body::Block(block) => {
                if !data
                    .blocks
                    .iter()
                    .any(|stored| stored.header == block.header)
                {
                    if data.blocks.len() == 8 {
                        data.blocks.pop_front();
                    }
                    data.blocks.push_back(block);
                }
            }
            Body::Transaction(hash, tx)
                if !data.transactions.iter().any(|(stored, _)| stored == &hash) =>
            {
                data.transaction_bytes += tx["size"].as_u64().unwrap_or_default();
                data.transactions.push_back((hash, tx));
                while data.transactions.len() > 256 || data.transaction_bytes > 8_000_000 {
                    if let Some((_, tx)) = data.transactions.pop_front() {
                        data.transaction_bytes -= tx["size"].as_u64().unwrap_or_default();
                    }
                }
            }
            _ => {}
        }
        wake = matches!(topic, Topic::HashBlock | Topic::RawBlock) || (gap && chain_topic);
        data.changed(
            wake,
            matches!(topic, Topic::HashTx | Topic::RawTx) || (gap && tx_topic),
            gap,
        );
    } else {
        let status = match event {
            Event::Connected => Some(Status::Connected),
            Event::Unreachable => Some(Status::Unreachable),
            Event::Error(error) => Some(Status::Error(error)),
            Event::Malformed | Event::Data(..) => None,
        };
        let changed = match status {
            Some(status) if data.statuses.get(&endpoint.url) != Some(&status) => {
                data.statuses.insert(endpoint.url.clone(), status);
                data.last_notifications
                    .retain(|topic, _| !endpoint.topics.contains(topic));
                sequences.clear();
                true
            }
            None => true,
            _ => false,
        };
        wake = changed && chain_topic;
        data.changed(wake, changed && tx_topic, true);
    }
    drop(data);
    if wake {
        hub.chain_wake.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_topic_sequences_wrap_and_stale_workers_are_ignored() {
        let hub = Hub::default();
        let endpoint = Endpoint {
            url: "tcp://127.0.0.1:65535".into(),
            topics: vec![Topic::HashBlock, Topic::HashTx],
        };
        hub.configure(Settings {
            configured: endpoint
                .topics
                .iter()
                .map(|&topic| (topic, endpoint.url.clone()))
                .collect(),
            endpoints: vec![endpoint.clone()],
            ..Default::default()
        });
        let revision = hub.data().revision;
        let mut seq = BTreeMap::new();
        let stop = AtomicBool::new(false);
        let mut event = |event| accept(&hub, revision, &endpoint, &mut seq, false, event, &stop);
        event(transport::Event::Connected);
        assert!(hub.chain_signal().connected);
        event(transport::Event::Data(Topic::HashTx, vec![0; 32], u32::MAX));
        let feeds = hub.feeds();
        assert!(
            feeds
                .iter()
                .any(|(topic, _, status)| *topic == Topic::HashBlock
                    && matches!(status, FeedStatus::Connected))
        );
        assert!(
            feeds
                .iter()
                .any(|(topic, _, status)| *topic == Topic::HashTx
                    && matches!(status, FeedStatus::Receiving(_)))
        );
        let chain = hub.chain_signal().version;
        event(transport::Event::Data(Topic::HashTx, vec![0; 32], 0));
        assert_eq!(hub.chain_signal().version, chain);
        event(transport::Event::Data(Topic::HashTx, vec![0; 32], 2));
        assert_ne!(hub.chain_signal().version, chain);
        hub.configure(Settings::default());
        let changed = hub.chain_signal().version;
        event(transport::Event::Connected);
        assert_eq!(hub.chain_signal().version, changed);
        assert!(!hub.chain_signal().connected);
    }
}
