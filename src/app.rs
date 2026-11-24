use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::ops::Range;
use std::sync::atomic::Ordering;
mod api_permission_ui;
mod bookmark_ui;
mod byte_view;
mod chart_ui;
mod currency_browser;
mod currency_ui;
mod defi_ui;
mod height_ui;
mod identity_evidence;
mod input;
mod job_control;
mod json_dates;
mod marketplace_trace;
mod notarization_ui;
mod peer_controls;
mod registration_payments;
mod reorg_ui;
mod scroll_view;
mod source_ui;
mod token_control;
mod ui_timing;
mod witness_ui;
use std::fs;
use std::io::{self, Stdout};
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use crossterm::event::{
    self, DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture,
    Event, KeyCode, KeyModifiers,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, SetTitle, disable_raw_mode, enable_raw_mode,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::symbols::Marker;
use ratatui::text::{Line, Span};
use ratatui::widgets::canvas::{Canvas, Map, MapResolution};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use ratatui::{Frame, Terminal};
use serde_json::{Value, json};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::block_time::{self, RpcTimeSource, TimeBlock, TimeSource};
use crate::cache::{Cache, cache_disk_usage};
use crate::chain_state::{ChainState, RpcObservation, StatusBadge, coinbase_maturity};
use crate::config::Config;
use crate::domain::{
    BlockProductionKind, BlockSummary, MarketplaceTag, ObjectRoute, OutputTag, OutputTagKind,
    TransactionSummary, TransactionTag, defi_currency_ids, emission_timelock, output_tags,
    private_input_count, private_output_count,
};
use crate::geoip::{self, GeoIpDatabase, GeoIpStatus, GeoLocation};
use crate::log_feed::{self, LogChunk};
use crate::resolver::classify_start_object;
use crate::rpc::{BlockId, RpcClient};
use crate::theme::{SELECTION_MARKER, Theme};

const APP_TITLE: &str = "Verus Carbide";

type Tui = Terminal<CrosstermBackend<Stdout>>;
type BlockHash = String;
type Txid = String;
const DASHBOARD_BLOCK_COUNT: u64 = 12;
const DASHBOARD_TRANSACTION_COUNT: usize = 100;
const DASHBOARD_LOADING_FRAME: Duration = Duration::from_millis(125);
const TRANSACTION_TILE_HEIGHT: u16 = 5;
const TRANSACTION_FLOW_TILE_HEIGHT: u16 = 6;
const JSON_STRING_PREVIEW_CHARS: usize = 96;
const SECTION_HEADING_HEIGHT: u16 = 2;
const BOOKMARKS_HINT: &str = "F6 bookmarks | ";
const ADDRESS_TRANSACTION_LIMIT: usize = 100;
const ADDRESS_RECENT_BLOCK_WINDOW: u64 = 10_000;
const TRANSACTION_LOAD_BATCH_SIZE: usize = 8;
const TRANSACTION_LOAD_CONCURRENCY: usize = 2;
const LOAD_RETRY_SECONDS: u64 = 10;
const ADDRESS_FLOW_LIMIT: usize = 6;
const ADDRESS_TRANSACTION_TILE_MIN_HEIGHT: u16 = 3;
const PEER_REFRESH_SECONDS: u64 = 5;
const MEMPOOL_REFRESH_SECONDS: u64 = 2;
const MEMPOOL_TILE_HEIGHT: u16 = 5;
const MARKETPLACE_TILE_HEIGHT: u16 = 5;
const LOG_REFRESH_MILLISECONDS: u64 = 750;
const LOG_LINE_LIMIT: usize = 4_000;
const PEER_TILE_HEIGHT: u16 = 5;
const APP_VERSION: &str = "TEST";
const APP_EVENT_BATCH_LIMIT: usize = 32;
const APP_EVENT_BUDGET: Duration = Duration::from_millis(8);

#[derive(Debug, Clone)]
struct ChainTipStatus {
    label: String,
    healthy: bool,
    height: Option<u64>,
    longest_chain: Option<u64>,
    last_block_time: Option<u64>,
    connections: Option<u64>,
}

#[derive(Debug, Clone)]
struct DashboardSnapshot {
    tip: ChainTipStatus,
    blocks: Vec<BlockRecord>,
}

#[derive(Debug, Clone)]
struct BlockRecord {
    summary: BlockSummary,
    raw_json: Value,
    detail_loaded: bool,
    prepared_transactions: HashMap<Txid, Arc<PreparedTransaction>>,
}

#[derive(Debug, Clone)]
struct TransactionRecord {
    source_id: Option<String>,
    summary: TransactionSummary,
    raw_json: Value,
    block_hash: Option<BlockHash>,
    block_height: Option<u64>,
    block_time: u64,
    prepared: Option<Arc<PreparedTransaction>>,
}

#[derive(Debug)]
struct PreparedTransaction {
    tags: Vec<TransactionTag>,
    identities: BTreeSet<String>,
    parents: Vec<Txid>,
    currencies: Vec<String>,
    currency_names: HashMap<String, String>,
    marketplace_publication: Option<marketplace_trace::Offer>,
}

impl PreparedTransaction {
    fn new(summary: &TransactionSummary, raw: &Value) -> Self {
        let mut identities = BTreeSet::new();
        collect_identity_references(raw, &mut identities);
        let mut currency_names = HashMap::new();
        collect_currency_name_hints(raw, &mut currency_names);
        Self {
            tags: summary.tags(),
            identities,
            parents: source_transaction_ids(raw),
            currencies: defi_currency_ids(raw),
            currency_names,
            marketplace_publication: marketplace_trace::publication(raw),
        }
    }
}

#[derive(Debug, Clone)]
struct AddressRecord {
    address: String,
    transaction_count: usize,
    transaction_ids: Vec<Txid>,
    first_txid: Option<Txid>,
    latest_txid: Option<Txid>,
    unspent_outpoints: HashSet<OutPoint>,
    balance_response: Option<Value>,
    utxos_complete: bool,
    history_complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct OutPoint {
    txid: Txid,
    index: u64,
}

#[derive(Debug)]
struct AddressLoadResult {
    address: AddressRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IdentityAssociation {
    identity_address: String,
    display_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IdentityAssociationSource {
    LocalDaemon,
    PublicApi,
}

#[derive(Debug, Clone)]
struct AddressIdentityAssociations {
    identities: Vec<IdentityAssociation>,
    source: IdentityAssociationSource,
}

#[derive(Debug, Clone)]
struct IdentityVersionRecord {
    identity_address: String,
    fully_qualified_name: String,
    block_height: u64,
    block_time: u64,
    raw_json: Value,
}

#[derive(Debug)]
struct IdentityLoadResult {
    query: String,
    versions: Vec<IdentityVersionRecord>,
    resolved_names: HashMap<String, String>,
    transactions: Vec<TransactionRecord>,
    history_complete: bool,
}

#[derive(Debug, Clone)]
struct CurrencyDefinitionRecord {
    currency_id: String,
    name: String,
    definition_json: Value,
}

#[derive(Debug, Clone, Default)]
struct ObjectStore {
    workspace_system: Option<crate::world::SystemId>,
    stale_identities: HashSet<String>,
    transaction_inclusions: HashMap<Txid, HashSet<BlockHash>>,
    transactions_by_inclusion: HashMap<BlockHash, HashSet<Txid>>,
    stale_addresses: HashSet<String>,
    reverse_relationships: HashMap<Txid, Vec<crate::relationships::Link>>,
    chain: ChainState,
    blocks_by_hash: HashMap<BlockHash, BlockRecord>,
    block_hash_by_height: BTreeMap<u64, BlockHash>,
    transactions_by_txid: HashMap<Txid, TransactionRecord>,
    addresses_by_address: HashMap<String, AddressRecord>,
    output_unspent: HashMap<OutPoint, bool>,
    output_status_attempted: HashSet<OutPoint>,
    identity_versions: HashMap<(String, u64), IdentityVersionRecord>,
    identity_heights: HashMap<String, BTreeSet<u64>>,
    identity_aliases: HashMap<String, String>,
    identity_names: HashMap<String, String>,
    identity_history_complete: HashSet<String>,
    identity_evidence: HashMap<String, (u64, identity_evidence::IdentityEvidence)>,
    registration_payments:
        HashMap<registration_payments::Key, (Arc<()>, registration_payments::Status)>,
    token_control: HashMap<String, token_control::Lookup>,
    loading_identity_evidence: HashSet<String>,
    loading_historical_identity_evidence: HashSet<String>,
    identities_by_primary_address: HashMap<String, AddressIdentityAssociations>,
    currencies_by_id: HashMap<String, CurrencyDefinitionRecord>,
    currency_references: BTreeSet<String>,
    currency_name_hints: HashMap<String, String>,
    marketplace_tags_by_txid: HashMap<Txid, BTreeSet<MarketplaceTag>>,
    marketplace_proofs: HashMap<Txid, HashSet<Txid>>,
    marketplace_searches: HashMap<Txid, marketplace_trace::search::Cursor>,
    identity_offers: HashMap<String, IdentityOffersState>,
}

#[derive(Debug, Clone)]
struct DashboardTransaction {
    block: BlockHash,
    position: usize,
    txid: Txid,
}

#[derive(Debug, Clone)]
struct DashboardState {
    recent_blocks: Vec<BlockHash>,
    block_cursor: usize,
    recent_transactions: Vec<DashboardTransaction>,
    selected_transaction: usize,
    transactions_focused: bool,
    loading_older_since: Option<Instant>,
    has_more: bool,
    older_retry_after: Option<Instant>,
    older_error: Option<String>,
}

impl Default for DashboardState {
    fn default() -> Self {
        Self {
            recent_blocks: Vec::new(),
            block_cursor: 0,
            recent_transactions: Vec::new(),
            selected_transaction: 0,
            transactions_focused: false,
            loading_older_since: None,
            has_more: true,
            older_retry_after: None,
            older_error: None,
        }
    }
}

impl DashboardState {
    fn selection(&mut self) -> (&mut usize, usize) {
        if self.transactions_focused {
            (
                &mut self.selected_transaction,
                self.recent_transactions.len(),
            )
        } else {
            (&mut self.block_cursor, self.recent_blocks.len() + 1)
        }
    }

    fn selected_block_index(&self) -> Option<usize> {
        self.block_cursor.checked_sub(1)
    }

    fn selected_block_hash(&self) -> Option<&BlockHash> {
        self.recent_blocks.get(self.selected_block_index()?)
    }

    fn restore_block_selection(&mut self, hash: Option<BlockHash>) {
        self.block_cursor = hash
            .and_then(|hash| {
                self.recent_blocks
                    .iter()
                    .position(|candidate| *candidate == hash)
            })
            .map_or(0, |index| index + 1);
    }
}

#[derive(Debug, Clone)]
struct MempoolEntry {
    txid: Txid,
    size: u64,
    fee: Option<f64>,
    time: u64,
    height: Option<u64>,
    depends: Vec<Txid>,
}

#[derive(Debug)]
struct MempoolSnapshot {
    entries: Vec<MempoolEntry>,
    total_bytes: u64,
    previews: HashMap<Txid, TransactionSummary>,
}

#[derive(Debug, Clone, Default)]
struct IdentityOffersState {
    offers: Vec<MarketplaceOffer>,
    loading: bool,
    checked_at: Option<Instant>,
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct MarketplaceOffer {
    txid: Txid,
    category: String,
    subject_id: Option<String>,
    asking_price: Option<MarketplaceAsk>,
    offered: Value,
    accepted: Value,
    expiry_height: u64,
}

#[derive(Debug, Clone)]
struct MarketplaceAsk {
    currency: String,
    amount: i128,
}

#[derive(Debug)]
struct MarketplaceSnapshot {
    subject: String,
    offers: Vec<MarketplaceOffer>,
}

#[derive(Debug, Default)]
struct MarketplaceState {
    subject: String,
    offers: Vec<MarketplaceOffer>,
    selected: usize,
    selected_link: usize,
    preferred_link: Option<MarketplaceLink>,
    sort: MarketplaceSort,
    descending: bool,
    loading: bool,
    last_checked_tip: Option<BlockHash>,
    attempted: bool,
    refresh_requested: bool,
    error: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum MarketplaceSort {
    #[default]
    Expiry,
    Name,
    Price,
    Age,
}
impl MarketplaceSort {
    fn next(self) -> Self {
        match self {
            Self::Expiry => Self::Name,
            Self::Name => Self::Price,
            Self::Price => Self::Age,
            Self::Age => Self::Expiry,
        }
    }
    fn previous(self) -> Self {
        match self {
            Self::Expiry => Self::Age,
            Self::Name => Self::Expiry,
            Self::Price => Self::Name,
            Self::Age => Self::Price,
        }
    }
    fn label(self) -> &'static str {
        match self {
            Self::Expiry => "expiry",
            Self::Name => "name",
            Self::Price => "ask amount",
            Self::Age => "listing age",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MarketplaceLink {
    Funding(Txid),
    Identity(String),
    Currency(String),
    Address(String),
}

#[derive(Debug, Default)]
struct MempoolState {
    entries: Vec<MempoolEntry>,
    previews: HashMap<Txid, TransactionSummary>,
    total_bytes: u64,
    selected: usize,
    loading: bool,
    bulk_pending: bool,
    last_refresh: Option<Instant>,
    error: Option<String>,
}

#[derive(Debug)]
struct AppState {
    route_label: String,
    tip: ChainTipStatus,
    store: ObjectStore,
    dashboard: DashboardState,
    mempool: MempoolState,
    marketplace: MarketplaceState,
    screen: Screen,
    screen_stack: Vec<Screen>,
    loading_blocks: BTreeSet<String>,
    loading_transactions: BTreeSet<Txid>,
    transaction_retry_after: HashMap<Txid, Instant>,
    transaction_verified_tip: HashMap<Txid, BlockHash>,
    loading_addresses: BTreeSet<String>,
    loading_address_histories: BTreeSet<String>,
    loading_address_utxos: BTreeSet<String>,
    loading_address_balances: BTreeSet<String>,
    address_errors: HashMap<String, String>,
    address_retry_after: HashMap<String, Instant>,
    address_history_errors: HashMap<String, String>,
    address_history_retry_after: HashMap<String, Instant>,
    address_utxo_errors: HashMap<String, String>,
    address_utxo_retry_after: HashMap<String, Instant>,
    address_balance_errors: HashMap<String, String>,
    address_balance_retry_after: HashMap<String, Instant>,
    loading_outpoints: HashSet<OutPoint>,
    loading_identities: HashSet<String>,
    identity_load_attempted: HashSet<String>,
    loading_identity_names: HashSet<String>,
    identity_name_attempted: HashSet<String>,
    loading_address_identities: HashSet<String>,
    address_identities_attempted: HashSet<String>,
    loading_currencies: HashSet<String>,
    currency_load_attempted: HashSet<String>,
    unmask_identity_addresses: bool,
    notice: Option<TransientNotice>,
    clipboard_busy: bool,
    save_prompt: Option<SavePrompt>,
    cache_clear_prompt: Option<CacheClearPrompt>,
    cache_size_bytes: Option<u64>,
    search: SearchState,
    auxiliary: Option<AuxiliaryScreen>,
    help_return: Option<AuxiliaryScreen>,
    peers: PeerState,
    log_feed: LogFeedState,
}

#[derive(Debug, Clone)]
enum AuxiliaryScreen {
    Help {
        scroll: usize,
    },
    PeerBrowser,
    PeerDetail {
        peer_id: i64,
        mode: ViewMode,
        json: JsonState,
        scroll: usize,
    },
}

#[derive(Debug, Clone)]
struct PeerRecord {
    id: i64,
    raw_json: Value,
    ip: Option<IpAddr>,
    geo: Option<GeoLocation>,
}

struct PeerState {
    records: Vec<PeerRecord>,
    controls: peer_controls::State,
    selected: usize,
    loading: bool,
    last_refresh: Option<Instant>,
    map_enabled: bool,
    geoip_directory: PathBuf,
    geoip_status: GeoIpStatus,
    geoip_database: Option<Arc<GeoIpDatabase>>,
    geoip_downloading: bool,
}

impl std::fmt::Debug for PeerState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PeerState")
            .field("records", &self.records)
            .field("selected", &self.selected)
            .field("loading", &self.loading)
            .field("last_refresh", &self.last_refresh)
            .field("map_enabled", &self.map_enabled)
            .field("geoip_directory", &self.geoip_directory)
            .field("geoip_status", &self.geoip_status)
            .field("geoip_downloading", &self.geoip_downloading)
            .finish()
    }
}

#[derive(Debug)]
struct LogFeedState {
    path: Option<PathBuf>,
    offset: Option<u64>,
    partial: String,
    lines: VecDeque<String>,
    loading: bool,
    last_refresh: Option<Instant>,
    error: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct SearchState {
    active: bool,
    query: String,
    pending_request: Option<u64>,
    task: Option<tokio::task::AbortHandle>,
    next_request: u64,
    status: Option<SearchStatus>,
}

#[derive(Debug, Clone)]
struct SearchStatus {
    message: String,
    error: bool,
}

#[derive(Debug, Clone)]
struct TransientNotice {
    message: String,
    until: Instant,
}

#[derive(Debug, Clone)]
enum Screen {
    Dashboard,
    Mempool,
    Marketplace,
    Currency(Box<currency_ui::View>),
    Notarizations(Box<notarization_ui::View>),
    Witnesses(Box<witness_ui::View>),
    Bytes(byte_view::View),
    Currencies(Box<currency_browser::View>),
    Charts(Box<chart_ui::View>),
    Estimate(Box<crate::block_forecast::Estimate>),
    Block {
        target: BlockTarget,
        selected_tx: usize,
        mode: ViewMode,
        json: JsonState,
    },
    Transaction {
        txid: Txid,
        selected_flow: usize,
        mode: ViewMode,
        json: JsonState,
        notarization: Option<notarization_ui::TransactionContext>,
    },
    Address {
        address: String,
        selected_tx: usize,
        selected_identity: Option<usize>,
    },
    Identity {
        target: IdentityTarget,
        selected_tx: usize,
        selected_primary_address: Option<usize>,
        historical_height_input: Option<String>,
        mode: ViewMode,
        json: JsonState,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IdentityTarget {
    query: String,
    height: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BlockTarget {
    Hash(BlockHash),
    Height(u64),
}

impl BlockTarget {
    fn to_block_id(&self) -> BlockId {
        match self {
            Self::Hash(hash) => BlockId::Hash(hash.clone()),
            Self::Height(height) => BlockId::Height(*height),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ViewMode {
    Overview,
    Json,
}

#[derive(Debug)]
enum AppEvent {
    NotarizationChanged,
    MarketplaceSearch {
        funding: String,
        request: Arc<()>,
        page: Box<marketplace_trace::search::ResultPage>,
    },
    RegistrationPayments {
        key: registration_payments::Key,
        request: Arc<()>,
        report: Option<registration_payments::Report>,
    },
    TokenControl {
        identity: String,
        request: Arc<()>,
        status: token_control::Status,
    },
    ClipboardCopied {
        label: String,
        bytes: usize,
        result: Result<crate::clipboard::PreparedCopy, String>,
    },
    ChartUpdated {
        request: u64,
        page: Option<chart_ui::Page>,
        done: bool,
        error: Option<String>,
    },
    ChartSeeked {
        request: u64,
        result: Result<chart_ui::SeekOutcome, String>,
    },
    ChartSeekProgress {
        request: u64,
        message: String,
    },
    ReconciliationStarted,
    RevisionGuarded {
        control: Arc<crate::sources::Control>,
        epoch: u64,
        generation: u64,
        event: Box<AppEvent>,
    },
    Reconciled(Result<Arc<crate::chain_state::PreparedBranch>, String>),
    Guarded {
        control: Arc<crate::sources::Control>,
        generation: u64,
        event: Box<AppEvent>,
    },
    Observation(RpcObservation),
    DashboardRefresh(Result<DashboardSnapshot, String>),
    DashboardPage {
        started: Instant,
        result: Result<Vec<BlockRecord>, String>,
    },
    MempoolRefresh(Result<MempoolSnapshot, String>),
    IdentityOffers {
        identity: String,
        result: Result<Vec<MarketplaceOffer>, String>,
    },
    MarketplaceRefresh {
        tip: BlockHash,
        result: Result<MarketplaceSnapshot, String>,
    },
    BlockDetail {
        key: String,
        result: Result<BlockRecord, String>,
    },
    TransactionRefresh {
        txid: Txid,
        tip: BlockHash,
        result: Result<TransactionRecord, String>,
    },
    Transactions {
        requested: Vec<Txid>,
        records: Vec<TransactionRecord>,
        failed: Vec<Txid>,
    },
    AddressDetail {
        address: String,
        result: Result<AddressLoadResult, String>,
    },
    AddressHistory {
        address: String,
        result: Result<Vec<Txid>, String>,
    },
    AddressUtxos {
        address: String,
        result: Result<HashSet<OutPoint>, String>,
    },
    AddressBalance {
        address: String,
        result: Result<Value, String>,
    },
    IdentityDetail {
        query: String,
        result: Result<IdentityLoadResult, String>,
    },
    IdentityEvidence {
        address: String,
        height: u64,
        cached: bool,
        result: Result<identity_evidence::IdentityEvidence, String>,
    },
    IdentityHistoricalEvidence {
        address: String,
        height: u64,
        result: Result<identity_evidence::IdentityEvidence, String>,
    },
    AddressIdentities {
        address: String,
        result: Result<Option<AddressIdentityAssociations>, String>,
    },
    IdentityNames {
        requested: Vec<String>,
        resolved: HashMap<String, String>,
    },
    CurrencyDefinitions {
        requested: Vec<String>,
        records: Vec<CurrencyDefinitionRecord>,
    },
    CurrencyViewLoaded(currency_ui::Loaded),
    CurrencyBrowserLoaded(currency_browser::Loaded),
    CurrencyHistoryLoaded {
        query: String,
        request_id: u64,
        note: Option<String>,
        result: Result<crate::cache::CurrencySnapshot, String>,
    },
    CurrencyActivityLoaded {
        query: String,
        request_id: u64,
        result: Result<crate::currency_activity::Page, String>,
    },
    CurrencyTransfersLoaded {
        query: String,
        request_id: u64,
        result: Result<(Vec<crate::world::OutpointRef>, bool), String>,
    },
    OutputStates {
        requested: Vec<OutPoint>,
        states: Vec<(OutPoint, bool)>,
    },
    SearchResolved {
        request_id: u64,
        result: Result<SearchResolution, String>,
    },
    Peers(peer_controls::Refresh),
    PeerAction(Result<String, String>),
    GeoIpDownloaded(Result<String, String>),
    LogChunk(Result<LogChunk, String>),
    CacheCleared(Result<u64, String>),
    StoredRelationships {
        txid: Txid,
        result: Result<Vec<crate::relationships::Link>, String>,
    },
}

#[derive(Debug)]
enum SearchResolution {
    Estimate(crate::block_forecast::Estimate),
    Identity {
        query: String,
        version: IdentityVersionRecord,
    },
    Block(BlockRecord),
    TimeBlock {
        block: BlockRecord,
        found: crate::block_time::LocatedBlock,
    },
    Transaction {
        txid: Txid,
        records: Vec<TransactionRecord>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CachedHashMatch {
    Transaction,
    Block,
}

#[derive(Debug, Clone)]
struct JsonState {
    initial_focus: Option<String>,
    expanded: BTreeSet<String>,
    cursor: usize,
    scroll: Cell<usize>,
    last_drawn_cursor: Cell<usize>,
    scalar_scroll: usize,
}

struct JsonDocument<'a> {
    title: String,
    root: &'a Value,
    default_filename: String,
}

#[derive(Debug, Clone)]
struct SavePrompt {
    basename: String,
    extension: String,
    selection_active: bool,
    status: Option<String>,
}

#[derive(Debug, Clone)]
struct CacheClearPrompt {
    size_bytes: u64,
    clearing: bool,
}

struct SummaryTile {
    title: Option<Line<'static>>,
    selected: bool,
    lines: Vec<Line<'static>>,
    alignment: Alignment,
}

struct TransactionTileContext<'a> {
    currency: &'a str,
    store: &'a ObjectStore,
    block_time: u64,
}

struct TransactionFlowRenderContext<'a> {
    transaction: &'a TransactionRecord,
    theme: &'a Theme,
    store: &'a ObjectStore,
    currency: &'a str,
    unmask_identity_addresses: bool,
}

impl ObjectStore {
    fn insert_block(&mut self, block: BlockRecord) -> BlockHash {
        let hash = block.summary.hash.clone();
        self.chain.remember_block(
            hash.clone(),
            block.summary.height,
            block.summary.time,
            block.summary.previousblockhash.clone(),
        );
        if self.chain.reconciled.is_none()
            || self
                .chain
                .reconciled
                .as_ref()
                .and_then(|s| s.canonical.get(&block.summary.height))
                .is_some_and(|h| h.hash == hash)
        {
            self.block_hash_by_height
                .insert(block.summary.height, hash.clone());
        }

        let should_replace_block = self
            .blocks_by_hash
            .get(&hash)
            .is_none_or(|current| block.detail_loaded || !current.detail_loaded);

        if should_replace_block {
            for tx in &block.summary.tx {
                self.note_inclusion(&tx.txid, &hash);
                let Some(raw_json) = tx.raw.clone() else {
                    continue;
                };
                self.replace_transaction(TransactionRecord {
                    source_id: None,
                    summary: tx.clone(),
                    raw_json,
                    block_hash: Some(hash.clone()),
                    block_height: Some(block.summary.height),
                    block_time: block.summary.time,
                    prepared: block.prepared_transactions.get(&tx.txid).cloned(),
                });
            }

            self.blocks_by_hash.insert(hash.clone(), block);
        }

        hash
    }

    fn block(&self, hash: &str) -> Option<&BlockRecord> {
        self.blocks_by_hash.get(hash)
    }

    fn block_hash_at_height(&self, height: u64) -> Option<&BlockHash> {
        if let Some(branch) = &self.chain.reconciled {
            return branch.canonical.get(&height).map(|header| &header.hash);
        }
        self.block_hash_by_height.get(&height)
    }

    fn block_for_target(&self, target: &BlockTarget) -> Option<&BlockRecord> {
        match target {
            BlockTarget::Hash(hash) => self.block(hash),
            BlockTarget::Height(height) => self
                .block_hash_at_height(*height)
                .and_then(|hash| self.block(hash)),
        }
    }

    fn transaction(&self, txid: &str) -> Option<&TransactionRecord> {
        self.transactions_by_txid.get(txid)
    }

    fn insert_transaction(&mut self, mut transaction: TransactionRecord) {
        if let Some(hash) = &transaction.block_hash {
            self.note_inclusion(&transaction.summary.txid, hash);
        }
        if let Some(block_hash) = transaction.block_hash.as_deref()
            && transaction.block_height.is_none()
        {
            transaction.block_height = self
                .blocks_by_hash
                .get(block_hash)
                .map(|block| block.summary.height);
        }
        let txid = transaction.summary.txid.clone();
        for hash in self.transaction_inclusions.get(&txid).into_iter().flatten() {
            if let Some(block) = self.blocks_by_hash.get_mut(hash)
                && let Some(summary) = block
                    .summary
                    .tx
                    .iter_mut()
                    .find(|summary| summary.txid == txid)
            {
                summary.raw = Some(transaction.raw_json.clone());
            }
        }
        if self
            .transactions_by_txid
            .get(&txid)
            .is_none_or(|current| current.block_hash.is_none() && transaction.block_hash.is_some())
        {
            self.replace_transaction(transaction);
        } else if let Some(prepared) = transaction.prepared {
            self.learn_prepared_currency_data(&prepared);
        }
    }

    fn learn_prepared_currency_data(&mut self, prepared: &PreparedTransaction) {
        self.currency_references
            .extend(prepared.currencies.iter().cloned());
        for (id, name) in &prepared.currency_names {
            self.currency_name_hints.insert(id.clone(), name.clone());
        }
    }

    fn replace_transaction(&mut self, mut transaction: TransactionRecord) {
        let txid = transaction.summary.txid.clone();
        let prepared = transaction.prepared.get_or_insert_with(|| {
            Arc::new(PreparedTransaction::new(
                &transaction.summary,
                &transaction.raw_json,
            ))
        });
        self.learn_prepared_currency_data(prepared);
        if let Some(offer) = &prepared.marketplace_publication {
            self.marketplace_proofs
                .entry(offer.funding.clone())
                .or_default()
                .insert(txid.clone());
        }
        self.transactions_by_txid.insert(txid, transaction);
    }

    fn note_inclusion(&mut self, txid: &str, hash: &str) {
        if self
            .transaction_inclusions
            .entry(txid.into())
            .or_default()
            .insert(hash.into())
        {
            self.transactions_by_inclusion
                .entry(hash.into())
                .or_default()
                .insert(txid.into());
        }
    }

    fn project_transaction_inclusions(
        &mut self,
        branch: &crate::chain_state::PreparedBranch,
        incremental: bool,
    ) {
        fn project(
            branch: &crate::chain_state::PreparedBranch,
            inclusions: &HashSet<BlockHash>,
            record: &mut TransactionRecord,
        ) {
            if let Some(header) = inclusions
                .iter()
                .filter_map(|hash| branch.canonical_block(hash))
                .min_by_key(|header| header.height)
            {
                record.block_hash = Some(header.hash.clone());
                record.block_height = Some(header.height);
                record.block_time = header.time;
            }
        }
        if incremental {
            for hash in &branch.changed_hashes {
                for txid in self
                    .transactions_by_inclusion
                    .get(hash)
                    .into_iter()
                    .flatten()
                {
                    if let Some(record) = self.transactions_by_txid.get_mut(txid) {
                        project(branch, &self.transaction_inclusions[txid], record);
                    }
                }
            }
        } else {
            for (txid, inclusions) in &self.transaction_inclusions {
                if let Some(record) = self.transactions_by_txid.get_mut(txid) {
                    project(branch, inclusions, record);
                }
            }
        }
    }

    fn contains_block(&self, hash: &str) -> bool {
        self.blocks_by_hash.contains_key(hash)
    }

    fn contains_transaction(&self, txid: &str) -> bool {
        self.transactions_by_txid.contains_key(txid)
    }

    fn insert_identity_version(&mut self, query: &str, version: IdentityVersionRecord) {
        let address = version.identity_address.clone();
        let height = version.block_height;
        self.insert_identity_alias(query, &address);
        self.identity_aliases
            .insert(address.clone(), address.clone());
        self.insert_identity_alias(&version.fully_qualified_name, &address);
        self.identity_heights
            .entry(address.clone())
            .or_default()
            .insert(height);
        self.identity_versions
            .insert((address.clone(), height), version);
        self.refresh_identity_name(&address);
    }

    fn insert_resolved_identity_name(&mut self, address: String, name: String) {
        self.insert_identity_alias(&address, &address);
        self.insert_identity_alias(&name, &address);
        self.identity_names.entry(address).or_insert(name);
    }

    fn insert_identity_alias(&mut self, alias: &str, address: &str) {
        self.identity_aliases
            .insert(alias.to_string(), address.to_string());
        self.identity_aliases
            .insert(identity_alias_key(alias), address.to_string());
    }

    fn refresh_identity_name(&mut self, address: &str) {
        let Some(heights) = self.identity_heights.get(address) else {
            return;
        };
        let preferred = heights
            .iter()
            .rev()
            .filter_map(|height| self.identity_versions.get(&(address.to_string(), *height)))
            .find(|version| identity_version_is_active(version))
            .or_else(|| {
                heights
                    .iter()
                    .next()
                    .and_then(|height| self.identity_versions.get(&(address.to_string(), *height)))
            })
            .map(|version| version.fully_qualified_name.clone());
        if let Some(name) = preferred {
            self.insert_identity_alias(&name, address);
            self.identity_names.insert(address.to_string(), name);
        }
    }

    fn identity_address_for_query(&self, query: &str) -> Option<&str> {
        self.identity_aliases
            .get(query)
            .or_else(|| self.identity_aliases.get(&identity_alias_key(query)))
            .map(String::as_str)
    }

    fn identity_for_target(&self, target: &IdentityTarget) -> Option<&IdentityVersionRecord> {
        let address = self.identity_address_for_query(&target.query)?;
        let height = target.height.or_else(|| {
            self.identity_heights
                .get(address)
                .and_then(|heights| heights.last().copied())
        })?;
        self.identity_versions.get(&(address.to_string(), height))
    }

    fn identity_name(&self, address: &str) -> Option<&str> {
        self.identity_names.get(address).map(String::as_str)
    }

    fn identity_local_name(&self, address: &str) -> Option<&str> {
        let heights = self.identity_heights.get(address)?;
        heights
            .iter()
            .rev()
            .filter_map(|height| self.identity_versions.get(&(address.to_string(), *height)))
            .find(|version| identity_version_is_active(version))
            .or_else(|| {
                heights
                    .iter()
                    .next()
                    .and_then(|height| self.identity_versions.get(&(address.to_string(), *height)))
            })
            .and_then(|version| version.raw_json.get("identity"))
            .and_then(|identity| identity.get("name"))
            .and_then(Value::as_str)
    }

    fn address(&self, address: &str) -> Option<&AddressRecord> {
        self.addresses_by_address.get(address)
    }

    fn address_mut(&mut self, address: &str) -> Option<&mut AddressRecord> {
        self.addresses_by_address.get_mut(address)
    }

    fn insert_address(&mut self, address: AddressRecord) {
        for outpoint in &address.unspent_outpoints {
            self.output_unspent.insert(outpoint.clone(), true);
        }
        self.addresses_by_address
            .insert(address.address.clone(), address);
    }

    fn identity_associations(&self, address: &str) -> Option<&AddressIdentityAssociations> {
        self.identities_by_primary_address.get(address)
    }

    fn insert_identity_associations(
        &mut self,
        address: String,
        associations: AddressIdentityAssociations,
    ) {
        self.identities_by_primary_address
            .insert(address, associations);
    }

    fn insert_currency_definition(&mut self, record: CurrencyDefinitionRecord) {
        self.currencies_by_id
            .entry(record.currency_id.clone())
            .or_insert(record);
    }

    fn learn_currency_data(&mut self, value: &Value) {
        self.currency_references.extend(defi_currency_ids(value));
        collect_currency_name_hints(value, &mut self.currency_name_hints);
    }

    fn currency_name(&self, currency_id: &str) -> Option<&str> {
        self.currencies_by_id
            .get(currency_id)
            .and_then(|record| {
                (!record.name.is_empty())
                    .then_some(record.name.as_str())
                    .or_else(|| {
                        record
                            .definition_json
                            .get("fullyqualifiedname")
                            .or_else(|| record.definition_json.get("name"))
                            .and_then(Value::as_str)
                    })
            })
            .or_else(|| {
                self.currency_name_hints
                    .get(currency_id)
                    .map(String::as_str)
            })
    }

    fn add_marketplace_tag(&mut self, txid: impl Into<Txid>, tag: MarketplaceTag) {
        self.marketplace_tags_by_txid
            .entry(txid.into())
            .or_default()
            .insert(tag);
    }

    fn marketplace_publication(&self, proof: &str) -> Option<&marketplace_trace::Offer> {
        let transaction = self.transaction(proof)?;
        if marketplace_trace::orphan(&transaction.raw_json) {
            return None;
        }
        transaction
            .prepared
            .as_deref()?
            .marketplace_publication
            .as_ref()
    }

    fn marketplace_contract(&self, funding: &str) -> Option<(&str, &marketplace_trace::Offer)> {
        let transaction = self.transaction(funding)?;
        if !marketplace_funding_shape(&transaction.raw_json)
            || marketplace_trace::orphan(&transaction.raw_json)
        {
            return None;
        }
        let proofs = self.marketplace_proofs.get(funding)?;
        let mut offers: Vec<_> = proofs
            .iter()
            .filter_map(|proof| {
                let offer = self
                    .marketplace_publication(proof)
                    .filter(|offer| offer.funding == funding)?;
                Some((proof.as_str(), offer))
            })
            .collect();
        offers.sort_unstable_by_key(|(proof, offer)| (!offer.spends_publication_funds, *proof));
        let &(proof, offer) = offers.first()?;
        let conflict = offers
            .iter()
            .take_while(|(_, other)| {
                other.spends_publication_funds == offer.spends_publication_funds
            })
            .any(|(_, other)| !offer.same_contract(other));
        (!conflict).then_some((proof, offer))
    }

    fn marketplace_execution(&self, raw: &Value) -> Option<marketplace_trace::Spend> {
        let funding = raw["vin"].get(0)?["txid"].as_str()?;
        let (_, offer) = self.marketplace_contract(funding)?;
        marketplace_trace::classify(offer, &self.transaction(funding)?.raw_json, raw)
    }

    fn marketplace_offer(&self, txid: &str) -> bool {
        self.marketplace_tags_by_txid
            .get(txid)
            .is_some_and(|tags| tags.contains(&MarketplaceTag::Offer))
            || self.marketplace_contract(txid).is_some()
    }

    fn marketplace_tags(&self, txid: &str) -> impl Iterator<Item = MarketplaceTag> {
        let mut tags = self
            .marketplace_tags_by_txid
            .get(txid)
            .cloned()
            .unwrap_or_default();
        if self.marketplace_offer(txid) {
            tags.insert(MarketplaceTag::Offer);
        }
        if let Some(offer) = self.marketplace_publication(txid)
            && self
                .transaction(&offer.funding)
                .is_some_and(|tx| marketplace_funding_shape(&tx.raw_json))
        {
            tags.insert(MarketplaceTag::OfferProof);
        }
        if let Some(tx) = self.transaction(txid) {
            match self.marketplace_execution(&tx.raw_json) {
                Some(marketplace_trace::Spend::Taken) => {
                    tags.insert(MarketplaceTag::OfferTaken);
                }
                Some(marketplace_trace::Spend::Closed { .. }) => {
                    tags.insert(MarketplaceTag::OfferClosed);
                }
                None if marketplace_trace::recovery::recover(&tx.raw_json, self).is_some() => {
                    tags.insert(MarketplaceTag::OfferReleased);
                }
                None => {}
            }
        }
        tags.into_iter()
    }
}

fn marketplace_funding_shape(raw: &Value) -> bool {
    transaction_output(raw, 0).is_some_and(|output| {
        let script = &output["scriptPubKey"];
        (script["identityprimary"].is_object() || script["commitmenthash"].is_object())
            && marketplace_trace::native_amount(output)
                .is_some_and(|amount| amount >= marketplace_trace::OFFER_FEE)
    })
}

fn transaction_output(raw: &Value, index: usize) -> Option<&Value> {
    raw.get("vout").and_then(Value::as_array)?.get(index)
}

fn extract_op_return_payload(script_hex: &str) -> Option<Vec<u8>> {
    let script = decode_hex_bytes(script_hex)?;
    if script.first() != Some(&0x6a) {
        return None;
    }
    let (&opcode, remainder) = script.get(1..).and_then(|value| value.split_first())?;
    let (header, payload_offset): (usize, usize) = match opcode {
        0x01..=0x4b => (usize::from(opcode), 0),
        0x4c => (usize::from(*remainder.first().unwrap_or(&0)), 1),
        0x4d if remainder.len() >= 2 => (
            usize::from(u16::from_le_bytes([remainder[0], remainder[1]])),
            2,
        ),
        _ => return None,
    };
    let payload = remainder.get(payload_offset..payload_offset.saturating_add(header))?;
    Some(payload.to_vec())
}

fn stakeguard_enforcement(raw: &Value, store: &ObjectStore) -> bool {
    contains_stakeguard_cheat_evidence(raw)
        && raw["vin"].as_array().into_iter().flatten().any(|input| {
            resolved_prevout(input, store)
                .and_then(|output| output.get("scriptPubKey"))
                .is_some_and(|script| script.get("stakeguard").is_some())
        })
}

fn contains_stakeguard_cheat_evidence(raw: &Value) -> bool {
    let Some(script_hex) = raw
        .get("vout")
        .and_then(Value::as_array)
        .and_then(|outputs| outputs.last())
        .and_then(|output| output.get("scriptPubKey"))
        .and_then(|script| script.get("hex"))
        .and_then(Value::as_str)
    else {
        return false;
    };
    let Some(inner_script) = extract_op_return_payload(script_hex) else {
        return false;
    };
    inner_script.first() == Some(&0x53)
        && inner_script
            .get(1)
            .is_some_and(|opcode| matches!(opcode, 0x01..=0x4e))
}

fn decode_hex_bytes(value: &str) -> Option<Vec<u8>> {
    let (pairs, remainder) = value.as_bytes().as_chunks::<2>();
    if !remainder.is_empty() {
        return None;
    }
    pairs
        .iter()
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16)?;
            let low = (pair[1] as char).to_digit(16)?;
            Some(((high << 4) | low) as u8)
        })
        .collect()
}

struct ChainSession {
    config: Config,
    state: AppState,
    rx: UnboundedReceiver<AppEvent>,
    tx: UnboundedSender<AppEvent>,
}

pub fn run(
    runtime: tokio::runtime::Runtime,
    cache: Cache,
    start_object: Option<String>,
    mut registry: crate::sources::Registry,
) -> Result<()> {
    let config = registry.configs[0].clone();
    for source in &registry.configs {
        let endpoint = format!("http://{}:{}", source.daemon.host, source.daemon.port);
        cache.register_source(
            &source.daemon_cache_key(),
            &source.chain_cache_key(),
            &endpoint,
        )?;
        for api in &source.daemon.api_pool.endpoints {
            cache.register_source(
                &crate::rpc::source_id("public-api", &api.url),
                &source.chain_cache_key(),
                &api.url,
            )?;
        }
    }
    let runtime_context = runtime.enter();
    let route = classify_start_object(start_object.as_deref());
    let mut sessions = vec![create_session(
        config.clone(),
        ObjectStore::default(),
        Screen::Dashboard,
        route_label(&route),
    )];
    if let Some(query) = start_object {
        sessions[0].state.search.query = query;
        sessions[0].state.search.active = true;
        let session = &mut sessions[0];
        handle_search_key(
            &mut session.state,
            &session.config,
            &session.tx,
            KeyCode::Enter,
        );
    }
    for connection in registry.configs.iter().skip(1) {
        sessions.push(create_session(
            connection.clone(),
            ObjectStore::default(),
            Screen::Dashboard,
            connection.chain.name.clone(),
        ));
    }
    sessions[0]
        .config
        .daemon
        .control
        .visible
        .store(true, std::sync::atomic::Ordering::Release);
    let theme = Theme::desert(config.emoji);
    let terminal_settings = job_control::TerminalSettings::capture();
    let mut terminal = setup_terminal()?;
    let job_control = terminal_settings.and_then(|settings| settings.start().ok());
    let result = run_loop(
        &mut terminal,
        &theme,
        &mut sessions,
        &mut registry,
        job_control.as_ref(),
    );
    for session in &sessions {
        session
            .config
            .daemon
            .control
            .set(crate::sources::ConnectionState::Disabled);
    }
    // The runtime joins running blocking work before the final cache handle
    // closes its SQLite worker. Keep the alternate screen through both waits.
    drop(runtime_context);
    drop(runtime);
    let cache_notice = if result.is_ok() {
        (|| {
            if job_control
                .as_ref()
                .is_some_and(job_control::JobControl::take_resume)
            {
                resume_terminal(&mut terminal, false)?;
            }
            draw_shutdown(&mut terminal, &theme, "Closing the local cache…")
        })()
    } else {
        Ok(())
    };
    drop(cache);
    let restored = if job_control
        .as_ref()
        .is_some_and(job_control::JobControl::take_resume)
    {
        // The worker already left the alternate screen for the shell.
        disable_raw_mode().map_err(Into::into)
    } else {
        restore_terminal(&mut terminal)
    };
    result?;
    restored?;
    cache_notice
}

fn create_session(
    mut config: Config,
    mut store: ObjectStore,
    screen: Screen,
    route_label: String,
) -> ChainSession {
    let (tx, rx) = unbounded_channel();
    let (observation_tx, mut observation_rx) = unbounded_channel::<RpcObservation>();
    config.daemon.observations = Some(observation_tx);
    store.workspace_system = config.chain.system_id.get().cloned();
    store.chain.reconciled = Some(Default::default());
    store.chain.reconciling = true;
    let event_tx = tx.clone();
    tokio::spawn(async move {
        let mut lookahead = None;
        loop {
            let mut snapshot = match lookahead.take() {
                Some(observation) => observation,
                None => match observation_rx.recv().await {
                    Some(observation) => observation,
                    None => break,
                },
            };
            // Keep ordering boundaries intact. Only consecutive scalar
            // snapshots from the same source and branch can be collapsed.
            for _ in 0..APP_EVENT_BATCH_LIMIT - 1 {
                let Ok(next) = observation_rx.try_recv() else {
                    break;
                };
                if snapshot.same_scalar_snapshot(&next) {
                    if next.order >= snapshot.order {
                        snapshot = next;
                    }
                } else {
                    lookahead = Some(next);
                    break;
                }
            }
            if event_tx.send(AppEvent::Observation(snapshot)).is_err() {
                break;
            }
        }
    });
    let cache_path = config.cache_path.clone();
    let system = config.chain_cache_key();
    let event_tx = tx.clone();
    tokio::task::spawn_blocking(move || {
        if let Ok(cache) = Cache::open(&cache_path)
            && let Ok(definitions) = cache.currency_definitions(&system)
        {
            for (currency_id, name, definition_json) in definitions {
                if event_tx
                    .send(AppEvent::CurrencyDefinitions {
                        requested: Vec::new(),
                        records: vec![CurrencyDefinitionRecord {
                            currency_id,
                            name,
                            definition_json,
                        }],
                    })
                    .is_err()
                {
                    break;
                }
            }
        }
    });
    spawn_dashboard_poller(config.clone(), tx.clone());
    let geoip_directory = geoip::database_directory(&config.cache_path);
    let geoip_status = geoip::status(&geoip_directory);
    let geoip_database = GeoIpDatabase::open(&geoip_directory).ok().map(Arc::new);
    let map_enabled = geoip_database.is_some();
    let native_currency = native_currency_name(&config, &store);
    let state = AppState {
        route_label,
        tip: ChainTipStatus {
            label: "polling daemon".to_string(),
            healthy: false,
            height: None,
            longest_chain: None,
            last_block_time: None,
            connections: None,
        },
        store,
        dashboard: DashboardState::default(),
        mempool: MempoolState::default(),
        marketplace: MarketplaceState {
            subject: native_currency,
            ..MarketplaceState::default()
        },
        screen,
        screen_stack: Vec::new(),
        loading_blocks: BTreeSet::new(),
        loading_transactions: BTreeSet::new(),
        transaction_retry_after: HashMap::new(),
        transaction_verified_tip: HashMap::new(),
        loading_addresses: BTreeSet::new(),
        loading_address_histories: BTreeSet::new(),
        loading_address_utxos: BTreeSet::new(),
        loading_address_balances: BTreeSet::new(),
        address_errors: HashMap::new(),
        address_retry_after: HashMap::new(),
        address_history_errors: HashMap::new(),
        address_history_retry_after: HashMap::new(),
        address_utxo_errors: HashMap::new(),
        address_utxo_retry_after: HashMap::new(),
        address_balance_errors: HashMap::new(),
        address_balance_retry_after: HashMap::new(),
        loading_outpoints: HashSet::new(),
        loading_identities: HashSet::new(),
        identity_load_attempted: HashSet::new(),
        loading_identity_names: HashSet::new(),
        identity_name_attempted: HashSet::new(),
        loading_address_identities: HashSet::new(),
        address_identities_attempted: HashSet::new(),
        loading_currencies: HashSet::new(),
        currency_load_attempted: HashSet::new(),
        unmask_identity_addresses: false,
        notice: None,
        clipboard_busy: false,
        save_prompt: None,
        cache_clear_prompt: None,
        cache_size_bytes: cache_disk_usage(&config.cache_path).ok(),
        search: SearchState::default(),
        auxiliary: None,
        help_return: None,
        peers: PeerState {
            records: Vec::new(),
            controls: peer_controls::State::default(),
            selected: 0,
            loading: false,
            last_refresh: None,
            map_enabled,
            geoip_directory,
            geoip_status,
            geoip_database,
            geoip_downloading: false,
        },
        log_feed: LogFeedState {
            path: log_feed::daemon_log_path(config.daemon.config_file.as_deref()),
            offset: None,
            partial: String::new(),
            lines: VecDeque::new(),
            loading: false,
            last_refresh: None,
            error: None,
        },
    };
    ChainSession {
        config,
        state,
        rx,
        tx,
    }
}

fn block_screen(target: BlockTarget) -> Screen {
    Screen::Block {
        target,
        selected_tx: 0,
        mode: ViewMode::Overview,
        json: JsonState::new(),
    }
}

fn transaction_screen(txid: Txid) -> Screen {
    Screen::Transaction {
        txid,
        selected_flow: 0,
        mode: ViewMode::Overview,
        json: JsonState::new(),
        notarization: None,
    }
}

fn address_screen(address: String) -> Screen {
    Screen::Address {
        address,
        selected_tx: 0,
        selected_identity: None,
    }
}

fn identity_screen(query: String, height: Option<u64>) -> Screen {
    Screen::Identity {
        target: IdentityTarget { query, height },
        selected_tx: 0,
        selected_primary_address: None,
        historical_height_input: None,
        mode: ViewMode::Overview,
        json: JsonState {
            initial_focus: Some("$.identity".into()),
            ..JsonState::new()
        },
    }
}

fn insert_address_load(store: &mut ObjectStore, result: AddressLoadResult) {
    let address = result.address;
    let mut viewed_output_states = Vec::new();
    for txid in &address.transaction_ids {
        let Some(outputs) = store
            .transaction(txid)
            .and_then(|transaction| transaction.raw_json.get("vout"))
            .and_then(Value::as_array)
        else {
            continue;
        };
        for (fallback_index, output) in outputs.iter().enumerate() {
            if !addresses_in(output)
                .iter()
                .any(|candidate| candidate == &address.address)
            {
                continue;
            }
            let index = output
                .get("n")
                .and_then(Value::as_u64)
                .unwrap_or(fallback_index as u64);
            let outpoint = OutPoint {
                txid: txid.clone(),
                index,
            };
            viewed_output_states.push((
                outpoint.clone(),
                address.unspent_outpoints.contains(&outpoint),
            ));
        }
    }
    store.output_unspent.extend(viewed_output_states);
    store.insert_address(address);
}

fn route_label(route: &ObjectRoute) -> String {
    match route {
        ObjectRoute::BlockHeight(height) => block_id_label(&BlockId::Height(*height)),
        ObjectRoute::Hash(hash) => block_id_label(&BlockId::Hash(hash.clone())),
        _ => route.label(),
    }
}

fn block_id_label(block: &BlockId) -> String {
    match block {
        BlockId::Height(height) => format!("requested block height {height}"),
        BlockId::Hash(hash) => format!("requested block hash {hash}"),
    }
}

fn spawn_dashboard_poller(mut config: Config, tx: UnboundedSender<AppEvent>) {
    // Automatic dashboard refresh never polls an API, including block hydration.
    config.daemon.api_pool = Arc::default();
    config.daemon.background = true;
    tokio::spawn(async move {
        let mut failures = 0u32;
        let mut reconciler = None;
        let mut published: Option<Arc<crate::chain_state::PreparedBranch>> = None;
        loop {
            if !config.daemon.control.polling() {
                tokio::time::sleep(Duration::from_millis(250)).await;
                if tx.is_closed() {
                    break;
                }
                continue;
            }
            let started = Instant::now();
            let generation = config.daemon.control.generation();
            if reconciler.is_none() {
                reconciler = crate::reconciliation::Reconciler::new(config.clone())
                    .await
                    .ok();
            }
            let reconciled = match &mut reconciler {
                Some(r) => r
                    .reconcile(|| {
                        use crate::sources::ChainCheck;
                        let control = config.daemon.control.clone();
                        control.record_chain_check(generation, ChainCheck::Checking);
                        let _ = tx.send(AppEvent::Guarded {
                            control,
                            generation,
                            event: Box::new(AppEvent::ReconciliationStarted),
                        });
                    })
                    .await
                    .map_err(|error| error.to_string()),
                None => Err("Could not initialize chain reconciliation".into()),
            };
            config.daemon.control.record_chain_check(
                generation,
                crate::sources::ChainCheck::Finished {
                    at: std::time::Instant::now(),
                    error: reconciled.as_ref().err().cloned(),
                },
            );
            let reconciled = match reconciled {
                Ok(change)
                    if published.as_ref().is_some_and(|branch| {
                        change.attached.is_empty()
                            && change.detached.is_empty()
                            && change.state.revision == branch.revision
                            && change.state.tip() == branch.tip()
                    }) =>
                {
                    Ok(published.as_ref().unwrap().clone())
                }
                Ok(change) => {
                    match tokio::task::spawn_blocking(move || {
                        Arc::new(crate::chain_state::PreparedBranch::from(change))
                    })
                    .await
                    {
                        Ok(branch) => {
                            published = Some(branch.clone());
                            Ok(branch)
                        }
                        Err(error) => Err(error.to_string()),
                    }
                }
                Err(error) => Err(error),
            };
            let _ = tx.send(AppEvent::Guarded {
                control: config.daemon.control.clone(),
                generation,
                event: Box::new(AppEvent::Reconciled(reconciled)),
            });
            let result = load_dashboard_snapshot(&config)
                .await
                .map_err(|error| error.to_string());

            failures = if result.is_ok() {
                0
            } else {
                failures.saturating_add(1)
            };
            if tx
                .send(AppEvent::Guarded {
                    control: config.daemon.control.clone(),
                    generation,
                    event: Box::new(AppEvent::DashboardRefresh(result)),
                })
                .is_err()
            {
                break;
            }

            let interval = Duration::from_secs(
                config
                    .poll_interval_seconds
                    .saturating_mul(1u64 << failures.min(6))
                    .min(120),
            );
            let elapsed = started.elapsed();
            if elapsed < interval {
                tokio::time::sleep(interval - elapsed).await;
            }
        }
    });
}

fn revision_sender(config: &Config, tx: UnboundedSender<AppEvent>) -> UnboundedSender<AppEvent> {
    let control = config.daemon.control.clone();
    let epoch = control
        .branch_epoch
        .load(std::sync::atomic::Ordering::Acquire);
    let generation = control.generation();
    let (sender, mut receiver) = unbounded_channel();
    tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            if tx
                .send(AppEvent::RevisionGuarded {
                    control: control.clone(),
                    epoch,
                    generation,
                    event: Box::new(event),
                })
                .is_err()
            {
                break;
            }
        }
    });
    sender
}

fn invalidate_reorg_views(state: &mut AppState) {
    state.store.marketplace_searches.clear();
    state.store.registration_payments.clear();
    state.store.token_control.clear();
    state
        .store
        .stale_identities
        .extend(state.store.identity_heights.keys().cloned());
    state
        .store
        .stale_addresses
        .extend(state.store.addresses_by_address.keys().cloned());
    state.address_retry_after.clear();
    state.address_history_retry_after.clear();
    state.address_balance_retry_after.clear();
    state.address_utxo_retry_after.clear();
    state.store.identities_by_primary_address.clear();
    state.transaction_verified_tip.clear();
    state.transaction_retry_after.clear();
    state.loading_transactions.clear();
    state.loading_blocks.clear();
    state.loading_addresses.clear();
    state.loading_address_histories.clear();
    state.loading_address_balances.clear();
    state.loading_address_utxos.clear();
    state.loading_outpoints.clear();
    state.loading_identities.clear();
    state.loading_identity_names.clear();
    state.loading_address_identities.clear();
    state.loading_currencies.clear();
    state.identity_load_attempted.clear();
    state.identity_name_attempted.clear();
    state.address_identities_attempted.clear();
    state.currency_load_attempted.clear();
    state.store.output_unspent.clear();
    state.store.output_status_attempted.clear();
    state.store.identity_history_complete.clear();
    state.store.identity_evidence.clear();
    state.store.loading_identity_evidence.clear();
    state.store.loading_historical_identity_evidence.clear();
    state.store.identity_offers.clear();
    state.store.marketplace_tags_by_txid.clear();
    for address in state.store.addresses_by_address.values_mut() {
        address.balance_response = None;
        address.utxos_complete = false;
        address.history_complete = false;
        address.unspent_outpoints.clear();
    }
    state.marketplace.loading = false;
    state.marketplace.refresh_requested = true;
    state.marketplace.error = Some("Branch changed; awaiting refreshed offers".into());
    state.mempool.loading = false;
    state.mempool.last_refresh = None;
    state.dashboard.loading_older_since = None;
    let selected_hash = state.dashboard.selected_block_hash().cloned();
    state
        .dashboard
        .recent_blocks
        .retain(|id| state.store.chain.confirmations(id).is_some());
    state.dashboard.restore_block_selection(selected_hash);
}

fn spawn_dashboard_page_loader(
    config: Config,
    tx: UnboundedSender<AppEvent>,
    newest_height: u64,
    started: Instant,
) {
    let tx = revision_sender(&config, tx);
    tokio::spawn(async move {
        let result = load_dashboard_page(&config, newest_height)
            .await
            .map_err(|error| error.to_string());
        let _ = tx.send(AppEvent::DashboardPage { started, result });
    });
}

fn spawn_block_loader(
    config: Config,
    tx: UnboundedSender<AppEvent>,
    target: BlockTarget,
    cached_block: Option<BlockRecord>,
) {
    let tx = revision_sender(&config, tx);
    let key = block_load_key(&target);
    tokio::spawn(async move {
        let result = load_block_detail(&config, target, cached_block)
            .await
            .map_err(|error| error.to_string());
        let _ = tx.send(AppEvent::BlockDetail { key, result });
    });
}

fn spawn_transaction_loader(config: Config, tx: UnboundedSender<AppEvent>, requested: Vec<Txid>) {
    let tx = revision_sender(&config, tx);
    tokio::spawn(async move {
        for requested in requested.chunks(TRANSACTION_LOAD_BATCH_SIZE) {
            let requested = requested.to_vec();
            let (records, failed) = load_transactions_bounded(&config, &requested).await;
            for record in records {
                if tx
                    .send(AppEvent::Transactions {
                        requested: vec![record.summary.txid.clone()],
                        records: vec![record],
                        failed: Vec::new(),
                    })
                    .is_err()
                {
                    return;
                }
            }
            if !failed.is_empty()
                && tx
                    .send(AppEvent::Transactions {
                        requested: failed.clone(),
                        records: Vec::new(),
                        failed,
                    })
                    .is_err()
            {
                return;
            }
        }
    });
}

fn spawn_currency_loader(config: Config, tx: UnboundedSender<AppEvent>, requested: Vec<String>) {
    let tx = revision_sender(&config, tx);
    tokio::spawn(async move {
        let Ok(client) = RpcClient::for_config(&config).map(RpcClient::noninteractive) else {
            let _ = tx.send(AppEvent::CurrencyDefinitions {
                requested,
                records: Vec::new(),
            });
            return;
        };
        for currency_id in requested {
            let mut records = Vec::new();
            if client.get_currency(&currency_id).await.is_ok()
                && let Ok(cache) = Cache::open(&config.cache_path)
                && let Ok(Some((currency_id, name, definition_json))) =
                    cache.currency_definition(&config.chain_cache_key(), &currency_id)
            {
                records.push(CurrencyDefinitionRecord {
                    currency_id,
                    name,
                    definition_json,
                });
            }
            if tx
                .send(AppEvent::CurrencyDefinitions {
                    requested: vec![currency_id],
                    records,
                })
                .is_err()
            {
                break;
            }
        }
    });
}

fn spawn_address_loader(config: Config, tx: UnboundedSender<AppEvent>, address: String) {
    let tx = revision_sender(&config, tx);
    tokio::spawn(async move {
        let result = load_address_detail(&config, &address)
            .await
            .map_err(|error| error.to_string());
        let _ = tx.send(AppEvent::AddressDetail { address, result });
    });
}

fn spawn_address_history_loader(config: Config, tx: UnboundedSender<AppEvent>, address: String) {
    let tx = revision_sender(&config, tx);
    tokio::spawn(async move {
        let result = load_address_history(&config, &address)
            .await
            .map_err(|error| error.to_string());
        let _ = tx.send(AppEvent::AddressHistory { address, result });
    });
}

fn spawn_address_utxo_loader(config: Config, tx: UnboundedSender<AppEvent>, address: String) {
    let tx = revision_sender(&config, tx);
    tokio::spawn(async move {
        let result = load_address_utxos(&config, &address)
            .await
            .map_err(|error| error.to_string());
        let _ = tx.send(AppEvent::AddressUtxos { address, result });
    });
}

fn spawn_address_balance_loader(config: Config, tx: UnboundedSender<AppEvent>, address: String) {
    let tx = revision_sender(&config, tx);
    tokio::spawn(async move {
        let result = load_address_balance(&config, &address)
            .await
            .map_err(|error| error.to_string());
        let _ = tx.send(AppEvent::AddressBalance { address, result });
    });
}

fn spawn_cache_clear(path: PathBuf, tx: UnboundedSender<AppEvent>) {
    tokio::spawn(async move {
        let result = tokio::task::spawn_blocking(move || Cache::clear_at(&path))
            .await
            .map_err(|error| format!("cache clear task failed: {error}"))
            .and_then(|result| result.map_err(|error| error.to_string()));
        let _ = tx.send(AppEvent::CacheCleared(result));
    });
}

fn spawn_address_identities_loader(config: Config, tx: UnboundedSender<AppEvent>, address: String) {
    let tx = revision_sender(&config, tx);
    tokio::spawn(async move {
        let result = load_address_identities(&config, &address)
            .await
            .map_err(|error| error.to_string());
        let _ = tx.send(AppEvent::AddressIdentities { address, result });
    });
}

fn spawn_identity_loader(config: Config, tx: UnboundedSender<AppEvent>, query: String) {
    let tx = revision_sender(&config, tx);
    tokio::spawn(async move {
        let result = load_identity_detail(&config, &query)
            .await
            .map_err(|error| error.to_string());
        let _ = tx.send(AppEvent::IdentityDetail { query, result });
    });
}

fn spawn_identity_evidence_loader(
    config: Config,
    tx: UnboundedSender<AppEvent>,
    address: String,
    height: u64,
    versions: Vec<IdentityVersionRecord>,
) {
    let tx = revision_sender(&config, tx);
    let branch_epoch = config.daemon.control.branch_epoch.load(Ordering::Acquire);
    tokio::spawn(async move {
        let mut retained = None;
        let version_txid = versions
            .first()
            .and_then(|version| version.raw_json["txid"].as_str())
            .map(str::to_string);
        if let Some(version_txid) = version_txid.as_ref()
            && let Ok(cache) = Cache::open(&config.cache_path)
            && let Ok(Some(record)) = cache
                .identity_evidence(
                    config.chain_cache_key(),
                    address.clone(),
                    version_txid.clone(),
                    height,
                )
                .await
        {
            let cached_report = identity_evidence::from_record(record);
            let _ = tx.send(AppEvent::IdentityEvidence {
                address: address.clone(),
                height,
                cached: true,
                result: Ok(cached_report.clone()),
            });
            retained = Some(cached_report);
        }
        let mut result = identity_evidence::load(&config, versions)
            .await
            .map_err(|error| error.to_string());
        if let Ok(report) = &mut result
            && config.daemon.control.branch_epoch.load(Ordering::Acquire) == branch_epoch
        {
            if let Some(retained) = &retained {
                identity_evidence::merge_historical(report, retained);
            }
            let record = version_txid.as_ref().and_then(|txid| {
                identity_evidence::to_record(
                    &config.chain_cache_key(),
                    &address,
                    txid,
                    height,
                    report,
                )
            });
            match record {
                Some(record) => {
                    let stored = async {
                        Cache::open(&config.cache_path)?
                            .record_identity_evidence(record)
                            .await
                    }
                    .await;
                    if let Err(error) = stored {
                        report
                            .notes
                            .push(format!("Evidence storage unavailable: {error}"));
                    }
                }
                None => report.notes.push(
                    "Evidence not retained: current identity transaction lacks an anchored source"
                        .into(),
                ),
            }
        }
        let _ = tx.send(AppEvent::IdentityEvidence {
            address,
            height,
            cached: false,
            result,
        });
    });
}

fn spawn_historical_identity_evidence_loader(
    config: Config,
    tx: UnboundedSender<AppEvent>,
    address: String,
    height: u64,
    versions: Vec<IdentityVersionRecord>,
    mut report: identity_evidence::IdentityEvidence,
    to_height: u64,
) {
    let tx = revision_sender(&config, tx);
    let branch_epoch = config.daemon.control.branch_epoch.load(Ordering::Acquire);
    tokio::spawn(async move {
        let from_height = to_height.saturating_sub(999);
        let mut result = identity_evidence::expand_historical(
            &config,
            &mut report,
            &versions,
            from_height,
            to_height,
        )
        .await
        .map(|()| report)
        .map_err(|error| error.to_string());
        if let Ok(report) = &mut result
            && config.daemon.control.branch_epoch.load(Ordering::Acquire) == branch_epoch
        {
            let record = versions
                .first()
                .and_then(|version| version.raw_json["txid"].as_str())
                .and_then(|txid| {
                    identity_evidence::to_record(
                        &config.chain_cache_key(),
                        &address,
                        txid,
                        height,
                        report,
                    )
                });
            if let Some(record) = record {
                let stored = async {
                    Cache::open(&config.cache_path)?
                        .record_identity_evidence(record)
                        .await
                }
                .await;
                if let Err(error) = stored {
                    report
                        .notes
                        .push(format!("Historical evidence storage unavailable: {error}"));
                }
            }
        }
        let _ = tx.send(AppEvent::IdentityHistoricalEvidence {
            address,
            height,
            result,
        });
    });
}

fn spawn_identity_name_loader(
    config: Config,
    tx: UnboundedSender<AppEvent>,
    requested: Vec<String>,
) {
    let tx = revision_sender(&config, tx);
    tokio::spawn(async move {
        let mut resolved = HashMap::new();
        if let Ok(client) = RpcClient::for_config(&config).map(RpcClient::noninteractive) {
            for address in &requested {
                let Ok((identity, _)) =
                    load_identity_version_with_client(&config, &client, address, None, false).await
                else {
                    continue;
                };
                resolved.insert(identity.identity_address, identity.fully_qualified_name);
            }
        }
        let _ = tx.send(AppEvent::IdentityNames {
            requested,
            resolved,
        });
    });
}

fn spawn_output_state_loader(
    config: Config,
    tx: UnboundedSender<AppEvent>,
    requested: Vec<OutPoint>,
) {
    let tx = revision_sender(&config, tx);
    tokio::spawn(async move {
        let Ok(client) = RpcClient::for_config(&config).map(RpcClient::noninteractive) else {
            let _ = tx.send(AppEvent::OutputStates {
                requested,
                states: Vec::new(),
            });
            return;
        };
        let mut states = Vec::new();
        for outpoint in &requested {
            let Ok(output) = client.get_tx_out(&outpoint.txid, outpoint.index).await else {
                continue;
            };
            states.push((outpoint.clone(), output.result.is_some()));
        }
        let _ = tx.send(AppEvent::OutputStates { requested, states });
    });
}

fn spawn_search_loader(
    config: Config,
    tx: UnboundedSender<AppEvent>,
    request_id: u64,
    route: ObjectRoute,
) -> tokio::task::AbortHandle {
    let tx = revision_sender(&config, tx);
    tokio::spawn(async move {
        let result = resolve_search(&config, route)
            .await
            .map_err(|error| error.to_string());
        let _ = tx.send(AppEvent::SearchResolved { request_id, result });
    })
    .abort_handle()
}

fn spawn_peer_loader(config: Config, tx: UnboundedSender<AppEvent>, snapshot: bool) {
    let generation = config.daemon.control.generation();
    tokio::spawn(async move {
        let result = peer_controls::refresh(&config, snapshot).await;
        let _ = tx.send(AppEvent::Guarded {
            control: config.daemon.control.clone(),
            generation,
            event: Box::new(AppEvent::Peers(result)),
        });
    });
}

fn spawn_mempool_loader(config: Config, tx: UnboundedSender<AppEvent>, full: bool) {
    let tx = revision_sender(&config, tx);
    let generation = config.daemon.control.generation();
    tokio::spawn(async move {
        let result = async {
            let client = RpcClient::for_config(&config)?;
            let output = client.get_raw_mempool_verbose(full).await?;
            Ok(parse_mempool_snapshot(output.result))
        }
        .await
        .map_err(|error: anyhow::Error| error.to_string());
        let _ = tx.send(AppEvent::Guarded {
            control: config.daemon.control.clone(),
            generation,
            event: Box::new(AppEvent::MempoolRefresh(result)),
        });
    });
}

fn spawn_marketplace_loader(
    config: Config,
    tx: UnboundedSender<AppEvent>,
    subject: String,
    tip: BlockHash,
) {
    let tx = revision_sender(&config, tx);
    tokio::spawn(async move {
        let result = async {
            let client = RpcClient::for_config(&config)?;
            let output = client.get_offers(&subject, true).await?;
            Ok(MarketplaceSnapshot {
                subject: subject.clone(),
                offers: parse_marketplace_offers(&output.result),
            })
        }
        .await
        .map_err(|error: anyhow::Error| error.to_string());
        let _ = tx.send(AppEvent::MarketplaceRefresh { tip, result });
    });
}

fn parse_marketplace_offers(value: &Value) -> Vec<MarketplaceOffer> {
    fn visit(value: &Value, category: &str, offers: &mut Vec<MarketplaceOffer>) {
        match value {
            Value::Array(values) => {
                for value in values {
                    visit(value, category, offers);
                }
            }
            Value::Object(fields) => {
                if let Some(details) = fields.get("offer").and_then(Value::as_object)
                    && let Some(txid) = details.get("txid").and_then(Value::as_str)
                    && let (Some(offered), Some(accepted)) =
                        (details.get("offer"), details.get("accept"))
                {
                    offers.push(MarketplaceOffer {
                        txid: txid.to_string(),
                        category: category.to_string(),
                        subject_id: fields
                            .get("identityid")
                            .or_else(|| fields.get("currencyid"))
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        asking_price: marketplace_asking_price(accepted),
                        offered: offered.clone(),
                        accepted: accepted.clone(),
                        expiry_height: details
                            .get("blockexpiry")
                            .and_then(Value::as_u64)
                            .unwrap_or_default(),
                    });
                    return;
                }
                for (key, value) in fields {
                    visit(value, key, offers);
                }
            }
            _ => {}
        }
    }

    let mut offers = Vec::new();
    visit(value, "offers", &mut offers);
    offers.sort_by(|left, right| {
        left.expiry_height
            .cmp(&right.expiry_height)
            .then_with(|| left.txid.cmp(&right.txid))
    });
    offers.dedup_by(|left, right| left.txid == right.txid);
    offers
}

fn parse_mempool_snapshot(values: HashMap<String, Value>) -> MempoolSnapshot {
    let mut total_bytes = 0_u64;
    let mut previews = HashMap::new();
    let mut entries = values
        .into_iter()
        .map(|(txid, mut value)| {
            if let Some(raw) = value
                .as_object_mut()
                .and_then(|fields| fields.remove("fulltx"))
                && raw["txid"].as_str() == Some(txid.as_str())
                && raw["vin"].is_array()
                && raw["vout"].is_array()
            {
                previews.insert(
                    txid.clone(),
                    TransactionSummary {
                        txid: txid.clone(),
                        raw: Some(raw),
                    },
                );
            }
            let size = value
                .get("size")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            total_bytes = total_bytes.saturating_add(size);
            MempoolEntry {
                txid,
                size,
                fee: value.get("fee").and_then(Value::as_f64),
                time: value
                    .get("time")
                    .and_then(Value::as_u64)
                    .unwrap_or_default(),
                height: value.get("height").and_then(Value::as_u64),
                depends: value
                    .get("depends")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect(),
            }
        })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        right
            .time
            .cmp(&left.time)
            .then_with(|| left.txid.cmp(&right.txid))
    });
    MempoolSnapshot {
        entries,
        total_bytes,
        previews,
    }
}

fn spawn_geoip_download(directory: PathBuf, tx: UnboundedSender<AppEvent>) {
    tokio::spawn(async move {
        let result = geoip::download(directory)
            .await
            .map_err(|error| error.to_string());
        let _ = tx.send(AppEvent::GeoIpDownloaded(result));
    });
}

fn spawn_log_reader(path: PathBuf, offset: Option<u64>, tx: UnboundedSender<AppEvent>) {
    tokio::spawn(async move {
        let result = tokio::task::spawn_blocking(move || log_feed::read_chunk(&path, offset))
            .await
            .map_err(|error| error.to_string())
            .and_then(|result| result.map_err(|error| error.to_string()));
        let _ = tx.send(AppEvent::LogChunk(result));
    });
}

async fn resolve_search(config: &Config, route: ObjectRoute) -> Result<SearchResolution> {
    if let ObjectRoute::Hash(hash) = &route {
        let cache = Cache::open(&config.cache_path)?;
        let system = config.chain_cache_key();
        let target = hash.clone();
        let cached_block = tokio::task::spawn_blocking(move || -> Result<Option<Value>> {
            let params = json!([target, 1]).to_string();
            if cache
                .bookmark_response(&system, "getrawtransaction", &params, &target)?
                .is_some()
            {
                return Ok(None);
            }
            Ok(cache
                .bookmark_response(&system, "getblock", &params, &target)?
                .map(|(_, raw)| raw))
        })
        .await??;
        if let Some(raw_json) = cached_block {
            let block = BlockRecord {
                summary: serde_json::from_value(raw_json.clone())?,
                prepared_transactions: HashMap::new(),
                raw_json,
                detail_loaded: false,
            };
            return Ok(SearchResolution::Block(
                load_block_detail(config, BlockTarget::Hash(hash.clone()), Some(block)).await?,
            ));
        }
    }
    match route {
        ObjectRoute::Estimate(query) => {
            let target = crate::block_forecast::parse_target(&query)?;
            let c = crate::block_forecast::calibrate(config).await?;
            Ok(SearchResolution::Estimate(
                crate::block_forecast::Estimate::new(c, target)?,
            ))
        }
        ObjectRoute::BlockTime(query) => {
            let time = block_time::parse(&query)?.timestamp();
            let quiet = config.noninteractive();
            let (source, tip, info) = RpcTimeSource::open(&quiet).await?;
            resolve_search_time(&quiet, source, tip, &info, time).await
        }
        ObjectRoute::BlockHeight(height) => {
            let quiet = config.noninteractive();
            if let Ok((mut source, tip, info)) = RpcTimeSource::open(&quiet).await
                && height > tip.height
            {
                let genesis = source.block_at(0).await?;
                if height >= genesis.timestamp as u64 {
                    let time = block_time::parse(&height.to_string())?.timestamp();
                    return resolve_search_time(&quiet, source, tip, &info, time).await;
                }
                let c = crate::block_forecast::from_source(&mut source, &quiet, tip, &info).await?;
                return Ok(SearchResolution::Estimate(
                    crate::block_forecast::Estimate::new(
                        c,
                        crate::block_forecast::Target::Height(height),
                    )?,
                ));
            }
            Ok(SearchResolution::Block(
                load_block_detail(config, BlockTarget::Height(height), None).await?,
            ))
        }
        ObjectRoute::Identity(query) => {
            let client = RpcClient::for_config(config)?;
            let lookup =
                load_identity_version_with_client(config, &client, &query, None, false).await;
            match lookup {
                Ok((version, _)) => Ok(SearchResolution::Identity { query, version }),
                Err(error) if crate::rpc::identity_not_found(&error) => {
                    anyhow::bail!("Identity not found: {query}")
                }
                Err(error) => Err(error),
            }
        }
        ObjectRoute::Hash(hash) => {
            // A transaction miss is expected for a block hash. Exhaust local
            // interpretations before considering an API request.
            let mut local = config.clone();
            local.daemon.api_pool = Default::default();
            match resolve_hash(&local, &hash).await {
                Ok(result) => Ok(result),
                Err(error) if config.daemon.api_pool.endpoints.is_empty() => Err(error),
                Err(_) => resolve_hash(config, &hash).await,
            }
        }
        _ => anyhow::bail!("unsupported daemon search route"),
    }
}

async fn resolve_search_time(
    config: &Config,
    mut source: RpcTimeSource<'_>,
    tip: TimeBlock,
    info: &Value,
    time: i64,
) -> Result<SearchResolution> {
    if time > current_unix_timestamp() {
        let calibration =
            crate::block_forecast::from_source(&mut source, config, tip, info).await?;
        return Ok(SearchResolution::Estimate(
            crate::block_forecast::Estimate::new(
                calibration,
                crate::block_forecast::Target::Time(time),
            )?,
        ));
    }
    let found = block_time::find(&mut source, time, tip).await?;
    let block =
        load_block_detail(config, BlockTarget::Hash(found.block.hash.clone()), None).await?;
    Ok(SearchResolution::TimeBlock { block, found })
}

async fn resolve_hash(config: &Config, hash: &str) -> Result<SearchResolution> {
    match load_transaction(config, hash).await {
        Ok(transaction) => {
            let mut records = vec![transaction.clone()];
            let source_txids = source_transaction_ids(&transaction.raw_json);
            records.extend(load_transactions(config, &source_txids).await);
            Ok(SearchResolution::Transaction {
                txid: hash.to_string(),
                records,
            })
        }
        Err(error) if error.is::<crate::sources::ApiCancelled>() => Err(error),
        Err(transaction_error) => {
            load_block_detail(config, BlockTarget::Hash(hash.to_string()), None)
                .await
                .map(SearchResolution::Block)
                .with_context(|| {
                    format!("not found as a transaction ({transaction_error}) or block")
                })
        }
    }
}

async fn load_dashboard_snapshot(config: &Config) -> Result<DashboardSnapshot> {
    let client = RpcClient::for_config(config)?;
    let info = client.get_info().await.context("failed to call getinfo")?;
    if info.result.get("chainid").is_some() {
        config.bind_system(&info.result)?;
    }

    let newest = rpc_u64_field(&info.result, "blocks")
        .or_else(|| rpc_u64_field(&info.result, "height"))
        .or_else(|| rpc_u64_field(&info.result, "blockheight"));
    let newest = if let Some(newest) = newest {
        newest
    } else {
        let tip = client
            .get_block_count()
            .await
            .context("failed to call getblockcount after getinfo omitted block height")?;
        tip.result
    };
    let longest_chain = rpc_u64_field(&info.result, "longestchain");
    let connections = rpc_u64_field(&info.result, "connections");

    let mut blocks = Vec::new();
    let oldest = newest.saturating_sub(DASHBOARD_BLOCK_COUNT - 1);
    for height in (oldest..=newest).rev().filter(|_| {
        config
            .daemon
            .control
            .visible
            .load(std::sync::atomic::Ordering::Acquire)
    }) {
        let block = if height == newest {
            fetch_block_summary(config, BlockId::Height(height)).await
        } else {
            load_block_summary(config, BlockId::Height(height)).await
        };
        blocks.push(block.with_context(|| format!("failed to call getblock for height {height}"))?);
    }
    let last_block_time = rpc_u64_field(&info.result, "tiptime")
        .or_else(|| blocks.first().map(|block| block.summary.time));

    Ok(DashboardSnapshot {
        tip: ChainTipStatus {
            label: if info.source_id.starts_with("public-api:") {
                "API snapshot"
            } else {
                "tip loaded"
            }
            .to_string(),
            healthy: !info.source_id.starts_with("public-api:"),
            height: Some(newest),
            longest_chain,
            last_block_time,
            connections,
        },
        blocks,
    })
}

async fn load_dashboard_page(config: &Config, newest_height: u64) -> Result<Vec<BlockRecord>> {
    let config = &config.noninteractive();
    let oldest_height = newest_height.saturating_sub(DASHBOARD_BLOCK_COUNT - 1);
    let mut blocks = Vec::new();
    for height in (oldest_height..=newest_height).rev() {
        blocks.push(
            load_block_summary(config, BlockId::Height(height))
                .await
                .with_context(|| format!("failed to load dashboard block at height {height}"))?,
        );
    }
    Ok(blocks)
}

async fn load_block_summary(config: &Config, block: BlockId) -> Result<BlockRecord> {
    let params_json = block_params_json(&block, 1)?;
    if let Some(raw_json) = cached_local_rpc_result(config, "getblock", &params_json).await? {
        let summary = serde_json::from_value(raw_json.clone())
            .context("cached getblock result did not match the expected block shape")?;
        return Ok(BlockRecord {
            prepared_transactions: prepare_embedded_transactions(&summary),
            summary,
            raw_json,
            detail_loaded: false,
        });
    }

    fetch_block_summary(config, block).await
}

async fn fetch_block_summary(config: &Config, block: BlockId) -> Result<BlockRecord> {
    let client = RpcClient::for_config(config)?;
    let output = client.get_block(block, 1).await?;
    let raw_json = rpc_result_value(&output.response_json)?;
    Ok(BlockRecord {
        prepared_transactions: prepare_embedded_transactions(&output.result),
        summary: output.result,
        raw_json,
        detail_loaded: false,
    })
}

async fn load_block_detail(
    config: &Config,
    target: BlockTarget,
    cached_block: Option<BlockRecord>,
) -> Result<BlockRecord> {
    let mut block = match cached_block {
        Some(block) => block,
        None => load_block_summary(config, target.to_block_id()).await?,
    };
    enrich_block_transactions(config, &mut block).await;
    block.detail_loaded = true;
    Ok(block)
}

fn prepare_embedded_transactions(
    summary: &BlockSummary,
) -> HashMap<Txid, Arc<PreparedTransaction>> {
    summary
        .tx
        .iter()
        .filter_map(|tx| {
            Some((
                tx.txid.clone(),
                Arc::new(PreparedTransaction::new(tx, tx.raw.as_ref()?)),
            ))
        })
        .collect()
}

async fn enrich_block_transactions(config: &Config, block: &mut BlockRecord) {
    let config = &config.noninteractive();
    for transaction in &mut block.summary.tx {
        if let Some(raw) = transaction
            .raw
            .as_ref()
            .filter(|raw| has_transaction_size(raw))
        {
            block
                .prepared_transactions
                .entry(transaction.txid.clone())
                .or_insert_with(|| Arc::new(PreparedTransaction::new(transaction, raw)));
            continue;
        }

        let Ok(record) = load_transaction(config, &transaction.txid).await else {
            continue;
        };
        transaction.raw = Some(record.raw_json);
        if let Some(prepared) = record.prepared {
            block
                .prepared_transactions
                .insert(transaction.txid.clone(), prepared);
        }
    }
}

async fn load_transactions(config: &Config, txids: &[Txid]) -> Vec<TransactionRecord> {
    let config = &config.noninteractive();
    let mut records = Vec::new();
    for txid in txids {
        if let Ok(record) = load_transaction(config, txid).await {
            records.push(record);
        }
    }
    records
}

async fn load_transactions_bounded(
    config: &Config,
    txids: &[Txid],
) -> (Vec<TransactionRecord>, Vec<Txid>) {
    let config = &config.noninteractive();
    let mut pending = txids.iter().cloned().collect::<VecDeque<_>>();
    let mut tasks = JoinSet::new();
    let mut records = Vec::new();

    loop {
        while tasks.len() < TRANSACTION_LOAD_CONCURRENCY {
            let Some(txid) = pending.pop_front() else {
                break;
            };
            let config = config.clone();
            tasks.spawn(async move { load_transaction(&config, &txid).await });
        }
        let Some(result) = tasks.join_next().await else {
            break;
        };
        if let Ok(Ok(record)) = result {
            records.push(record);
        }
    }

    let loaded = records
        .iter()
        .map(|record| record.summary.txid.as_str())
        .collect::<HashSet<_>>();
    let failed = txids
        .iter()
        .filter(|txid| !loaded.contains(txid.as_str()))
        .cloned()
        .collect();
    (records, failed)
}

async fn load_address_detail(config: &Config, address: &str) -> Result<AddressLoadResult> {
    let client = RpcClient::for_config(config)?;
    let info_output = client
        .get_info()
        .await
        .context("failed to call getinfo for address range")?;
    let tip_height = info_output
        .result
        .get("blocks")
        .or_else(|| info_output.result.get("longestchain"))
        .and_then(Value::as_u64)
        .context("getinfo omitted the chain height")?;
    let start_height = tip_height
        .saturating_sub(ADDRESS_RECENT_BLOCK_WINDOW.saturating_sub(1))
        .max(1);
    let recent_output = client
        .noninteractive()
        .get_address_txids_range(address, start_height, tip_height)
        .await;
    let all_txids = match recent_output {
        Ok(output) => output.result,
        Err(_) => Vec::new(),
    };

    let transaction_ids = all_txids
        .iter()
        .rev()
        .take(ADDRESS_TRANSACTION_LIMIT)
        .cloned()
        .collect::<Vec<_>>();
    let first_txid = all_txids.first().cloned();
    let latest_txid = all_txids.last().cloned();
    Ok(AddressLoadResult {
        address: AddressRecord {
            address: address.to_string(),
            transaction_count: all_txids.len(),
            transaction_ids,
            first_txid,
            latest_txid,
            unspent_outpoints: HashSet::new(),
            balance_response: None,
            utxos_complete: false,
            history_complete: false,
        },
    })
}

async fn load_address_utxos(config: &Config, address: &str) -> Result<HashSet<OutPoint>> {
    let client = RpcClient::for_config(config)?.noninteractive();
    let output = client
        .get_address_utxos(address)
        .await
        .context("failed to call getaddressutxos")?;
    Ok(output.result.iter().filter_map(utxo_outpoint).collect())
}

async fn load_address_balance(config: &Config, address: &str) -> Result<Value> {
    let client = RpcClient::for_config(config)?.noninteractive();
    let output = client
        .get_address_balance(address)
        .await
        .context("failed to call getaddressbalance")?;
    Ok(output.result)
}

async fn load_address_history(config: &Config, address: &str) -> Result<Vec<Txid>> {
    let client = RpcClient::for_config(config)?.noninteractive();
    let output = client
        .get_address_txids(address)
        .await
        .context("failed to call getaddresstxids for full history")?;
    Ok(output.result)
}

async fn load_address_identities(
    config: &Config,
    address: &str,
) -> Result<Option<AddressIdentityAssociations>> {
    let client = RpcClient::for_config(config)?.noninteractive();
    let output = client.get_identities_with_address(address).await?;
    Ok(Some(AddressIdentityAssociations {
        identities: parse_identity_associations(address, output.result),
        source: if output.source_id.starts_with("public-api:") {
            IdentityAssociationSource::PublicApi
        } else {
            IdentityAssociationSource::LocalDaemon
        },
    }))
}

fn parse_identity_associations(address: &str, values: Vec<Value>) -> Vec<IdentityAssociation> {
    let mut identities = values
        .into_iter()
        .filter(|identity| {
            identity
                .get("primaryaddresses")
                .and_then(Value::as_array)
                .is_some_and(|addresses| {
                    addresses
                        .iter()
                        .any(|value| value.as_str() == Some(address))
                })
        })
        .filter_map(|identity| {
            let identity_address = identity.get("identityaddress")?.as_str()?.to_string();
            let name = identity.get("name")?.as_str()?.trim_end_matches('@');
            Some(IdentityAssociation {
                identity_address,
                display_name: format!("{name}@"),
            })
        })
        .collect::<Vec<_>>();
    let mut seen = HashSet::new();
    identities.retain(|identity| seen.insert(identity.identity_address.clone()));
    identities.sort_by(|left, right| {
        left.display_name
            .to_ascii_lowercase()
            .cmp(&right.display_name.to_ascii_lowercase())
            .then_with(|| left.identity_address.cmp(&right.identity_address))
    });
    identities
}

#[cfg(test)]
fn address_index_range_params_json(address: &str, start: u64, end: u64) -> Result<String> {
    Ok(serde_json::to_string(&[json!({
        "addresses": [address],
        "start": start,
        "end": end
    })])?)
}

fn utxo_outpoint(utxo: &Value) -> Option<OutPoint> {
    let txid = utxo.get("txid")?.as_str()?.to_string();
    let index = ["outputIndex", "outputindex", "vout", "index"]
        .iter()
        .find_map(|key| utxo.get(key).and_then(Value::as_u64))?;
    Some(OutPoint { txid, index })
}

async fn load_transaction(config: &Config, txid: &str) -> Result<TransactionRecord> {
    let params_json = serde_json::to_string(&[json!(txid), json!(1)])
        .expect("serializing getrawtransaction parameters cannot fail");
    if let Some((raw_json, source)) =
        cached_rpc_response(config, "getrawtransaction", &params_json).await?
    {
        // A mempool response has no stable inclusion context and must be queried live.
        if raw_json.get("blockhash").and_then(Value::as_str).is_some() {
            let mut record = transaction_record(txid, raw_json);
            record.source_id = Some(source);
            return Ok(hydrate_transaction_block_context(config, record, false).await);
        }
    }
    fetch_transaction(config, txid).await
}

async fn fetch_transaction(config: &Config, txid: &str) -> Result<TransactionRecord> {
    let client = RpcClient::for_config(config)?;
    let output = client.get_raw_transaction(txid).await?;
    anyhow::ensure!(
        output.result.get("txid").and_then(Value::as_str) == Some(txid),
        "source returned a different transaction ID"
    );
    let mut record = transaction_record(txid, output.result);
    record.source_id = Some(output.source_id);
    Ok(hydrate_transaction_block_context(config, record, true).await)
}

async fn hydrate_transaction_block_context(
    config: &Config,
    mut transaction: TransactionRecord,
    live: bool,
) -> TransactionRecord {
    let Some(hash) = transaction.block_hash.as_deref() else {
        return transaction;
    };
    if transaction.block_height.is_some() && transaction.block_time > 0 {
        return transaction;
    }
    let params = [json!(hash)];
    let params_json = serde_json::to_string(&params).expect("block hash parameters");
    let cached = if live {
        None
    } else {
        let header = cached_local_rpc_result(config, "getblockheader", &params_json)
            .await
            .ok()
            .flatten();
        if header.is_some() {
            header
        } else if let Ok(params) = block_params_json(&BlockId::Hash(hash.into()), 1) {
            cached_local_rpc_result(config, "getblock", &params)
                .await
                .ok()
                .flatten()
        } else {
            None
        }
    };
    let header = if let Some(header) = cached {
        Some(header)
    } else {
        match RpcClient::for_config(config).map(RpcClient::noninteractive) {
            Ok(client) => match client
                .call_with_raw::<Value>("getblockheader", &params)
                .await
            {
                Ok(output) => Some(output.result),
                Err(_) => None,
            },
            Err(_) => None,
        }
    };
    if let Some(header) = header {
        transaction.block_height = header.get("height").and_then(Value::as_u64);
        transaction.block_time = header
            .get("time")
            .and_then(Value::as_u64)
            .unwrap_or(transaction.block_time);
    }
    transaction
}

fn transaction_record(txid: &str, raw_json: Value) -> TransactionRecord {
    let block_hash = raw_json
        .get("blockhash")
        .and_then(Value::as_str)
        .map(str::to_string);
    let block_time = raw_json
        .get("blocktime")
        .or_else(|| raw_json.get("time"))
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let block_height = raw_json.get("height").and_then(Value::as_u64);
    let summary = TransactionSummary {
        txid: txid.to_string(),
        raw: Some(raw_json.clone()),
    };
    let prepared = Some(Arc::new(PreparedTransaction::new(&summary, &raw_json)));
    TransactionRecord {
        source_id: None,
        summary,
        raw_json,
        block_hash,
        block_height,
        block_time,
        prepared,
    }
}

async fn load_identity_version_with_client(
    config: &Config,
    client: &RpcClient,
    query: &str,
    height: Option<u64>,
    load_timestamp: bool,
) -> Result<(IdentityVersionRecord, Option<TransactionRecord>)> {
    let params = match height {
        Some(height) => vec![json!(query), json!(height)],
        None => vec![json!(query)],
    };
    let params_json = serde_json::to_string(&params)?;
    let raw_json = if height.is_some() {
        cached_local_rpc_result(config, "getidentity", &params_json).await?
    } else {
        None
    };
    let raw_json = match raw_json {
        Some(raw_json) => raw_json,
        None => {
            let output = client.get_identity(query, height).await?;
            output.result
        }
    };
    let identity = raw_json
        .get("identity")
        .context("getidentity result omitted identity")?;
    let identity_address = identity
        .get("identityaddress")
        .and_then(Value::as_str)
        .context("getidentity result omitted identityaddress")?
        .to_string();
    let fully_qualified_name = raw_json
        .get("fullyqualifiedname")
        .or_else(|| raw_json.get("friendlyname"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            identity
                .get("name")
                .and_then(Value::as_str)
                .map(|name| format!("{name}@"))
        })
        .context("getidentity result omitted identity name")?;
    let block_height = raw_json
        .get("blockheight")
        .and_then(Value::as_u64)
        .context("getidentity result omitted blockheight")?;
    let transaction = match raw_json.get("txid").and_then(Value::as_str) {
        _ if !load_timestamp => None,
        Some(txid) if !txid.chars().all(|ch| ch == '0') => {
            load_transaction(&config.noninteractive(), txid).await.ok()
        }
        _ => None,
    };
    let block_time = transaction
        .as_ref()
        .map(|transaction| transaction.block_time)
        .unwrap_or_default();

    Ok((
        IdentityVersionRecord {
            identity_address,
            fully_qualified_name,
            block_height,
            block_time,
            raw_json,
        },
        transaction,
    ))
}

async fn load_identity_detail(config: &Config, query: &str) -> Result<IdentityLoadResult> {
    let client = RpcClient::for_config(config)?;
    let background_client = client.clone().noninteractive();
    let mut versions = Vec::new();
    let mut transactions = Vec::new();
    let mut seen_heights = HashSet::new();
    let mut requested_height = None;
    let history_complete;

    loop {
        let lookup_client = if requested_height.is_some() {
            &background_client
        } else {
            &client
        };
        let loaded =
            load_identity_version_with_client(config, lookup_client, query, requested_height, true)
                .await;
        let (version, transaction) = match loaded {
            Ok(loaded) => loaded,
            Err(error) if !versions.is_empty() && is_identity_not_found_error(&error) => {
                history_complete = true;
                break;
            }
            Err(_) if !versions.is_empty() => {
                // Historical enrichment must not discard an available current identity.
                history_complete = false;
                break;
            }
            Err(error) => return Err(error),
        };
        if !seen_heights.insert(version.block_height) {
            history_complete = true;
            break;
        }
        if let Some(transaction) = transaction {
            transactions.push(transaction);
        }
        let previous_height = version.block_height.checked_sub(1);
        versions.push(version);
        let Some(previous_height) = previous_height else {
            history_complete = true;
            break;
        };
        requested_height = Some(previous_height);
    }

    let mut references = BTreeSet::new();
    for version in &versions {
        collect_identity_references(&version.raw_json, &mut references);
    }
    let mut resolved_names = versions
        .iter()
        .map(|version| {
            (
                version.identity_address.clone(),
                version.fully_qualified_name.clone(),
            )
        })
        .collect::<HashMap<_, _>>();
    for reference in references {
        if resolved_names.contains_key(&reference) {
            continue;
        }
        let Ok((resolved, _)) =
            load_identity_version_with_client(config, &background_client, &reference, None, false)
                .await
        else {
            continue;
        };
        resolved_names.insert(reference, resolved.fully_qualified_name);
    }

    Ok(IdentityLoadResult {
        query: query.to_string(),
        versions,
        resolved_names,
        transactions,
        history_complete,
    })
}

fn is_identity_not_found_error(error: &anyhow::Error) -> bool {
    error
        .to_string()
        .to_ascii_lowercase()
        .contains("identity not found")
}

fn identity_alias_key(value: &str) -> String {
    value.to_lowercase()
}

fn identity_version_is_active(version: &IdentityVersionRecord) -> bool {
    version
        .raw_json
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| status.eq_ignore_ascii_case("active"))
}

fn collect_identity_references(value: &Value, references: &mut BTreeSet<String>) {
    match value {
        Value::String(value) if looks_like_identity_address(value) => {
            references.insert(value.clone());
        }
        Value::Array(values) => {
            for value in values {
                collect_identity_references(value, references);
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                if looks_like_identity_address(key) {
                    references.insert(key.clone());
                }
                collect_identity_references(value, references);
            }
        }
        _ => {}
    }
}

fn source_transaction_ids(raw: &Value) -> Vec<Txid> {
    let mut txids = raw
        .get("vin")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|input| input.get("txid").and_then(Value::as_str))
        .map(str::to_string)
        .collect::<Vec<_>>();
    txids.sort();
    txids.dedup();
    txids
}

fn has_transaction_size(raw: &Value) -> bool {
    json_u64(raw, "size").is_some()
        || json_u64(raw, "vsize").is_some()
        || transaction_hex_size(raw).is_some()
}

fn block_params_json(block: &BlockId, verbosity: u8) -> Result<String> {
    let selector = match block {
        BlockId::Height(height) => json!(height.to_string()),
        BlockId::Hash(hash) => json!(hash),
    };
    Ok(serde_json::to_string(&[selector, json!(verbosity)])?)
}

fn rpc_u64_field(value: &Value, key: &str) -> Option<u64> {
    value
        .get(key)
        .and_then(|field| field.as_u64().or_else(|| field.as_str()?.parse().ok()))
}

fn rpc_result_value(response_json: &str) -> Result<Value> {
    let response: Value = serde_json::from_str(response_json)?;
    response
        .get("result")
        .cloned()
        .context("RPC response did not contain result")
}

async fn cached_local_rpc_result(
    config: &Config,
    method: &str,
    params_json: &str,
) -> Result<Option<Value>> {
    Ok(cached_rpc_response(config, method, params_json)
        .await?
        .map(|(value, _)| value))
}

async fn cached_rpc_response(
    config: &Config,
    method: &str,
    params_json: &str,
) -> Result<Option<(Value, String)>> {
    let cache = Cache::open(&config.cache_path)?;
    let mut sources = vec![config.daemon_cache_key()];
    sources.extend(
        config
            .daemon
            .api_pool
            .endpoints
            .iter()
            .map(|endpoint| crate::rpc::source_id("public-api", &endpoint.url)),
    );
    for source in sources {
        if let Some(response) = cache
            .raw_rpc_response_async(&source, &config.chain_cache_key(), method, params_json)
            .await?
        {
            return Ok(Some((rpc_result_value(&response)?, source)));
        }
    }
    Ok(None)
}

#[derive(Default)]
struct DefiPanel {
    roots: Vec<(u32, String)>,
    selected_root: usize,
    selected_entry: usize,
    asset_selection: Option<usize>,
    pool_details: bool,
    pool_comparison: Option<Result<crate::defi::pool::Comparison, String>>,
    pool_task: Option<tokio::task::JoinHandle<()>>,
    pool_receiver: Option<UnboundedReceiver<Result<crate::defi::pool::Comparison, String>>>,
    scroll: u16,
    search: crate::defi::ImportSearch,
    height_input: Option<String>,
    message: Option<String>,
    report: Option<crate::defi::Trail>,
    task: Option<tokio::task::JoinHandle<()>>,
    receiver: Option<UnboundedReceiver<crate::defi::Trail>>,
}
impl Drop for DefiPanel {
    fn drop(&mut self) {
        if let Some(task) = self.pool_task.take() {
            task.abort();
        }

        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
impl DefiPanel {
    fn for_output(transaction: &Value, vout: u32) -> Option<Self> {
        let roots = crate::defi::roots(transaction);
        let selected_root = roots.iter().position(|(index, _)| *index == vout)?;
        let mut panel = Self::default();
        panel.roots = roots;
        panel.selected_root = selected_root;
        Some(panel)
    }
    fn start(&mut self, config: &Config, configs: &[Config], state: &AppState) {
        let Screen::Transaction { txid, .. } = &state.screen else {
            return;
        };
        let Some(transaction) = state.store.transaction(txid) else {
            return;
        };
        let Some((vout, _)) = self.roots.get(self.selected_root) else {
            return;
        };
        let Some(system_id) = config.chain.system_id.get() else {
            return;
        };
        if let Some(task) = self.task.take() {
            task.abort();
        }
        let root = crate::world::OutpointRef {
            system_id: system_id.clone(),
            txid: txid.clone(),
            vout: *vout,
        };
        let seed = transaction.raw_json.clone();
        let cache_path = config.cache_path.clone();
        let configs = configs.to_vec();
        let (sender, receiver) = unbounded_channel();
        let search_plan = crate::defi::SearchPlan {
            request: if self
                .report
                .as_ref()
                .is_some_and(|report| report.search_root.is_none())
            {
                crate::defi::ImportSearch::Initial
            } else {
                self.search
            },
            target: self
                .report
                .as_ref()
                .and_then(|report| report.search_root.clone()),
            known_imports: self
                .report
                .as_ref()
                .map(|report| report.import_heights.clone())
                .unwrap_or_default(),
        };
        if let Some(task) = self.pool_task.take() {
            task.abort();
        }
        self.pool_details = false;
        self.pool_receiver = None;
        self.pool_comparison = None;
        self.receiver = Some(receiver);
        self.report = None;
        self.selected_entry = 0;
        self.scroll = 0;
        self.message = None;
        self.task = Some(tokio::spawn(async move {
            let cache = Cache::open(&cache_path).ok();
            let retained = if let Some(cache) = &cache {
                cache
                    .defi_edges(root.clone())
                    .await
                    .ok()
                    .and_then(|edges| crate::defi::retained_trail(&root, &edges))
            } else {
                None
            };
            if let Some(cached) = &retained {
                let _ = sender.send(cached.clone());
            }
            let mut report = crate::defi::investigate(
                configs,
                root.clone(),
                seed,
                search_plan,
                Some(sender.clone()),
            )
            .await;
            let edges = crate::defi::lifecycle_edges(&root, &report);
            if !edges.is_empty() {
                match cache {
                    Some(cache) => {
                        if let Err(error) = cache.record_defi_edges(root.clone(), edges).await {
                            report
                                .notes
                                .push(format!("Lifecycle evidence storage unavailable: {error}"));
                        }
                    }
                    None => report
                        .notes
                        .push("Lifecycle evidence storage unavailable".into()),
                }
            }
            if !report.entries.iter().any(|entry| entry.label == "Import")
                && report.edges.is_empty()
                && let Some(mut cached) = retained
            {
                report.entries.extend(cached.entries.drain(1..));
                report.notes.append(&mut cached.notes);
            }
            let _ = sender.send(report);
        }));
    }
}

fn keep_defi_selection_visible(
    panel: &mut DefiPanel,
    store: &ObjectStore,
    theme: &Theme,
    size: Rect,
) {
    let lines = defi_lines(panel, store, theme);
    let Some(index) = lines.iter().position(|line| {
        line.spans
            .first()
            .is_some_and(|span| span.content == SELECTION_MARKER)
    }) else {
        return;
    };
    let body = workspace_layout(size, defi_controls(panel))[1];
    let content = scroll_view::paragraph_area(
        &Paragraph::new(lines.clone()).wrap(Wrap { trim: false }),
        body,
    );
    let width = content.width.max(1);
    let height = usize::from(content.height.max(1));
    let count = |lines: &[Line<'_>]| {
        Paragraph::new(lines.to_vec())
            .wrap(Wrap { trim: false })
            .line_count(width)
    };
    let mut start = index;
    while start > 0
        && lines[start - 1]
            .spans
            .first()
            .is_some_and(|span| span.content.starts_with("    "))
    {
        start -= 1;
        if lines[start]
            .spans
            .first()
            .is_some_and(|span| span.content.starts_with("    Leg "))
        {
            break;
        }
    }
    if !lines[start]
        .spans
        .first()
        .is_some_and(|span| span.content.starts_with("    Leg "))
    {
        start = index;
    }
    let top = if start == 0 {
        0
    } else {
        count(&lines[..start])
    };
    let bottom = count(&lines[..(index + 2).min(lines.len())]);
    if bottom.saturating_sub(top) > height {
        panel.scroll = scroll_view::follow_selection(&lines, body, panel.scroll);
        return;
    }
    let scroll = usize::from(panel.scroll);
    if top < scroll {
        panel.scroll = u16::try_from(top).unwrap_or(u16::MAX);
    } else if bottom > scroll + height {
        panel.scroll = u16::try_from(bottom.saturating_sub(height).min(top)).unwrap_or(u16::MAX);
    }
}

fn defi_lines(panel: &DefiPanel, store: &ObjectStore, theme: &Theme) -> Vec<Line<'static>> {
    if panel.pool_details {
        return defi_ui::pool_lines(panel, store, theme);
    }
    if panel.asset_selection.is_some() {
        return defi_ui::asset_lines(panel, store, theme);
    }
    let protocol_only = panel
        .roots
        .get(panel.selected_root)
        .is_some_and(|(_, label)| {
            matches!(label.as_str(), "Currency import" | "Fee pool checkpoint")
        });
    let mut lines = vec![
        Line::styled(
            if protocol_only {
                "Protocol references and observed balances are separate from attributed payments."
            } else {
                "Amounts are per leg. Sent is instruction principal; fee budgets are separate from transaction mining fees."
            },
            theme.muted(),
        ),
        Line::default(),
    ];
    if let Some(input) = &panel.height_input {
        lines.push(Line::from(format!(
            "Destination height: {input}_  (Enter search, Esc cancel)"
        )));
    }
    if let Some(message) = &panel.message {
        lines.push(Line::styled(message.clone(), theme.warning()));
    }
    if let Some((vout, label)) = panel.roots.get(panel.selected_root) {
        lines.push(Line::styled(
            format!("Selected output {vout}: {label}"),
            theme.focused(),
        ));
    }
    if let Some(report) = &panel.report {
        if let Some(search) = &report.search {
            lines.push(Line::from(format!(
                "Destination heights {}..{} of tip {} ({} window(s)); older: {}{}",
                theme.format_height_on(&search.system, search.first),
                theme.format_height_on(&search.system, search.last),
                theme.format_height_on(&search.system, search.tip),
                search.windows,
                if search.more_older {
                    "available"
                } else {
                    "genesis reached"
                },
                if report.searching {
                    " — scanning…"
                } else {
                    ""
                }
            )));
        }
        for (index, item) in report.entries.iter().enumerate() {
            if matches!(
                item.label.as_str(),
                "Conversion" | "Reserve transfer" | "Cross-system transfer" | "Launch contribution"
            ) && let Some((number, leg)) = report
                .legs
                .iter()
                .enumerate()
                .find(|(_, leg)| leg.source == item.output)
            {
                lines.extend(defi_ui::summary_lines(leg, number + 1, store, theme));
            }
            lines.push(
                selection_line(
                    theme,
                    index == panel.selected_entry,
                    format!(
                        "{} — {}:{}:{}",
                        item.label, item.output.system_id, item.output.txid, item.output.vout
                    ),
                )
                .style(theme.tile_body(index == panel.selected_entry)),
            );
            lines.push(Line::from(format!(
                "    {}",
                currency_text_display(
                    store,
                    &theme.height_text(&item.detail, item.output.system_id.as_ref())
                )
            )));
        }
        if report.entries.is_empty() {
            for (index, leg) in report.legs.iter().enumerate() {
                lines.extend(defi_ui::summary_lines(leg, index + 1, store, theme));
            }
        }
        for note in &report.notes {
            lines.push(Line::from(format!(
                "Note: {}",
                currency_text_display(store, note)
            )));
        }
        if let Some(stop) = &report.stop {
            lines.push(Line::from(format!("Stopped: {}", stop.message)));
        }
    } else if panel.task.is_some() {
        lines.push(Line::from(
            "Inspecting selected DeFi output and its protocol references…",
        ));
    } else {
        for (index, (vout, label)) in panel.roots.iter().enumerate() {
            lines.push(selection_line(
                theme,
                index == panel.selected_root,
                format!("Output {vout}: {label}"),
            ));
        }
        if panel.roots.is_empty() {
            lines.push(Line::from(
                "No reserve transfer, currency import or fee pool in this transaction",
            ));
        }
    }
    lines
}

fn defi_controls(panel: &DefiPanel) -> &'static str {
    if panel.height_input.is_some() {
        "Enter look up height | Esc cancel"
    } else if panel.pool_details {
        "j/k or PgUp/PgDn scroll | p open pool | v/Esc back | q quit"
    } else if panel.asset_selection.is_some() {
        "j/k select | Enter/Right/l open currency | c/Esc back | F8 close | q quit"
    } else if panel
        .roots
        .get(panel.selected_root)
        .is_some_and(|(_, label)| {
            matches!(label.as_str(), "Currency import" | "Fee pool checkpoint")
        })
    {
        "j/k select | Enter/Right/l investigate/open | F5/r refresh | F8/Esc close | q quit"
    } else {
        "j/k select | Enter/Right/l investigate/open | c currencies | p pool | v pool states | g destination height | [/] adjacent window | s scan 32 older windows | F5/r refresh | F8/Esc close | q quit"
    }
}
fn read_terminal_event(timing: &ui_timing::Timing) -> Result<(Event, Instant)> {
    let _measurement = timing.measure("terminal_read");
    Ok((event::read()?, Instant::now()))
}

fn run_loop(
    terminal: &mut Tui,
    theme: &Theme,
    sessions: &mut Vec<ChainSession>,
    registry: &mut crate::sources::Registry,
    job_control: Option<&job_control::JobControl>,
) -> Result<()> {
    let mut mouse_capture = true;
    let mut active = 0;
    let mut current = 0;
    let mut bookmarks =
        bookmark_ui::Workspace::new(&sessions[0].config, registry.bookmark_options());
    let mut cross_history: Vec<(usize, usize, usize)> = Vec::new();
    let mut links_open = false;
    let mut defi: Option<DefiPanel> = None;
    let mut investigation: Option<reorg_ui::Panel> = None;
    let mut link_selection = 0usize;
    let mut pending_link: Option<(crate::world::OutpointRef, bool)> = None;
    let mut pending_notarization: Option<notarization_ui::TransactionContext> = None;
    let mut pending_protocol_block: Option<(String, String)> = None;
    let mut pending_currency: Option<defi_ui::AssetLink> = None;
    let mut chart_task: Option<chart_ui::Task> = None;
    let mut height_dates = height_ui::Manager::new();
    let mut pending_destination: Option<(crate::world::SystemId, String)> = None;
    let mut pending_trail: Option<crate::world::OutpointRef> = None;
    let mut pending_output_focus: HashMap<usize, crate::world::OutpointRef> = HashMap::new();
    let mut local_permission: Option<(usize, crate::world::OutpointRef)> = None;
    let mut source_selection = 0;
    let mut sources_open = false;
    let mut consent: Option<api_permission_ui::Prompt> = None;
    let mut source_message = String::new();
    let mut source_activity = source_ui::Activity::default();
    let mut redraw = true;
    let mut last_periodic_draw = Instant::now();
    let mut pending_input = None;
    let mut input_needs_draw = false;
    let mut activity_needs_update = true;
    let mut background_cursor = 0;
    let timing = ui_timing::Timing::open()?;
    loop {
        if let Some(job) = job_control {
            if job.suspended() {
                std::thread::sleep(Duration::from_millis(10));
                continue;
            }
            if job.take_resume() {
                resume_terminal(terminal, mouse_capture)?;
                pending_input = None;
                redraw = true;
            }
        }
        if pending_input.is_none() && event::poll(Duration::ZERO)? {
            pending_input = Some(read_terminal_event(&timing)?);
        }
        if pending_input
            .as_ref()
            .is_some_and(|(event, _)| input::quit_pressed(event))
            && consent.is_none()
        {
            // Use the visible layer's focus, never an editor hidden beneath it.
            let entering_text = if investigation.is_some() {
                false
            } else if bookmarks.open || bookmarks.editing() {
                bookmarks.editing()
            } else if sources_open {
                false
            } else if let Some(panel) = &defi {
                panel.height_input.is_some()
            } else if local_permission.is_some() || links_open {
                false
            } else {
                quit_input_active(&sessions[active].state)
            };
            if !entering_text {
                draw_shutdown(
                    terminal,
                    theme,
                    "Finishing background work and pending cache writes…",
                )?;
                break;
            }
        }
        if !input_needs_draw && let Some((event, received)) = pending_input.take() {
            timing.record("input_queue", received.elapsed());
            let _timing = timing.measure("input_dispatch");
            input_needs_draw = true;
            activity_needs_update = true;
            let session_links = if links_open {
                session_relationships(sessions, active)
            } else {
                Vec::new()
            };
            let session = &mut sessions[active];
            let config = &session.config;
            let state = &mut session.state;
            let tx = session.tx.clone();
            let size = terminal.size()?;
            let body = main_layout(state, theme, size.into(), mouse_capture)[3];
            if let Event::Resize(width, height) = event {
                // Fullscreen draw resizes and clears both buffers without querying the cursor.
                theme.copy_values.borrow_mut().begin(false);
                let area = Rect::new(0, 0, width, height);
                if let Some(panel) = &mut defi {
                    keep_defi_selection_visible(panel, &state.store, theme, area);
                }
                let body = main_layout(state, theme, area, mouse_capture)[3];
                if let Screen::Witnesses(view) = &mut state.screen
                    && view.mode == ViewMode::Overview
                {
                    view.follow_selection(theme, state.unmask_identity_addresses, body);
                }
                redraw = true;
                continue;
            }
            if let Event::Paste(text) = event {
                if consent.is_none()
                    && local_permission.is_none()
                    && !sources_open
                    && !links_open
                    && investigation.is_none()
                {
                    let text = input::paste_text(&text);
                    if bookmarks.open || bookmarks.editing() {
                        bookmarks.paste(&text);
                    } else if let Some(panel) = &mut defi {
                        if let Some(input) = &mut panel.height_input {
                            input::paste_height(input, &text);
                        }
                    } else {
                        paste_into_active_input(state, &text);
                    }
                }
                redraw = true;
                continue;
            }
            if let Event::Mouse(mouse) = event
                && mouse_capture
                && mouse.kind
                    == crossterm::event::MouseEventKind::Down(crossterm::event::MouseButton::Left)
                && mouse.modifiers.is_empty()
            {
                let target = theme
                    .copy_values
                    .borrow_mut()
                    .select(ratatui::layout::Position::new(mouse.column, mouse.row));
                if let Some(target) = target {
                    copy_clicked_value(state, &tx, target);
                    redraw = true;
                }
                continue;
            }
            let Some(key) = input::navigation_key(event) else {
                continue;
            };
            if key.code == KeyCode::Char('z')
                && key.modifiers.contains(KeyModifiers::CONTROL)
                && let Some(job) = job_control
            {
                job.suspend()?;
                continue;
            }
            theme.copy_values.borrow_mut().clear_selection();
            redraw = true;
            if let Some(prompt) = &mut consent {
                if let Some(decision) = prompt.key(key.code)
                    && let Some(prompt) = consent.take()
                {
                    let _ = prompt.reply.send(decision);
                }
                continue;
            }
            if key.code == KeyCode::Esc {
                let closed = if investigation.take().is_some() {
                    true
                } else if bookmarks.open || bookmarks.editing() {
                    bookmarks.key(KeyCode::Esc, config, &registry.configs);
                    true
                } else if sources_open {
                    let rows = registry.rows();
                    if let Some(&(chain, api)) = rows.get(source_selection) {
                        if !source_activity.key(KeyCode::Esc, &registry.configs[chain], api) {
                            sources_open = false;
                        }
                    } else {
                        sources_open = false;
                    }
                    true
                } else if let Some(panel) = &mut defi {
                    if panel.height_input.take().is_some() {
                        // Cancel only the height entry.
                    } else if panel.pool_details {
                        panel.pool_details = false;
                        panel.scroll = 0;
                        if let Some(task) = panel.pool_task.take() {
                            task.abort();
                        }
                        panel.pool_receiver = None;
                    } else if panel.asset_selection.take().is_some() {
                        panel.scroll = 0;
                    } else {
                        defi = None;
                    }
                    true
                } else if local_permission.take().is_some() {
                    pending_notarization = None;
                    true
                } else if links_open {
                    links_open = false;
                    true
                } else {
                    close_base_interface(state, config, &tx)
                };
                if closed {
                    continue;
                }
            }
            if state.peers.controls.prompt.is_some() {
                if key.code == KeyCode::Char('z') && key.modifiers.contains(KeyModifiers::CONTROL) {
                    suspend_terminal(terminal, mouse_capture)?;
                } else if !key.modifiers.contains(KeyModifiers::CONTROL) {
                    peer_controls::prompt_key(state, config, &tx, key.code);
                }
                continue;
            }
            if key.code == KeyCode::Char('M') && !text_input_active(state) && !bookmarks.editing() {
                mouse_capture = !mouse_capture;
                input::set_mouse_capture(terminal.backend_mut(), mouse_capture)?;
                continue;
            }
            if let Some((index, target)) = &local_permission {
                match key.code {
                    KeyCode::Char('1') => {
                        pending_link = Some((target.clone(), true));
                        local_permission = None;
                    }
                    KeyCode::Char('2') => {
                        registry.configs[*index]
                            .daemon
                            .control
                            .set(crate::sources::ConnectionState::Enabled);
                        pending_link = Some((target.clone(), false));
                        local_permission = None;
                    }
                    _ => {}
                }
                continue;
            }
            if state.auxiliary.is_none()
                && !sources_open
                && !links_open
                && !bookmarks.open
                && investigation.is_none()
                && defi.is_none()
                && chart_ui::prompt_key(state, key)
            {
                continue;
            }
            if let Some(panel) = &mut investigation {
                match key.code {
                    KeyCode::Char('z') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        suspend_terminal(terminal, mouse_capture)?
                    }
                    _ => {
                        if matches!(panel.key(key.code), Some(reorg_ui::Action::Close)) {
                            investigation = None;
                        }
                    }
                }
                continue;
            }
            if let Some(panel) = &mut defi {
                if let Some(input) = &mut panel.height_input {
                    let mut jump = None;
                    match key.code {
                        KeyCode::Char('z') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                            suspend_terminal(terminal, mouse_capture)?
                        }
                        KeyCode::F(8) => panel.height_input = None,
                        KeyCode::Backspace => {
                            input.pop();
                        }
                        KeyCode::Char(digit) if digit.is_ascii_digit() && input.len() < 20 => {
                            input.push(digit)
                        }
                        KeyCode::Enter => jump = input.parse::<u64>().ok(),
                        _ => {}
                    }
                    if let Some(height) = jump {
                        panel.height_input = None;
                        panel.search = crate::defi::ImportSearch::WindowEndingAt(height);
                        panel.start(config, &registry.configs, state);
                    } else if key.code == KeyCode::Enter {
                        panel.message = Some("Enter a valid destination block height".into());
                    }
                    continue;
                }
                if panel.pool_details {
                    match key.code {
                        KeyCode::Char('z') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                            suspend_terminal(terminal, mouse_capture)?
                        }
                        KeyCode::Char('v') => {
                            panel.pool_details = false;
                            panel.scroll = 0;
                            if let Some(task) = panel.pool_task.take() {
                                task.abort();
                            }
                            panel.pool_receiver = None;
                        }
                        KeyCode::F(8) => defi = None,
                        KeyCode::Up | KeyCode::Char('k') => {
                            panel.scroll = panel.scroll.saturating_sub(1)
                        }
                        KeyCode::PageUp => panel.scroll = panel.scroll.saturating_sub(10),
                        KeyCode::Down | KeyCode::Char('j') | KeyCode::PageDown => {
                            let body =
                                workspace_layout(terminal.size()?.into(), defi_controls(panel))[1];
                            let max = wrapped_max_scroll(
                                &defi_lines(panel, &state.store, theme),
                                body.width,
                                body.height,
                            );
                            panel.scroll = panel
                                .scroll
                                .saturating_add(if key.code == KeyCode::PageDown { 10 } else { 1 })
                                .min(u16::try_from(max).unwrap_or(u16::MAX));
                        }
                        KeyCode::Char('p') => {
                            pending_currency =
                                defi_ui::assets(panel).into_iter().find(|link| link.pool)
                        }
                        _ => {}
                    }
                    continue;
                }
                if panel.asset_selection.is_some() {
                    let links = defi_ui::assets(panel);
                    let selected = panel.asset_selection.as_mut().expect("asset picker open");
                    match key.code {
                        KeyCode::Char('z') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                            suspend_terminal(terminal, mouse_capture)?
                        }
                        KeyCode::Char('c') => {
                            panel.asset_selection = None;
                            panel.scroll = 0;
                        }
                        KeyCode::F(8) => defi = None,
                        KeyCode::Down | KeyCode::Char('j') => {
                            *selected = (*selected + 1).min(links.len().saturating_sub(1))
                        }
                        KeyCode::Up | KeyCode::Char('k') => *selected = selected.saturating_sub(1),
                        code if is_open_key(code) => {
                            pending_currency = links.get(*selected).cloned();
                        }
                        _ => {}
                    }
                    if matches!(
                        key.code,
                        KeyCode::Up | KeyCode::Down | KeyCode::Char('j' | 'k')
                    ) && let Some(panel) = &mut defi
                    {
                        let inner =
                            workspace_layout(terminal.size()?.into(), defi_controls(panel))[1];
                        panel.scroll = scroll_view::follow_selection(
                            &defi_ui::asset_lines(panel, &state.store, theme),
                            inner,
                            panel.scroll,
                        );
                    }
                    continue;
                }
                match key.code {
                    KeyCode::Char('z') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        suspend_terminal(terminal, mouse_capture)?
                    }
                    KeyCode::F(8) => defi = None,
                    KeyCode::Char('v') if panel.task.is_none() => {
                        if let Some(leg) = defi_ui::selected_leg(panel)
                            .filter(|leg| leg.pool.is_some())
                            .cloned()
                        {
                            let configs = registry.configs.clone();
                            let (sender, receiver) = unbounded_channel();
                            panel.pool_details = true;
                            panel.scroll = 0;
                            panel.pool_comparison = None;
                            panel.pool_receiver = Some(receiver);
                            panel.pool_task = Some(tokio::spawn(async move {
                                let result = crate::defi::pool::inspect(configs, leg)
                                    .await
                                    .map_err(|error| error.to_string());
                                let _ = sender.send(result);
                            }));
                        } else {
                            panel.message = Some("No pool state for this leg".into());
                        }
                    }
                    KeyCode::Char('c') => {
                        if !defi_ui::assets(panel).is_empty() {
                            panel.asset_selection = Some(0);
                            panel.scroll = 0;
                        } else {
                            panel.message = Some("No currencies available for this entry".into());
                        }
                    }
                    KeyCode::Char('p') => {
                        pending_currency =
                            defi_ui::assets(panel).into_iter().find(|link| link.pool);
                        if pending_currency.is_none() {
                            panel.message = Some("No verified pool context for this leg".into());
                        }
                    }
                    KeyCode::Char('r') | KeyCode::F(5) => {
                        panel.start(config, &registry.configs, state)
                    }
                    KeyCode::Char('g') => {
                        panel.height_input = Some(String::new());
                        panel.message = None;
                    }
                    KeyCode::Char('[' | ']' | 's') if panel.task.is_none() => {
                        if let Some(search) = panel.report.as_ref().and_then(|r| r.search.as_ref())
                        {
                            let request = match key.code {
                                KeyCode::Char('[') if search.first > 0 => Some(
                                    crate::defi::ImportSearch::WindowEndingAt(search.first - 1),
                                ),
                                KeyCode::Char(']') if search.last < search.tip => {
                                    Some(crate::defi::ImportSearch::WindowEndingAt(
                                        search
                                            .last
                                            .saturating_add(crate::defi::IMPORT_WINDOW)
                                            .min(search.tip),
                                    ))
                                }
                                KeyCode::Char('s') if search.more_older => {
                                    Some(crate::defi::ImportSearch::BackwardFrom(search.first - 1))
                                }
                                _ => None,
                            };
                            if let Some(request) = request {
                                panel.search = request;
                                panel.start(config, &registry.configs, state);
                            }
                        }
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        if let Some(report) = &panel.report {
                            panel.selected_entry = (panel.selected_entry + 1)
                                .min(report.entries.len().saturating_sub(1));
                        } else if panel.task.is_none() {
                            panel.selected_root =
                                (panel.selected_root + 1).min(panel.roots.len().saturating_sub(1));
                        }
                        keep_defi_selection_visible(
                            panel,
                            &state.store,
                            theme,
                            terminal.size()?.into(),
                        );
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        if panel.report.is_some() {
                            panel.selected_entry = panel.selected_entry.saturating_sub(1);
                        } else if panel.task.is_none() {
                            panel.selected_root = panel.selected_root.saturating_sub(1);
                        }
                        keep_defi_selection_visible(
                            panel,
                            &state.store,
                            theme,
                            terminal.size()?.into(),
                        );
                    }
                    KeyCode::PageDown => {
                        let body =
                            workspace_layout(terminal.size()?.into(), defi_controls(panel))[1];
                        let max = wrapped_max_scroll(
                            &defi_lines(panel, &state.store, theme),
                            body.width,
                            body.height,
                        );
                        panel.scroll = panel
                            .scroll
                            .saturating_add(10)
                            .min(u16::try_from(max).unwrap_or(u16::MAX));
                    }
                    KeyCode::PageUp => panel.scroll = panel.scroll.saturating_sub(10),
                    code if is_open_key(code) => {
                        if let Some(report) = &panel.report
                            && panel.task.is_none()
                        {
                            if let Some(item) = report.entries.get(panel.selected_entry) {
                                pending_link = Some((item.output.clone(), false));
                                defi = None;
                            }
                        } else if panel.task.is_none() {
                            panel.start(config, &registry.configs, state);
                        }
                    }
                    _ => {}
                }
                continue;
            }
            if bookmarks.open || bookmarks.editing() {
                if key.code == KeyCode::Char('z') && key.modifiers.contains(KeyModifiers::CONTROL) {
                    suspend_terminal(terminal, mouse_capture)?;
                } else if !key.modifiers.contains(KeyModifiers::CONTROL) {
                    let saving_current = !bookmarks.open && key.code == KeyCode::Enter;
                    bookmarks.key(key.code, config, &registry.configs);
                    if saving_current && !bookmarks.editing() {
                        state.notice = Some(TransientNotice {
                            message: "Bookmark saved".into(),
                            until: Instant::now() + Duration::from_secs(4),
                        });
                    }
                }
                continue;
            }
            if key.code == KeyCode::Char('z') && key.modifiers.contains(KeyModifiers::CONTROL) {
                suspend_terminal(terminal, mouse_capture)?;
                continue;
            }
            if sources_open {
                use crate::sources::ConnectionState;
                if key.code == KeyCode::F(2) {
                    sources_open = false;
                    continue;
                }
                let rows = registry.rows();
                let (chain_index, api_index) = rows[source_selection];
                let selected_config = &registry.configs[chain_index];
                if source_activity.key(key.code, selected_config, api_index) {
                    continue;
                }
                let control = api_index
                    .map(|index| &selected_config.daemon.api_pool.endpoints[index].control)
                    .unwrap_or(&selected_config.daemon.control);
                match key.code {
                    KeyCode::Up | KeyCode::Char('k') => {
                        source_selection = source_selection.saturating_sub(1)
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        source_selection = (source_selection + 1).min(rows.len() - 1)
                    }
                    KeyCode::Char('e') => {
                        control.set(if control.state() == ConnectionState::Disabled {
                            ConnectionState::Enabled
                        } else {
                            ConnectionState::Disabled
                        });
                        source_message = "Session change; s saves connection states".into();
                    }
                    KeyCode::Char('m') if api_index.is_none() => {
                        if control.state() != ConnectionState::Disabled {
                            control.set(if control.state() == ConnectionState::Monitored {
                                ConnectionState::Enabled
                            } else {
                                ConnectionState::Monitored
                            });
                            source_message = "Session change; s saves connection states".into();
                        } else {
                            source_message = "Enable this connection before monitoring it".into();
                        }
                    }
                    KeyCode::Char('s') => {
                        source_message = match registry.save() {
                            Ok(()) => "Connection states saved".into(),
                            Err(error) => error.to_string(),
                        }
                    }
                    KeyCode::Enter => {
                        config
                            .daemon
                            .control
                            .visible
                            .store(false, std::sync::atomic::Ordering::Release);
                        current = chain_index;
                        if matches!(
                            state.screen,
                            Screen::Dashboard | Screen::Mempool | Screen::Marketplace
                        ) || state.auxiliary.is_some()
                        {
                            active = current;
                        }
                        sessions[active]
                            .config
                            .daemon
                            .control
                            .visible
                            .store(true, std::sync::atomic::Ordering::Release);
                        sources_open = false;
                        continue;
                    }
                    _ => {}
                }
                continue;
            }
            if links_open {
                if key.code == KeyCode::F(3) {
                    links_open = false;
                    continue;
                }
                let links = &session_links;
                match key.code {
                    KeyCode::Down | KeyCode::Char('j') => {
                        link_selection = (link_selection + 1).min(links.len().saturating_sub(1))
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        link_selection = link_selection.saturating_sub(1)
                    }
                    code if is_open_key(code) => {
                        if let Some(link) = links.get(link_selection) {
                            pending_link = Some((link.target.clone(), false));
                        }
                    }
                    _ => {}
                }
                continue;
            }
            if state.cache_clear_prompt.is_some() {
                handle_cache_clear_prompt_key(state, config, &tx, key.code);
                continue;
            }
            if state.save_prompt.is_some() {
                handle_save_prompt_key(state, key.code);
                continue;
            }
            if state.search.active {
                handle_search_key(state, config, &tx, key.code);
                continue;
            }
            if let Screen::Currencies(view) = &mut state.screen
                && view.edit_key(key.code)
            {
                continue;
            }
            if let Screen::Currency(view) = &mut state.screen
                && let Some(input) = &mut view.height_input
            {
                match key.code {
                    KeyCode::Backspace => {
                        input.pop();
                    }
                    KeyCode::Char(ch) if !ch.is_control() && input.len() < 160 => input.push(ch),
                    KeyCode::Enter => {
                        let target = chart_ui::seek::parse(input, chrono::Utc::now());
                        view.height_input = None;
                        match (target, view.id.clone()) {
                            (Ok(target), Some(id)) => {
                                currency_ui::clear_activity(view);
                                view.history_note = None;
                                let cached = match target {
                                    chart_ui::seek::Target::Height(height) => {
                                        currency_ui::show_cached_height(view, height)
                                    }
                                    chart_ui::seek::Target::Number(height)
                                        if state
                                            .store
                                            .chain
                                            .height
                                            .is_some_and(|tip| height <= tip) =>
                                    {
                                        currency_ui::show_cached_height(view, height)
                                    }
                                    _ => false,
                                };
                                let request = currency_ui::request_id();
                                view.history_request = if cached { None } else { Some(request) };
                                view.loading = !cached;
                                if !cached {
                                    view.loading = true;
                                    currency_ui::spawn_history(
                                        config.clone(),
                                        tx.clone(),
                                        view.query.clone(),
                                        id,
                                        target,
                                        request,
                                    );
                                }
                            }
                            (Err(error), _) => view.error = Some(error.to_string()),
                            _ => view.error = Some("Wait for the currency to load".into()),
                        }
                    }
                    _ => {}
                }
                continue;
            }
            if let Screen::Identity {
                historical_height_input: Some(input),
                ..
            } = &mut state.screen
            {
                let mut submitted = None;
                match key.code {
                    KeyCode::Backspace => {
                        input.pop();
                    }
                    KeyCode::Char(ch) if ch.is_ascii_digit() && input.len() < 12 => input.push(ch),
                    KeyCode::Enter => submitted = input.parse::<u64>().ok(),
                    _ => {}
                }
                if key.code == KeyCode::Enter
                    && let Screen::Identity {
                        historical_height_input,
                        ..
                    } = &mut state.screen
                {
                    *historical_height_input = None;
                }
                if let Some(height) = submitted {
                    request_historical_identity_evidence(state, config, &tx, height);
                }
                continue;
            }
            if state.auxiliary.is_none()
                && !sources_open
                && !links_open
                && let Screen::Notarizations(view) = &mut state.screen
            {
                let (handled, action) = notarization_ui::key(view, key.code, body.height);
                if let Some(action) = action {
                    match action {
                        notarization_ui::Navigation::Transaction(context) => {
                            pending_link = Some((context.target.clone(), false));
                            pending_notarization = Some(context);
                        }
                        notarization_ui::Navigation::Block { system, hash } => {
                            pending_protocol_block = Some((system, hash))
                        }
                    }
                }
                if handled {
                    continue;
                }
            }
            if state.auxiliary.is_none()
                && !sources_open
                && !links_open
                && let Screen::Bytes(view) = &mut state.screen
                && view.key(key.code, body)
            {
                continue;
            }
            if key.code == KeyCode::Char('D') && !text_input_active(state) && !bookmarks.editing() {
                let mut display = theme.heights.borrow_mut();
                display.dates = !display.dates;
                continue;
            }
            if key.code == KeyCode::Char('I') && !text_input_active(state) && !bookmarks.editing() {
                state.unmask_identity_addresses = !state.unmask_identity_addresses;
                continue;
            }
            if key.code == KeyCode::F(10) && !sources_open && !links_open {
                state.auxiliary = None;
                state.help_return = None;
                begin_search(state);
                state.search.query = "estimate:".into();
                continue;
            }
            if key.code == KeyCode::F(9)
                && !sources_open
                && !links_open
                && state.auxiliary.is_none()
            {
                if matches!(state.screen, Screen::Charts(_)) {
                    pop_screen(state);
                } else {
                    let screen = chart_ui::screen(state);
                    push_screen(state, screen);
                }
                continue;
            }
            if state.auxiliary.is_none()
                && !sources_open
                && !links_open
                && chart_ui::handle_key(state, key.code, config, &tx)
            {
                continue;
            }
            if key.code == KeyCode::F(7)
                && !sources_open
                && !links_open
                && state.auxiliary.is_none()
            {
                investigation = reorg_ui::Panel::for_screen(config, state);
                continue;
            }
            if key.code == KeyCode::F(8)
                && state.auxiliary.is_none()
                && let Screen::Currency(view) = &mut state.screen
                && view.mode == ViewMode::Overview
            {
                if let Some(currency_ui::Link::Trail(target)) = currency_ui::selected_link(view) {
                    pending_trail = Some(target.clone());
                    pending_link = Some((target, false));
                } else if let Some(currency_ui::Link::Activity(record)) =
                    currency_ui::selected_link(view)
                    && view.transfer_request.is_none()
                {
                    if let Some(target) = &record.export {
                        if let Some(source) = registry
                            .configs
                            .iter()
                            .find(|config| config.chain.system_id.get() == Some(&target.system_id))
                        {
                            let id = currency_ui::request_id();
                            view.transfer_request = Some(id);
                            view.transfer_export = Some(target.clone());
                            view.transfer_inputs.clear();
                            view.error = None;
                            currency_ui::spawn_transfers(
                                source.clone(),
                                tx.clone(),
                                view.query.clone(),
                                id,
                                record,
                            );
                        } else {
                            view.error =
                                Some(format!("No configured source for {}", target.system_id));
                        }
                    } else {
                        view.error = Some("Activity has no source export reference".into());
                    }
                }
                continue;
            }
            if key.code == KeyCode::F(8) && state.auxiliary.is_none() {
                if let Screen::Transaction {
                    txid,
                    selected_flow,
                    ..
                } = &state.screen
                    && let Some(transaction) = state.store.transaction(txid)
                {
                    let roots = crate::defi::roots(&transaction.raw_json);
                    let flows = transaction_flows(&transaction.raw_json, &state.store);
                    let output = transaction_flow_selections(&flows)
                        .get(*selected_flow)
                        .map(|(i, _)| &flows[*i])
                        .filter(|f| f.kind == TransactionFlowKind::Output)
                        .map(|f| f.index as u32);
                    let selected_root = roots
                        .iter()
                        .position(|(n, _)| Some(*n) == output)
                        .unwrap_or(0);
                    defi = Some(DefiPanel {
                        roots,
                        selected_root,
                        selected_entry: 0,
                        asset_selection: None,
                        pool_details: false,
                        pool_comparison: None,
                        pool_task: None,
                        pool_receiver: None,
                        scroll: 0,
                        search: crate::defi::ImportSearch::Initial,
                        height_input: None,
                        message: None,
                        report: None,
                        task: None,
                        receiver: None,
                    });
                    links_open = false;
                    sources_open = false;
                }
                continue;
            }
            if key.code == KeyCode::F(6) {
                bookmarks.show();
                sources_open = false;
                links_open = false;
                continue;
            }
            if key.code == KeyCode::Char('B')
                && !sources_open
                && !links_open
                && bookmark_ui::supports_current(state)
            {
                if let Err(error) = bookmarks.add_current(config, state) {
                    state.notice = Some(TransientNotice {
                        message: error.to_string(),
                        until: Instant::now() + Duration::from_secs(4),
                    });
                }
                continue;
            }

            if key.code == KeyCode::F(4)
                && !sources_open
                && !links_open
                && state.auxiliary.is_none()
            {
                if matches!(state.screen, Screen::Witnesses(_)) {
                    pop_screen(state);
                } else if let Screen::Transaction {
                    txid,
                    selected_flow,
                    ..
                } = &state.screen
                    && let Some(transaction) = state.store.transaction(txid)
                    && let Some(system) = config.chain.system_id.get()
                {
                    let flows = transaction_flows(&transaction.raw_json, &state.store);
                    let output = transaction_flow_selections(&flows)
                        .get(*selected_flow)
                        .map(|(index, _)| &flows[*index])
                        .filter(|flow| flow.kind == TransactionFlowKind::Output)
                        .map(|flow| flow.index as u32);
                    let view = witness_ui::View::new(
                        system.clone(),
                        txid.clone(),
                        &transaction.raw_json,
                        output,
                    );
                    push_screen(state, Screen::Witnesses(Box::new(view)));
                }
                continue;
            }
            if key.code == KeyCode::F(3)
                && state.auxiliary.is_none()
                && let Screen::Transaction { txid, .. } = &state.screen
            {
                links_open = true;
                link_selection = 0;
                source_message.clear();
                let txid = txid.clone();
                let config = config.clone();
                let sender = tx.clone();
                tokio::spawn(async move {
                    let result = match Cache::open(&config.cache_path) {
                        Ok(cache) => cache
                            .reverse_links(config.chain_cache_key(), txid.clone())
                            .await
                            .map_err(|e| e.to_string()),
                        Err(error) => Err(error.to_string()),
                    };
                    let _ = sender.send(AppEvent::StoredRelationships { txid, result });
                });
                continue;
            }
            if key.code == KeyCode::F(2) {
                sources_open = true;
                source_message.clear();
                source_activity = source_ui::Activity::default();
                continue;
            }
            if active != current && matches!(key.code, KeyCode::Char('d' | 'm' | 'o' | '`' | '/')) {
                config
                    .daemon
                    .control
                    .visible
                    .store(false, std::sync::atomic::Ordering::Release);
                active = current;
                let destination = &mut sessions[current];
                destination
                    .config
                    .daemon
                    .control
                    .visible
                    .store(true, std::sync::atomic::Ordering::Release);
                match key.code {
                    KeyCode::Char('d') => jump_to_dashboard(&mut destination.state),
                    KeyCode::Char('m') => open_mempool_root(&mut destination.state),
                    KeyCode::Char('o') => open_marketplace_root(&mut destination.state),
                    KeyCode::Char('`') => toggle_peer_interface(&mut destination.state),
                    _ => begin_search(&mut destination.state),
                }
                continue;
            }
            if is_back_key(key.code)
                && let Some(&(origin, target, depth)) = cross_history.last()
                && active == target
                && state.screen_stack.len() == depth
            {
                pop_screen(state);
                config
                    .daemon
                    .control
                    .visible
                    .store(false, std::sync::atomic::Ordering::Release);
                active = origin;
                sessions[active]
                    .config
                    .daemon
                    .control
                    .visible
                    .store(true, std::sync::atomic::Ordering::Release);
                cross_history.pop();
                continue;
            }

            if key.code == KeyCode::Char('d') {
                jump_to_dashboard(state);
                continue;
            }
            if matches!(key.code, KeyCode::F(1) | KeyCode::Char('?')) {
                toggle_help(state, config);
                continue;
            }
            if key.code == KeyCode::Char('`') {
                toggle_peer_interface(state);
                continue;
            }
            if key.code == KeyCode::Char('m') {
                state.auxiliary = None;
                state.help_return = None;
                open_mempool_root(state);
                continue;
            }
            if key.code == KeyCode::Char('o') {
                state.auxiliary = None;
                state.help_return = None;
                open_marketplace_root(state);
                continue;
            }
            if state.auxiliary.is_some() {
                if peer_controls::key(state, config, &tx, key.code) {
                    continue;
                }
                let page_height = main_layout(
                    state,
                    theme,
                    Rect::new(0, 0, size.width, size.height),
                    mouse_capture,
                )[3]
                .height;
                handle_auxiliary_key(state, theme, config, &tx, key.code, page_height);
                continue;
            }

            if let Screen::Witnesses(view) = &state.screen
                && view.mode == ViewMode::Json
                && is_open_key(key.code)
            {
                match current_json_entry(state).and_then(|entry| view.json_action(&entry.pointer)) {
                    Some(witness_ui::Action::Identity(system, address)) => {
                        pending_destination = Some((system, address))
                    }
                    Some(witness_ui::Action::Evidence(output)) => {
                        pending_link = Some((output, false))
                    }
                    None => toggle_json_node(state),
                }
                continue;
            }
            if let Screen::Witnesses(view) = &mut state.screen {
                let (handled, action) = view.key(
                    key.code,
                    config,
                    &state.store,
                    theme,
                    state.unmask_identity_addresses,
                    body,
                );
                match action {
                    Some(witness_ui::Action::Identity(system, address)) => {
                        pending_destination = Some((system, address));
                    }
                    Some(witness_ui::Action::Evidence(output)) => {
                        pending_link = Some((output, false))
                    }
                    None => {}
                }
                if handled {
                    continue;
                }
            }
            if handle_marketplace_cycle_key(state, key.code, key.modifiers) {
                continue;
            }
            match key.code {
                KeyCode::Char('/') => begin_search(state),
                KeyCode::Char('c') => {
                    let at_cross_chain_entry =
                        cross_history.last().is_some_and(|&(_, target, depth)| {
                            active == target && state.screen_stack.len() == depth
                        });
                    if !toggle_identity_currency(state, config, &tx, !at_cross_chain_entry) {
                        let screen = currency_browser::screen(&state.store);
                        let request = if let Screen::Currencies(view) = &screen {
                            view.request
                        } else {
                            unreachable!()
                        };
                        push_screen(state, screen);
                        currency_browser::spawn(config.clone(), tx.clone(), request);
                    }
                }
                KeyCode::Char('F') if matches!(state.screen, Screen::Currencies(_)) => {
                    if let Screen::Currencies(view) = &mut state.screen {
                        view.options_open = true;
                    }
                }
                KeyCode::Char('f') if matches!(state.screen, Screen::Currencies(_)) => {
                    if let Screen::Currencies(view) = &mut state.screen {
                        view.editing = true;
                    }
                }
                KeyCode::Char('r') | KeyCode::F(5)
                    if matches!(state.screen, Screen::Currencies(_)) =>
                {
                    if let Screen::Currencies(view) = &mut state.screen {
                        view.request = currency_ui::request_id();
                        view.loading = true;
                        currency_browser::spawn(config.clone(), tx.clone(), view.request);
                    }
                }
                KeyCode::Down
                | KeyCode::Char('j')
                | KeyCode::Up
                | KeyCode::Char('k')
                | KeyCode::PageDown
                | KeyCode::PageUp
                | KeyCode::Home
                | KeyCode::End
                    if matches!(state.screen, Screen::Currencies(_)) =>
                {
                    if let Screen::Currencies(view) = &mut state.screen {
                        let step = (body.height.saturating_sub(5) / 5).max(1) as isize;
                        let delta = match key.code {
                            KeyCode::Up | KeyCode::Char('k') => -1,
                            KeyCode::PageDown => step,
                            KeyCode::PageUp => -step,
                            KeyCode::Home => -(view.entries.len() as isize),
                            KeyCode::End => view.entries.len() as isize,
                            _ => 1,
                        };
                        view.move_by(delta);
                    }
                }
                code if matches!(state.screen, Screen::Currencies(_)) && is_open_key(code) => {
                    let entry = if let Screen::Currencies(view) = &state.screen {
                        view.selected()
                    } else {
                        None
                    };
                    if let Some(entry) = entry {
                        push_screen(
                            state,
                            currency_ui::screen(entry.id.clone(), config.chain_cache_key()),
                        );
                        currency_ui::spawn_load(config.clone(), tx.clone(), entry.id, false);
                    }
                }
                KeyCode::Esc => navigate_back_or_dashboard(state),
                code if is_back_key(code) && state.screen_stack.is_empty() => {}
                code if is_back_key(code) => pop_screen(state),
                KeyCode::Home if current_json_document(state).is_some() => {
                    move_json_cursor_to_boundary(state, false);
                }
                KeyCode::End if current_json_document(state).is_some() => {
                    move_json_cursor_to_boundary(state, true);
                }
                KeyCode::PageDown if current_json_document(state).is_some() => {
                    move_json_cursor(state, body.height.saturating_sub(3).max(1) as isize);
                }
                KeyCode::PageUp if current_json_document(state).is_some() => {
                    move_json_cursor(state, -(body.height.saturating_sub(3).max(1) as isize));
                }
                KeyCode::Home if overview_summary_list_active(state) => {
                    select_summary_boundary(state, false);
                }
                KeyCode::End if overview_summary_list_active(state) => {
                    select_summary_boundary(state, true);
                }
                KeyCode::PageDown if overview_summary_list_active(state) => {
                    let amount = summary_page_size(state, body.height);
                    move_summary_page(state, amount, 1);
                }
                KeyCode::PageUp if overview_summary_list_active(state) => {
                    let amount = summary_page_size(state, body.height);
                    move_summary_page(state, amount, -1);
                }
                KeyCode::Down | KeyCode::Char('j')
                    if matches!(
                        state.screen,
                        Screen::Currency(ref view) if view.mode == ViewMode::Overview
                    ) =>
                {
                    if let Screen::Currency(view) = &mut state.screen {
                        currency_ui::select(view, 1, body.height);
                    }
                }
                KeyCode::Up | KeyCode::Char('k')
                    if matches!(
                        state.screen,
                        Screen::Currency(ref view) if view.mode == ViewMode::Overview
                    ) =>
                {
                    if let Screen::Currency(view) = &mut state.screen {
                        currency_ui::select(view, -1, body.height);
                    }
                }
                KeyCode::Char('n') if matches!(state.screen, Screen::Currency(ref view) if view.mode == ViewMode::Overview) => {
                    if let Screen::Currency(view) = &state.screen
                        && let Some(screen) =
                            notarization_ui::browser(view, config, &registry.configs)
                    {
                        push_screen(state, screen);
                    }
                }
                KeyCode::Char('r') | KeyCode::F(5)
                    if matches!(state.screen, Screen::Currency(_)) =>
                {
                    if let Screen::Currency(view) = &mut state.screen {
                        view.loading = true;
                        view.notarizations.refresh();
                        currency_ui::spawn_load(
                            config.clone(),
                            tx.clone(),
                            view.query.clone(),
                            true,
                        );
                    }
                }
                KeyCode::Char('g') if matches!(state.screen, Screen::Currency(_)) => {
                    if let Screen::Currency(view) = &mut state.screen {
                        view.height_input = Some(String::new());
                    }
                }
                KeyCode::Char('v')
                    if matches!(
                        state.screen,
                        Screen::Identity {
                            mode: ViewMode::Overview,
                            ..
                        }
                    ) =>
                {
                    let height = if let Screen::Identity { target, .. } = &state.screen {
                        state.store.identity_for_target(target).and_then(|version| {
                            state
                                .store
                                .identity_evidence
                                .contains_key(&version.identity_address)
                                .then_some(version.block_height)
                        })
                    } else {
                        None
                    };
                    if let (
                        Some(height),
                        Screen::Identity {
                            historical_height_input,
                            ..
                        },
                    ) = (height, &mut state.screen)
                    {
                        *historical_height_input = Some(height.to_string());
                    }
                }
                KeyCode::Char('a' | 'w' | '[' | ']') if matches!(state.screen, Screen::Currency(ref view) if view.mode == ViewMode::Overview) => {
                    if let Screen::Currency(view) = &mut state.screen {
                        match key.code {
                            KeyCode::Char('a') => currency_ui::start_activity(view, config, &tx),
                            KeyCode::Char('w') => currency_ui::start_window(view, config, &tx, 0),
                            KeyCode::Char('[') => currency_ui::start_window(view, config, &tx, -1),
                            _ => currency_ui::start_window(view, config, &tx, 1),
                        }
                    }
                }
                code if matches!(
                    state.screen,
                    Screen::Currency(ref view) if view.mode == ViewMode::Overview
                ) && is_open_key(code) =>
                {
                    let link = if let Screen::Currency(view) = &state.screen {
                        currency_ui::selected_link(view)
                    } else {
                        None
                    };
                    match link {
                        Some(currency_ui::Link::Currency(id)) => {
                            push_screen(
                                state,
                                currency_ui::screen(id.clone(), config.chain_cache_key()),
                            );
                            currency_ui::spawn_load(config.clone(), tx.clone(), id, false);
                        }
                        Some(currency_ui::Link::Notarizations) => {
                            if let Screen::Currency(view) = &state.screen
                                && let Some(screen) =
                                    notarization_ui::browser(view, config, &registry.configs)
                            {
                                push_screen(state, screen);
                            }
                        }
                        Some(currency_ui::Link::Transaction(txid)) => {
                            push_screen(state, transaction_screen(txid.clone()));
                            if state.loading_transactions.insert(txid.clone()) {
                                spawn_transaction_loader(config.clone(), tx.clone(), vec![txid]);
                            }
                        }
                        Some(currency_ui::Link::Activity(record)) => {
                            pending_link = Some((record.output, false));
                        }
                        Some(currency_ui::Link::Trail(target)) => {
                            pending_trail = Some(target.clone());
                            pending_link = Some((target, false));
                        }
                        Some(currency_ui::Link::ActivityWindow(index)) => {
                            if let Screen::Currency(view) = &mut state.screen {
                                currency_ui::clear_activity(view);
                                view.activity = view.retained_activity.get(index).cloned();
                                view.activity_retained = true;
                                currency_ui::rebuild_document(view);
                                currency_ui::focus_section(view, "Currency activity heights");
                            }
                        }
                        Some(currency_ui::Link::Historical(height)) => {
                            if let Screen::Currency(view) = &mut state.screen {
                                currency_ui::show_cached_height(view, height);
                            }
                        }
                        Some(currency_ui::Link::Block(hash)) => {
                            let target = BlockTarget::Hash(hash);
                            push_screen(state, block_screen(target.clone()));
                            request_block_detail(state, config, &tx, target);
                        }
                        None => {}
                    }
                }
                KeyCode::Down | KeyCode::Char('j') if matches!(state.screen, Screen::Mempool) => {
                    select_next_mempool_transaction(state);
                }
                KeyCode::Up | KeyCode::Char('k') if matches!(state.screen, Screen::Mempool) => {
                    select_previous_mempool_transaction(state);
                }
                code if matches!(state.screen, Screen::Mempool) && is_open_key(code) => {
                    open_selected_mempool_transaction(state, config, &tx);
                }
                KeyCode::Down | KeyCode::Char('j')
                    if matches!(state.screen, Screen::Marketplace) =>
                {
                    select_next_marketplace_offer(state);
                }
                KeyCode::Up | KeyCode::Char('k') if matches!(state.screen, Screen::Marketplace) => {
                    select_previous_marketplace_offer(state);
                }
                KeyCode::F(5) if matches!(state.screen, Screen::Dashboard) => {
                    let config = config.clone();
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        let result = load_dashboard_snapshot(&config)
                            .await
                            .map_err(|e| e.to_string());
                        let _ = tx.send(AppEvent::DashboardRefresh(result));
                    });
                }
                KeyCode::F(5) if matches!(state.screen, Screen::Marketplace) => {
                    state.marketplace.refresh_requested = true;
                }
                KeyCode::Char('r') if matches!(state.screen, Screen::Marketplace) => {
                    state.marketplace.descending = !state.marketplace.descending;
                    sort_marketplace_from_top(state);
                }
                KeyCode::Char('a') if matches!(state.screen, Screen::Block { .. }) => {
                    if let Screen::Block { target, .. } = &state.screen
                        && let Some(block) = state.store.block_for_target(target)
                        && let Some(hash) = state
                            .store
                            .block_hash_by_height
                            .get(&block.summary.height)
                            .cloned()
                    {
                        push_screen(state, block_screen(BlockTarget::Hash(hash.clone())));
                        request_block_detail(state, config, &tx, BlockTarget::Hash(hash));
                    }
                }
                code if matches!(state.screen, Screen::Marketplace) && is_open_key(code) => {
                    open_selected_marketplace_offer(state, config, &tx);
                }
                KeyCode::Down | KeyCode::Char('j') if matches!(state.screen, Screen::Dashboard) => {
                    move_dashboard_selection(&mut state.dashboard, 1);
                }
                KeyCode::Up | KeyCode::Char('k') if matches!(state.screen, Screen::Dashboard) => {
                    move_dashboard_selection(&mut state.dashboard, -1);
                }
                KeyCode::Tab | KeyCode::BackTab if matches!(state.screen, Screen::Dashboard) => {
                    state.dashboard.transactions_focused = !state.dashboard.transactions_focused;
                }
                code if matches!(state.screen, Screen::Dashboard) && is_open_key(code) => {
                    if state.dashboard.transactions_focused {
                        if let Some(entry) = state
                            .dashboard
                            .recent_transactions
                            .get(state.dashboard.selected_transaction)
                        {
                            let txid = entry.txid.clone();
                            push_screen(state, transaction_screen(txid.clone()));
                            if state.store.contains_transaction(&txid) {
                                request_transaction_context(state, config, &tx, &txid);
                            } else if state.loading_transactions.insert(txid.clone()) {
                                spawn_transaction_loader(config.clone(), tx.clone(), vec![txid]);
                            }
                        }
                        continue;
                    }
                    let Some(hash) = state.dashboard.selected_block_hash().cloned() else {
                        continue;
                    };
                    push_screen(
                        state,
                        Screen::Block {
                            target: BlockTarget::Hash(hash.clone()),
                            selected_tx: 0,
                            mode: ViewMode::Overview,
                            json: JsonState::new(),
                        },
                    );
                    request_block_detail(state, config, &tx, BlockTarget::Hash(hash));
                }
                KeyCode::Char('[') | KeyCode::Char('p')
                    if matches!(state.screen, Screen::Block { .. }) =>
                {
                    navigate_adjacent_block(state, config, &tx, -1);
                }
                KeyCode::Char(']') | KeyCode::Char('n')
                    if matches!(state.screen, Screen::Block { .. }) =>
                {
                    if reorg_ui::competing_children(state) {
                        investigation = reorg_ui::Panel::for_screen(config, state);
                    } else {
                        navigate_adjacent_block(state, config, &tx, 1);
                    }
                }
                KeyCode::Char('[') | KeyCode::Char('p')
                    if matches!(state.screen, Screen::Identity { .. }) =>
                {
                    navigate_adjacent_identity_version(state, -1);
                }
                KeyCode::Char(']') | KeyCode::Char('n')
                    if matches!(state.screen, Screen::Identity { .. }) =>
                {
                    navigate_adjacent_identity_version(state, 1);
                }
                KeyCode::Down | KeyCode::Char('j')
                    if matches!(
                        state.screen,
                        Screen::Block {
                            mode: ViewMode::Overview,
                            ..
                        }
                    ) =>
                {
                    select_next_transaction(state);
                }
                KeyCode::Up | KeyCode::Char('k')
                    if matches!(
                        state.screen,
                        Screen::Block {
                            mode: ViewMode::Overview,
                            ..
                        }
                    ) =>
                {
                    select_previous_transaction(state);
                }
                KeyCode::Down | KeyCode::Char('j')
                    if matches!(
                        state.screen,
                        Screen::Transaction {
                            mode: ViewMode::Overview,
                            ..
                        }
                    ) =>
                {
                    select_next_transaction_flow(state);
                }
                KeyCode::Up | KeyCode::Char('k')
                    if matches!(
                        state.screen,
                        Screen::Transaction {
                            mode: ViewMode::Overview,
                            ..
                        }
                    ) =>
                {
                    select_previous_transaction_flow(state);
                }
                KeyCode::Down | KeyCode::Char('j')
                    if matches!(
                        state.screen,
                        Screen::Address { .. }
                            | Screen::Identity {
                                mode: ViewMode::Overview,
                                ..
                            }
                    ) =>
                {
                    select_next_address_transaction(state);
                }
                KeyCode::Up | KeyCode::Char('k')
                    if matches!(
                        state.screen,
                        Screen::Address { .. }
                            | Screen::Identity {
                                mode: ViewMode::Overview,
                                ..
                            }
                    ) =>
                {
                    select_previous_address_transaction(state);
                }
                code if matches!(
                    state.screen,
                    Screen::Block {
                        mode: ViewMode::Overview,
                        ..
                    }
                ) && is_open_key(code) =>
                {
                    open_selected_transaction(state, config, &tx);
                }
                code if matches!(
                    state.screen,
                    Screen::Transaction {
                        mode: ViewMode::Overview,
                        ..
                    }
                ) && is_open_key(code) =>
                {
                    match open_selected_transaction_flow(state, config, &tx) {
                        FlowNavigation::Block { system, hash } => {
                            pending_protocol_block = Some((system, hash))
                        }
                        FlowNavigation::Handled => {}
                        FlowNavigation::Currency(id) => {
                            push_screen(
                                state,
                                currency_ui::screen(id.clone(), config.chain_cache_key()),
                            );
                            currency_ui::spawn_load(config.noninteractive(), tx.clone(), id, false);
                        }
                        FlowNavigation::Destination { system, address } => {
                            if let Some(system) =
                                system.or_else(|| config.chain.system_id.get().cloned())
                            {
                                pending_destination = Some((system, address));
                            } else {
                                state.notice = Some(TransientNotice {
                                    message: "Destination system is unresolved; reference retained in JSON".into(),
                                    until: Instant::now() + Duration::from_secs(8),
                                });
                            }
                        }
                        FlowNavigation::Outpoint(target) => {
                            if registry.configs.iter().any(|config| {
                                config.chain.system_id.get() == Some(&target.system_id)
                            }) {
                                pending_link = Some((target, false));
                            } else {
                                state.notice = Some(TransientNotice {
                                    message: format!(
                                        "No configured source for {}; reference retained in JSON",
                                        target.system_id
                                    ),
                                    until: Instant::now() + Duration::from_secs(8),
                                });
                            }
                        }
                        FlowNavigation::Trail(vout) => {
                            if let Screen::Transaction { txid, .. } = &state.screen
                                && let Some(transaction) = state.store.transaction(txid)
                                && let Some(mut panel) =
                                    DefiPanel::for_output(&transaction.raw_json, vout)
                            {
                                panel.start(config, &registry.configs, state);
                                defi = Some(panel);
                            }
                        }
                    }
                }
                code if matches!(
                    state.screen,
                    Screen::Address { .. }
                        | Screen::Identity {
                            mode: ViewMode::Overview,
                            ..
                        }
                ) && is_open_key(code) =>
                {
                    open_selected_address_transaction(state, config, &tx);
                }
                KeyCode::Char('e')
                    if matches!(
                        state.screen,
                        Screen::Transaction {
                            mode: ViewMode::Overview,
                            ..
                        }
                    ) =>
                {
                    marketplace_trace::search::start(state, config, &tx);
                }
                KeyCode::Char('b') if matches!(state.screen, Screen::Transaction { .. }) => {
                    open_transaction_block(state, config, &tx);
                }
                KeyCode::Tab | KeyCode::BackTab => toggle_view_mode(state),
                KeyCode::Down | KeyCode::Char('j') if current_json_document(state).is_some() => {
                    move_json_cursor(state, 1);
                }
                KeyCode::Up | KeyCode::Char('k') if current_json_document(state).is_some() => {
                    move_json_cursor(state, -1);
                }
                code if is_open_key(code) && current_json_document(state).is_some() => {
                    activate_json_entry(state, config, &tx);
                }
                KeyCode::Char(' ') if current_json_document(state).is_some() => {
                    toggle_json_node(state);
                }
                KeyCode::Char('s') if current_json_document(state).is_some() => {
                    start_save_prompt(state);
                }
                KeyCode::Char('y') if current_json_document(state).is_some() => {
                    yank_current_json_value(state, &tx);
                }
                _ => {}
            }
            continue;
        }
        let foreground_turn = input_needs_draw;
        let _updates = timing.measure("ui_update");
        let mut backlog = false;
        redraw |= bookmarks.has_updates();
        if let Some((bookmark, snapshot)) = bookmarks.poll() {
            let index = match sessions
                .iter()
                .position(|s| s.config.chain.system_id.get() == Some(&bookmark.system_id))
            {
                Some(index) => index,
                None => {
                    // A cached object remains navigable after its source configuration is removed.
                    // This placeholder has no usable transport and grants no permissions.
                    let mut config = sessions[0].config.clone();
                    config.chain.name = bookmark.system_id.to_string();
                    config.chain.system_id =
                        Arc::new(std::sync::OnceLock::from(bookmark.system_id.clone()));
                    config.daemon.config_file = None;
                    config.daemon.user = None;
                    config.daemon.password = None;
                    config.daemon.host = "127.0.0.1".into();
                    config.daemon.port = 0;
                    config.daemon.control = Arc::default();
                    config
                        .daemon
                        .control
                        .set(crate::sources::ConnectionState::Disabled);
                    let _ = config.daemon.control.expected.set((
                        bookmark.system_id.clone(),
                        crate::world::Network::from_testnet(config.chain.testnet),
                    ));
                    config.daemon.api_pool = Arc::default();
                    config.daemon.observations = None;
                    registry.configs.push(config.clone());
                    sessions.push(create_session(
                        config,
                        ObjectStore::default(),
                        Screen::Dashboard,
                        bookmark.system_id.to_string(),
                    ));
                    sessions.len() - 1
                }
            };
            let destination = &mut sessions[index];
            let depth = destination.state.screen_stack.len();
            match bookmark_ui::install(
                &mut destination.state,
                &destination.config,
                &registry.configs,
                &bookmark,
                snapshot,
            ) {
                Ok(()) => {
                    if let Screen::Currency(view) = &destination.state.screen {
                        currency_ui::spawn_cached(
                            destination.config.clone(),
                            destination.tx.clone(),
                            view.query.clone(),
                        );
                    }
                    if active != index {
                        cross_history.push((active, index, depth + 1));
                    }
                    sessions[active]
                        .config
                        .daemon
                        .control
                        .visible
                        .store(false, std::sync::atomic::Ordering::Release);
                    active = index;
                    sessions[active]
                        .config
                        .daemon
                        .control
                        .visible
                        .store(true, std::sync::atomic::Ordering::Release);
                }
                Err(error) => {
                    bookmarks.show_error(error.to_string());
                }
            }
            redraw = true;
        }
        if let Some(panel) = &mut defi
            && let Some(receiver) = &mut panel.receiver
            && let Ok(report) = receiver.try_recv()
        {
            let complete = !report.searching;
            panel.report = Some(report);
            if complete {
                panel.task = None;
                panel.receiver = None;
            }
            redraw = true;
        }
        if let Some(panel) = &mut defi
            && let Some(receiver) = &mut panel.pool_receiver
            && let Ok(result) = receiver.try_recv()
        {
            panel.pool_comparison = Some(result);
            panel.pool_receiver = None;
            panel.pool_task = None;
            redraw = true;
        }
        if consent
            .as_ref()
            .is_some_and(|request| request.reply.is_closed())
        {
            consent = None;
            redraw = true;
        }
        if let Some((target, once)) = pending_link.take() {
            let open_trail = pending_trail.as_ref() == Some(&target);
            if let Some(index) = registry
                .configs
                .iter()
                .position(|config| config.chain.system_id.get() == Some(&target.system_id))
            {
                let cached = sessions[index]
                    .state
                    .store
                    .transaction(&target.txid)
                    .is_some();
                let connection = &registry.configs[index];
                if !cached
                    && !open_trail
                    && !once
                    && connection.daemon.config_file.is_some()
                    && connection.daemon.control.state()
                        == crate::sources::ConnectionState::Disabled
                {
                    local_permission = Some((index, target));
                } else {
                    pending_output_focus.insert(index, target.clone());
                    let destination = &mut sessions[index];
                    let depth = destination.state.screen_stack.len();
                    let mut screen = transaction_screen(target.txid.clone());
                    if let Screen::Transaction { notarization, .. } = &mut screen {
                        *notarization = pending_notarization
                            .take()
                            .filter(|context| context.target == target);
                    }
                    push_screen(&mut destination.state, screen);
                    if !cached {
                        let mut request_config = if open_trail {
                            destination.config.noninteractive()
                        } else {
                            destination.config.clone()
                        };
                        if once {
                            request_config.daemon.control = Arc::default();
                            if let Some(id) = request_config.chain.system_id.get() {
                                let _ = request_config.daemon.control.expected.set((
                                    id.clone(),
                                    crate::world::Network::from_testnet(
                                        request_config.chain.testnet,
                                    ),
                                ));
                            }
                        }
                        destination
                            .state
                            .loading_transactions
                            .insert(target.txid.clone());
                        spawn_transaction_loader(
                            request_config,
                            destination.tx.clone(),
                            vec![target.txid],
                        );
                    }
                    if active != index {
                        cross_history.push((active, index, depth + 1));
                    }
                    sessions[active]
                        .config
                        .daemon
                        .control
                        .visible
                        .store(false, std::sync::atomic::Ordering::Release);
                    active = index;
                    // Viewing a foreign object permits on-demand work, never monitoring.
                    sessions[active]
                        .config
                        .daemon
                        .control
                        .visible
                        .store(true, std::sync::atomic::Ordering::Release);
                    links_open = false;
                }
            } else {
                source_message = format!(
                    "No connection configured for {}. Reference retained.",
                    target.system_id
                );
            }
            redraw = true;
        }

        if let Some((system, hash)) = pending_protocol_block.take()
            && let Some(index) = registry.configs.iter().position(|c| {
                c.chain.testnet == sessions[active].config.chain.testnet
                    && c.chain_cache_key() == system
            })
        {
            let destination = &mut sessions[index];
            let depth = destination.state.screen_stack.len();
            let target = BlockTarget::Hash(hash);
            push_screen(&mut destination.state, block_screen(target.clone()));
            request_block_detail(
                &mut destination.state,
                &destination.config.noninteractive(),
                &destination.tx,
                target,
            );
            if active != index {
                cross_history.push((active, index, depth + 1));
                sessions[active]
                    .config
                    .daemon
                    .control
                    .visible
                    .store(false, Ordering::Release);
                active = index;
                sessions[active]
                    .config
                    .daemon
                    .control
                    .visible
                    .store(true, Ordering::Release);
            }
            redraw = true;
        }

        if let Some(link) = pending_currency.take() {
            if let Some(index) = registry
                .configs
                .iter()
                .position(|config| config.chain.system_id.get() == Some(&link.system))
            {
                let destination = &mut sessions[index];
                let depth = destination.state.screen_stack.len();
                push_screen(
                    &mut destination.state,
                    currency_ui::screen(
                        link.currency.clone(),
                        destination.config.chain_cache_key(),
                    ),
                );
                let request_config = destination.config.noninteractive();
                if let Some(height) = link.height {
                    let request = currency_ui::request_id();
                    if let Screen::Currency(view) = &mut destination.state.screen {
                        view.history_request = Some(request);
                    }
                    currency_ui::spawn_at_height(
                        request_config,
                        destination.tx.clone(),
                        link.currency,
                        height,
                        request,
                    );
                } else {
                    currency_ui::spawn_load(
                        request_config,
                        destination.tx.clone(),
                        link.currency,
                        false,
                    );
                }
                if active != index {
                    cross_history.push((active, index, depth + 1));
                }
                sessions[active]
                    .config
                    .daemon
                    .control
                    .visible
                    .store(false, Ordering::Release);
                active = index;
                sessions[active]
                    .config
                    .daemon
                    .control
                    .visible
                    .store(true, Ordering::Release);
                defi = None;
            } else if let Some(panel) = &mut defi {
                panel.message = Some(format!("No source configured for {}", link.system));
            }
            redraw = true;
        }

        if let Some((system, address)) = pending_destination.take() {
            let witness_signer = matches!(sessions[active].state.screen, Screen::Witnesses(_));
            if let Some(index) = registry
                .configs
                .iter()
                .position(|config| config.chain.system_id.get() == Some(&system))
            {
                let target = identity_screen(address.clone(), None);
                let has_identity = matches!(&target, Screen::Identity { target, .. }
                    if sessions[index].state.store.identity_for_target(target).is_some());
                if witness_signer
                    && !has_identity
                    && registry.configs[index].daemon.control.state()
                        == crate::sources::ConnectionState::Disabled
                {
                    if let Screen::Witnesses(view) = &mut sessions[active].state.screen {
                        view.navigation_message = Some(format!(
                            "Signer identity unavailable. Enable the {} daemon in F2 Sources.",
                            registry.configs[index].chain.name
                        ));
                    }
                    redraw = true;
                    continue;
                }
                let destination = &mut sessions[index];
                let depth = destination.state.screen_stack.len();
                let screen = if witness_signer || address.starts_with('i') {
                    target
                } else {
                    address_screen(address.clone())
                };
                push_screen(&mut destination.state, screen);
                if !witness_signer && !address.starts_with('i') {
                    request_address_detail(
                        &mut destination.state,
                        &destination.config.noninteractive(),
                        &destination.tx,
                        &address,
                    );
                }
                if active != index {
                    cross_history.push((active, index, depth + 1));
                }
                sessions[active]
                    .config
                    .daemon
                    .control
                    .visible
                    .store(false, Ordering::Release);
                active = index;
                sessions[active]
                    .config
                    .daemon
                    .control
                    .visible
                    .store(true, Ordering::Release);
            } else if let Screen::Witnesses(view) = &mut sessions[active].state.screen {
                view.navigation_message = Some(format!(
                    "Signer identity unavailable. No source is configured for {system}."
                ));
            } else {
                sessions[active].state.notice = Some(TransientNotice {
                    message: format!(
                        "No configured source for {system}; destination remains available in JSON"
                    ),
                    until: Instant::now() + Duration::from_secs(8),
                });
            }
            redraw = true;
        }

        if !foreground_turn {
            let started = Instant::now();
            for offset in 0..sessions.len() {
                let index = (background_cursor + offset) % sessions.len();
                let session = &mut sessions[index];
                if session.config.daemon.control.state()
                    == crate::sources::ConnectionState::Monitored
                    && !session.state.mempool.loading
                    && session
                        .state
                        .mempool
                        .last_refresh
                        .is_none_or(|last| last.elapsed() >= Duration::from_secs(5))
                {
                    session.state.mempool.loading = true;
                    let mut background = session.config.clone();
                    background.daemon.background = true;
                    spawn_mempool_loader(background, session.tx.clone(), false);
                }
                redraw |= drain_app_events_until(
                    &mut session.state,
                    &mut session.rx,
                    started,
                    &timing,
                    || event::poll(Duration::ZERO).unwrap_or(true),
                ) > 0;
                if started.elapsed() >= APP_EVENT_BUDGET || event::poll(Duration::ZERO)? {
                    background_cursor = (index + 1) % sessions.len();
                    break;
                }
            }
            if event::poll(Duration::ZERO)? {
                pending_input = Some(read_terminal_event(&timing)?);
                continue;
            }
        }
        backlog |= sessions.iter().any(|session| !session.rx.is_empty());
        pending_output_focus.retain(|index, target| {
            let session = &mut sessions[*index];
            let config = &session.config;
            let state = &mut session.state;
            let Screen::Transaction {
                txid,
                selected_flow,
                ..
            } = &mut state.screen
            else {
                return false;
            };
            if *txid != target.txid {
                return false;
            }
            let Some(transaction) = state.store.transaction(txid) else {
                return state.loading_transactions.contains(txid);
            };
            let flows = transaction_flows(&transaction.raw_json, &state.store);
            if let Some(selection) = output_selection(&flows, target.vout) {
                *selected_flow = selection;
            } else {
                state.notice = Some(TransientNotice {
                    message: format!(
                        "Referenced output {} is absent from this transaction",
                        target.vout
                    ),
                    until: Instant::now() + Duration::from_secs(6),
                });
            }
            if pending_trail.as_ref() == Some(target) {
                if let Some(mut panel) = DefiPanel::for_output(&transaction.raw_json, target.vout) {
                    panel.start(config, &registry.configs, state);
                    defi = Some(panel);
                } else {
                    state.notice = Some(TransientNotice {
                        message: format!(
                            "Output {} is not a reserve transfer; no trail started",
                            target.vout
                        ),
                        until: Instant::now() + Duration::from_secs(8),
                    });
                }
                pending_trail = None;
            }
            redraw = true;
            false
        });
        if pending_trail.as_ref().is_some_and(|target| {
            !pending_output_focus
                .values()
                .any(|pending| pending == target)
        }) {
            pending_trail = None;
        }
        if consent.is_none() {
            consent = registry
                .consent_rx
                .try_recv()
                .ok()
                .map(api_permission_ui::Prompt::new);
            if consent.is_some() {
                redraw = true;
            }
        }
        let session_links = if links_open {
            session_relationships(sessions, active)
        } else {
            Vec::new()
        };
        let relationship_statuses: Vec<_> = session_links
            .iter()
            .map(|link| {
                sessions
                    .iter()
                    .find(|session| {
                        session.config.chain.system_id.get() == Some(&link.target.system_id)
                    })
                    .map(|session| {
                        let source = &sessions[active];
                        let Screen::Transaction { txid, .. } = &source.state.screen else {
                            return "Unresolved";
                        };
                        let Some(carrier) = source.state.store.transaction(txid) else {
                            return "Unresolved";
                        };
                        crate::relationships::link_status(
                            link,
                            source.config.chain.system_id.get().unwrap(),
                            &carrier.raw_json,
                            &link.target.system_id,
                            session
                                .state
                                .store
                                .transaction(&link.target.txid)
                                .map(|tx| &tx.raw_json),
                        )
                    })
                    .unwrap_or("No connection configured")
            })
            .collect();
        let source_names = source_chain_display_names(&registry.configs, sessions);
        let chain_name_requests = registry
            .configs
            .iter()
            .enumerate()
            .filter(|(index, _)| sources_open || *index == active)
            .filter_map(|(_, config)| config.chain.system_id.get())
            .filter(|id| {
                !sessions
                    .iter()
                    .any(|session| session.state.store.identity_name(id.as_ref()).is_some())
            })
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let current_config = sessions[current].config.clone();
        let current_tip = sessions[current].state.tip.clone();
        redraw |= update_witness_view(sessions, active);
        let witness_path = cross_history
            .last()
            .and_then(|&(origin, destination, depth)| {
                (destination == active)
                    .then(|| {
                        witness_breadcrumb(
                            &sessions[origin].state,
                            &sessions[active].state,
                            theme,
                            depth,
                        )
                    })
                    .flatten()
            });
        let date_tips: HashMap<_, _> = sessions
            .iter()
            .filter_map(|s| s.state.tip.height.map(|h| (s.config.chain_cache_key(), h)))
            .collect();
        let session = &mut sessions[active];
        let config = &session.config;
        let state = &mut session.state;
        let tx = session.tx.clone();

        if !foreground_turn {
            request_chain_identity_names(state, config, &tx, chain_name_requests);
            let activity_changed = std::mem::take(&mut activity_needs_update);
            if redraw || activity_changed {
                request_current_activity_identity_names(state, config, &tx);
            }
            chart_ui::drive(state, config, &tx, &mut chart_task);
            redraw |= height_dates.drive(
                theme,
                config,
                state.tip.height,
                &registry.configs,
                &date_tips,
            );
            if let Some(panel) = &mut investigation {
                let (changed, action) = panel.poll(state);
                redraw |= changed;
                if let Some(reorg_ui::Action::Open(row, raw)) = action {
                    panel.open(state, config, &tx, row, raw);
                    investigation = None;
                }
            }
            redraw |= token_control::request_current(state, config, &tx);
            if config.daemon.control.state() != crate::sources::ConnectionState::Disabled {
                initialize_identity_json(state);
                update_marketplace_sort(state);
                request_marketplace_age_context(state, config, &tx);
                request_identity_offers(state, config, &tx);
                let observed_height = match &state.screen {
                    Screen::Block { target, .. } => state
                        .store
                        .block_for_target(target)
                        .map(|b| b.summary.height),
                    Screen::Transaction { txid, .. } => {
                        state.store.transaction(txid).and_then(|t| t.block_height)
                    }
                    _ => None,
                };
                if let Some(height) = observed_height
                    && state
                        .store
                        .chain
                        .reconciled
                        .as_ref()
                        .is_some_and(|s| !s.canonical.contains_key(&height))
                {
                    config
                        .daemon
                        .control
                        .reconciliation_heights
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .insert(height);
                }
                request_current_transaction_refresh(state, config, &tx);
                if let Screen::Transaction { txid, .. } = &state.screen {
                    let txid = txid.clone();
                    request_transaction_context(state, config, &tx, &txid);
                    registration_payments::request(state, config, &tx, &txid);
                }
                request_dashboard_page_if_needed(state, config, &tx);
                request_mempool_transaction_hydration(state, config, &tx);
                request_currency_definitions(state, config, &tx);
                if let Some(report) = defi.as_ref().and_then(|panel| panel.report.as_ref()) {
                    let ids = report
                        .entries
                        .iter()
                        .flat_map(|entry| entry.detail.split(|c: char| !c.is_ascii_alphanumeric()))
                        .chain(
                            report
                                .notes
                                .iter()
                                .flat_map(|note| note.split(|c: char| !c.is_ascii_alphanumeric())),
                        )
                        .filter(|id| looks_like_identity_address(id))
                        .map(str::to_string)
                        .chain(report.legs.iter().flat_map(|leg| leg.currency_ids()))
                        .collect();
                    request_currency_ids(state, config, &tx, ids);
                }
                request_current_identity_dependencies(state, config, &tx);
                request_current_evidence_identity_names(state, config, &tx);
                request_current_address_identities(state, config, &tx);
                request_current_address_balance(state, config, &tx);
                request_current_address_utxos(state, config, &tx);
                request_current_address_transactions(state, config, &tx);
                request_current_address_history(state, config, &tx);
                request_current_address_output_states(state, config, &tx);
                request_mempool_refresh_if_needed(state, config, &tx);
                request_marketplace_refresh_if_needed(state, config, &tx);
                request_peer_refresh_if_needed(state, config, &tx);
                request_log_refresh_if_needed(state, &tx);
            }
        }

        let size = terminal.size()?;
        let body = main_layout(state, theme, size.into(), mouse_capture)[3];
        redraw |= notarization_ui::drive(state, config, &registry.configs, &tx, body.height);
        if sources_open {
            redraw |= source_activity.poll(&config.cache_path);
        }
        if !foreground_turn && event::poll(Duration::ZERO)? {
            pending_input = Some(read_terminal_event(&timing)?);
            continue;
        }
        drop(_updates);
        let dashboard_animating = matches!(state.screen, Screen::Dashboard)
            && state.auxiliary.is_none()
            && (!state.dashboard.transactions_focused || body.width >= 72)
            && state.dashboard.loading_older_since.is_some();
        let draw_interval = if dashboard_animating {
            DASHBOARD_LOADING_FRAME
        } else {
            Duration::from_secs(1)
        };
        if last_periodic_draw.elapsed() >= draw_interval
            || redraw
                && (foreground_turn
                    || !backlog
                    || last_periodic_draw.elapsed() >= Duration::from_millis(16))
        {
            let _timing = timing.measure("redraw");
            clamp_scroll_positions(
                state,
                config,
                theme,
                Rect::new(0, 0, size.width, size.height),
                mouse_capture,
            );
            let mut dashboard_transactions = None;
            terminal.draw(|frame| {
                let can_copy = mouse_capture
                    && !text_input_active(state)
                    && !links_open
                    && local_permission.is_none()
                    && defi.is_none()
                    && bookmarks.can_copy()
                    && investigation.is_none()
                    && consent.is_none()
                    && state.save_prompt.is_none()
                    && state.cache_clear_prompt.is_none()
                    && state.peers.controls.prompt.is_none();
                theme.copy_values.borrow_mut().begin(can_copy);
                dashboard_transactions = render(frame, config, theme, state, mouse_capture);
                if active != current {
                    let area = frame.area();
                    frame.render_widget(
                        Clear,
                        Rect::new(area.x, area.y, area.width, area.height.min(2)),
                    );
                    frame.render_widget(
                        Paragraph::new(header_line(
                            &current_config,
                            &source_names[current],
                            theme,
                            &current_tip,
                            current_unix_timestamp(),
                            area.width,
                        ))
                        .style(theme.header()),
                        Rect::new(area.x, area.y, area.width, 1),
                    );
                    frame.render_widget(
                        Paragraph::new(format!(
                            "Object system: {} ({})",
                            config.chain.name,
                            config.chain_cache_key()
                        ))
                        .style(theme.base()),
                        Rect::new(area.x, area.y + 1, area.width, 1),
                    );
                }
                if let Some(path) = &witness_path {
                    let area = frame.area();
                    let breadcrumb = Rect::new(area.x, area.y + 1, area.width, 1);
                    frame.render_widget(Clear, breadcrumb);
                    frame.render_widget(
                        Paragraph::new(breadcrumb_text(theme, path, area.width))
                            .style(theme.base()),
                        breadcrumb,
                    );
                }
                if links_open {
                    render_relationships(
                        frame,
                        theme,
                        &session_links,
                        &relationship_statuses,
                        link_selection,
                        &source_message,
                    );
                }

                if let Some(panel) = &defi {
                    render_scrolling_panel(
                        frame,
                        theme,
                        "DeFi trail — F8",
                        defi_lines(panel, &state.store, theme),
                        panel.scroll,
                        defi_controls(panel),
                    );
                }
                if sources_open {
                    source_ui::render(
                        frame,
                        theme,
                        registry,
                        &source_names,
                        current,
                        source_selection,
                        &source_message,
                        &mut source_activity,
                    );
                }
                if bookmarks.open {
                    bookmarks.render(
                        frame,
                        theme,
                        mouse_capture,
                        &registry.configs,
                        &source_names,
                    );
                } else if bookmarks.editing() {
                    let layout = main_layout(state, theme, frame.area(), mouse_capture);
                    bookmarks.render_add_popup(frame, theme, layout[3], layout[4]);
                }
                if let Some(panel) = &investigation {
                    panel.render(frame, theme);
                }
                if let Some((index, _)) = &local_permission {
                    let footer = main_layout(state, theme, frame.area(), mouse_capture)[4];
                    render_disabled_daemon(frame, theme, &registry.configs[*index], footer);
                }
                if let Some(request) = &consent {
                    let footer = main_layout(state, theme, frame.area(), mouse_capture)[4];
                    request.render(frame, theme, footer);
                }
                theme
                    .copy_values
                    .borrow_mut()
                    .finish(frame.buffer_mut(), theme.selection_cursor());
            })?;
            if let Some(visible) = dashboard_transactions
                && config.daemon.control.state() != crate::sources::ConnectionState::Disabled
            {
                request_dashboard_transaction_hydration(state, config, &tx, visible);
            }
            redraw = false;
            last_periodic_draw = Instant::now();
            bookmarks.start_after_draw(&registry.configs);
        }

        input_needs_draw = false;
        // An input frame is followed immediately by polling, so pending navigation
        // can schedule its work on the next idle turn.
        let _timing = timing.measure("terminal_wait");
        if pending_input.is_none()
            && event::poll(if backlog || foreground_turn {
                Duration::ZERO
            } else {
                Duration::from_millis(50)
            })?
        {
            pending_input = Some(read_terminal_event(&timing)?);
        }
    }

    for session in sessions.iter_mut() {
        cancel_search(&mut session.state);
    }
    Ok(())
}

fn request_dashboard_transaction_hydration(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
    visible: Range<usize>,
) {
    let first = state
        .dashboard
        .selected_block_index()
        .unwrap_or_default()
        .saturating_sub((DASHBOARD_BLOCK_COUNT / 2) as usize);
    let last = (first + DASHBOARD_BLOCK_COUNT as usize).min(state.dashboard.recent_blocks.len());
    let block_transactions = state
        .dashboard
        .recent_blocks
        .get(first..last)
        .unwrap_or_default()
        .iter()
        .filter_map(|hash| state.store.block(hash))
        .flat_map(|block| block.summary.tx.iter())
        .map(|summary| summary.txid.clone());
    let candidates = state.dashboard.recent_transactions[visible]
        .iter()
        .map(|entry| entry.txid.clone())
        .chain(block_transactions);
    let requested = loadable_transaction_ids(state, candidates);
    let _ = start_transaction_batch(state, config, tx, requested);
}

fn request_dashboard_page_if_needed(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
) {
    if !matches!(state.screen, Screen::Dashboard)
        || state.dashboard.loading_older_since.is_some()
        || !state.dashboard.has_more
        || !dashboard_should_load_older(&state.dashboard)
        || state
            .dashboard
            .older_retry_after
            .is_some_and(|retry_at| retry_at > Instant::now())
    {
        return;
    }

    let Some(oldest_height) = state
        .dashboard
        .recent_blocks
        .last()
        .and_then(|hash| state.store.block(hash))
        .map(|block| block.summary.height)
    else {
        return;
    };
    let Some(next_height) = oldest_height.checked_sub(1) else {
        state.dashboard.has_more = false;
        return;
    };

    let started = Instant::now();
    state.dashboard.loading_older_since = Some(started);
    state.dashboard.older_error = None;
    spawn_dashboard_page_loader(config.clone(), tx.clone(), next_height, started);
}

fn dashboard_should_load_older(dashboard: &DashboardState) -> bool {
    dashboard.selected_block_index().is_some_and(|index| {
        index
            >= dashboard
                .recent_blocks
                .len()
                .saturating_sub((DASHBOARD_BLOCK_COUNT / 2) as usize)
    })
}

fn request_mempool_transaction_hydration(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
) {
    if !matches!(state.screen, Screen::Mempool)
        || state.mempool.bulk_pending
        || state.mempool.loading
        || !state.loading_transactions.is_empty()
    {
        return;
    }
    let candidates = state
        .mempool
        .entries
        .iter()
        .filter(|entry| !state.mempool.previews.contains_key(&entry.txid))
        .map(|entry| entry.txid.clone())
        .collect::<Vec<_>>();
    let requested = loadable_transaction_ids(state, candidates);
    let _ = start_transaction_batch(state, config, tx, requested);
}

fn request_currency_definitions(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
) {
    let mut requested = Vec::new();
    if let Some(id) = &state.store.chain.chain_id
        && !state.store.currencies_by_id.contains_key(id)
        && !state.loading_currencies.contains(id)
        && !state.currency_load_attempted.contains(id)
    {
        requested.push(id.clone());
    }
    requested.extend(state.store.currency_references.iter().cloned());
    if let Screen::Currency(view) = &state.screen {
        requested.extend(defi_currency_ids(&view.document));
    }
    for offer in &state.marketplace.offers {
        for asset in [&offer.offered, &offer.accepted] {
            for currency_id in asset
                .as_object()
                .into_iter()
                .flat_map(|fields| fields.iter())
                .filter(|(_, amount)| amount.as_f64().is_some())
                .map(|(currency_id, _)| currency_id.clone())
            {
                if !state.store.currencies_by_id.contains_key(&currency_id)
                    && !state.loading_currencies.contains(&currency_id)
                    && !state.currency_load_attempted.contains(&currency_id)
                    && !requested.contains(&currency_id)
                {
                    requested.push(currency_id);
                }
            }
        }
    }
    request_currency_ids(state, config, tx, requested);
}

fn request_currency_ids(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
    ids: Vec<String>,
) {
    let limit = 24usize.saturating_sub(state.loading_currencies.len());
    let requested: Vec<_> = ids
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|id| {
            !state.store.currencies_by_id.contains_key(id)
                && (state.store.currency_name(id).is_none() || *id == config.chain_cache_key())
                && !state.loading_currencies.contains(id)
                && !state.currency_load_attempted.contains(id)
        })
        .take(limit)
        .collect();
    if requested.is_empty() {
        return;
    }
    state.loading_currencies.extend(requested.iter().cloned());
    state
        .currency_load_attempted
        .extend(requested.iter().cloned());
    spawn_currency_loader(config.clone(), tx.clone(), requested);
}

fn toggle_help(state: &mut AppState, config: &Config) {
    if matches!(state.auxiliary, Some(AuxiliaryScreen::Help { .. })) {
        state.auxiliary = state.help_return.take();
    } else {
        state.cache_size_bytes = cache_disk_usage(&config.cache_path).ok();
        state.help_return = state.auxiliary.take();
        state.auxiliary = Some(AuxiliaryScreen::Help { scroll: 0 });
    }
}

fn jump_to_dashboard(state: &mut AppState) {
    state.auxiliary = None;
    state.help_return = None;
    state.search.active = false;
    cancel_search(state);
    state.search.status = None;
    set_navigation_root(state, Screen::Dashboard);
}

fn open_mempool_root(state: &mut AppState) {
    cancel_search(state);
    state.search.status = None;
    if !matches!(state.screen, Screen::Mempool) {
        state.mempool.bulk_pending = true;
    }
    set_navigation_root(state, Screen::Mempool);
}

fn open_marketplace_root(state: &mut AppState) {
    cancel_search(state);
    state.search.status = None;
    set_navigation_root(state, Screen::Marketplace);
}

fn toggle_peer_interface(state: &mut AppState) {
    let peer_is_open = matches!(
        state.auxiliary,
        Some(AuxiliaryScreen::PeerBrowser | AuxiliaryScreen::PeerDetail { .. })
    ) || matches!(
        state.help_return,
        Some(AuxiliaryScreen::PeerBrowser | AuxiliaryScreen::PeerDetail { .. })
    );
    if peer_is_open {
        state.auxiliary = None;
        state.help_return = None;
        return;
    }
    if !state.tip.healthy {
        state.notice = Some(TransientNotice {
            message: "Peer browser requires a working local daemon RPC connection".to_string(),
            until: Instant::now() + Duration::from_secs(4),
        });
        return;
    }
    state.auxiliary = Some(AuxiliaryScreen::PeerBrowser);
    state.peers.last_refresh = None;
}

fn close_auxiliary(state: &mut AppState) {
    if matches!(state.auxiliary, Some(AuxiliaryScreen::Help { .. })) {
        state.auxiliary = state.help_return.take();
    } else {
        state.auxiliary = None;
    }
}

fn handle_auxiliary_key(
    state: &mut AppState,
    theme: &Theme,
    config: &Config,
    event_tx: &UnboundedSender<AppEvent>,
    key: KeyCode,
    page_height: u16,
) {
    if key == KeyCode::Char('y') && matches!(state.auxiliary, Some(AuxiliaryScreen::Help { .. })) {
        request_clipboard_copy(
            state,
            event_tx,
            "test phrase (paste locally to verify)".into(),
            crate::clipboard::TEST_TEXT.into(),
        );
        return;
    }
    if key == KeyCode::Esc {
        close_auxiliary(state);
        return;
    }

    if matches!(
        state.auxiliary,
        Some(AuxiliaryScreen::PeerDetail {
            mode: ViewMode::Json,
            ..
        })
    ) {
        match key {
            KeyCode::Tab | KeyCode::BackTab => {
                if let Some(AuxiliaryScreen::PeerDetail { mode, .. }) = state.auxiliary.as_mut() {
                    *mode = ViewMode::Overview;
                }
            }
            KeyCode::Down | KeyCode::Char('j') => move_json_cursor(state, 1),
            KeyCode::Up | KeyCode::Char('k') => move_json_cursor(state, -1),
            KeyCode::Home => move_json_cursor_to_boundary(state, false),
            KeyCode::End => move_json_cursor_to_boundary(state, true),
            KeyCode::PageDown => move_json_cursor(state, page_height.max(1) as isize),
            KeyCode::PageUp => move_json_cursor(state, -(page_height.max(1) as isize)),
            KeyCode::Enter | KeyCode::Right | KeyCode::Char('l') | KeyCode::Char(' ') => {
                toggle_json_node(state);
            }
            KeyCode::Char('s') => start_save_prompt(state),
            KeyCode::Char('y') => yank_current_json_value(state, event_tx),
            KeyCode::Char('[') | KeyCode::Char('p') => select_adjacent_peer(state, -1),
            KeyCode::Char(']') | KeyCode::Char('n') => select_adjacent_peer(state, 1),
            KeyCode::Left | KeyCode::Char('h') | KeyCode::Backspace => {
                state.auxiliary = Some(AuxiliaryScreen::PeerBrowser);
            }
            _ => {}
        }
        return;
    }

    match state.auxiliary.as_mut() {
        Some(AuxiliaryScreen::Help { scroll }) => match key {
            KeyCode::Char('t') => {
                let mut display = theme.time_display.get();
                display.toggle();
                theme.time_display.set(display);
            }
            KeyCode::Down | KeyCode::Char('j') => *scroll = scroll.saturating_add(1),
            KeyCode::Up | KeyCode::Char('k') => *scroll = scroll.saturating_sub(1),
            KeyCode::Home => *scroll = 0,
            KeyCode::End => *scroll = usize::MAX,
            KeyCode::PageDown => *scroll = scroll.saturating_add(page_height.max(1) as usize),
            KeyCode::PageUp => *scroll = scroll.saturating_sub(page_height.max(1) as usize),
            KeyCode::Char('c') => {
                let size_bytes = cache_disk_usage(&config.cache_path)
                    .ok()
                    .or(state.cache_size_bytes)
                    .unwrap_or_default();
                state.cache_size_bytes = Some(size_bytes);
                state.cache_clear_prompt = Some(CacheClearPrompt {
                    size_bytes,
                    clearing: false,
                });
            }
            _ => {}
        },
        Some(AuxiliaryScreen::PeerBrowser) => match key {
            KeyCode::Down | KeyCode::Char('j') => {
                state.peers.selected =
                    (state.peers.selected + 1).min(state.peers.records.len().saturating_sub(1));
            }
            KeyCode::Up | KeyCode::Char('k') => {
                state.peers.selected = state.peers.selected.saturating_sub(1);
            }
            KeyCode::Home => state.peers.selected = 0,
            KeyCode::End => {
                state.peers.selected = state.peers.records.len().saturating_sub(1);
            }
            KeyCode::PageDown => {
                let page = usize::from(page_height / PEER_TILE_HEIGHT).max(1);
                state.peers.selected = state
                    .peers
                    .selected
                    .saturating_add(page)
                    .min(state.peers.records.len().saturating_sub(1));
            }
            KeyCode::PageUp => {
                let page = usize::from(page_height / PEER_TILE_HEIGHT).max(1);
                state.peers.selected = state.peers.selected.saturating_sub(page);
            }
            KeyCode::Enter | KeyCode::Right | KeyCode::Char('l') => {
                if let Some(peer) = state.peers.records.get(state.peers.selected) {
                    state.auxiliary = Some(AuxiliaryScreen::PeerDetail {
                        peer_id: peer.id,
                        mode: ViewMode::Overview,
                        json: JsonState::new(),
                        scroll: 0,
                    });
                    state.log_feed.last_refresh = None;
                }
            }
            KeyCode::Char('w') if state.peers.geoip_database.is_some() => {
                state.peers.map_enabled = !state.peers.map_enabled;
            }
            KeyCode::Char('g') if !state.peers.geoip_downloading => {
                state.peers.geoip_downloading = true;
                spawn_geoip_download(state.peers.geoip_directory.clone(), event_tx.clone());
            }
            _ => {}
        },
        Some(AuxiliaryScreen::PeerDetail {
            peer_id,
            mode,
            json: _,
            scroll,
        }) => match key {
            KeyCode::Tab | KeyCode::BackTab => {
                *mode = if *mode == ViewMode::Overview {
                    ViewMode::Json
                } else {
                    ViewMode::Overview
                }
            }
            KeyCode::Char('[') | KeyCode::Char('p') => select_adjacent_peer(state, -1),
            KeyCode::Char(']') | KeyCode::Char('n') => select_adjacent_peer(state, 1),
            KeyCode::Left | KeyCode::Char('h') | KeyCode::Backspace => {
                state.auxiliary = Some(AuxiliaryScreen::PeerBrowser);
            }
            KeyCode::Down | KeyCode::Char('j') => *scroll = scroll.saturating_add(1),
            KeyCode::Up | KeyCode::Char('k') => *scroll = scroll.saturating_sub(1),
            KeyCode::Home => *scroll = 0,
            KeyCode::End => *scroll = usize::MAX,
            KeyCode::PageDown => *scroll = scroll.saturating_add(page_height.max(1) as usize),
            KeyCode::PageUp => *scroll = scroll.saturating_sub(page_height.max(1) as usize),
            _ => {
                let _ = peer_id;
            }
        },
        None => {}
    }
}

fn select_adjacent_peer(state: &mut AppState, delta: isize) {
    if state.peers.records.is_empty() {
        return;
    }
    let current = match state.auxiliary.as_ref() {
        Some(AuxiliaryScreen::PeerDetail { peer_id, .. }) => state
            .peers
            .records
            .iter()
            .position(|peer| peer.id == *peer_id)
            .unwrap_or(state.peers.selected),
        _ => return,
    };
    let next = current
        .saturating_add_signed(delta)
        .min(state.peers.records.len() - 1);
    state.peers.selected = next;
    if let Some(AuxiliaryScreen::PeerDetail {
        peer_id,
        json,
        scroll,
        ..
    }) = state.auxiliary.as_mut()
    {
        *peer_id = state.peers.records[next].id;
        *json = JsonState::new();
        *scroll = 0;
    }
}

fn request_peer_refresh_if_needed(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
) {
    let active = matches!(
        state.auxiliary,
        Some(AuxiliaryScreen::PeerBrowser | AuxiliaryScreen::PeerDetail { .. })
    );
    if !active || state.peers.loading {
        return;
    }
    let due = state
        .peers
        .last_refresh
        .is_none_or(|last| last.elapsed() >= Duration::from_secs(PEER_REFRESH_SECONDS));
    if due {
        state.peers.loading = true;
        spawn_peer_loader(
            config.clone(),
            tx.clone(),
            state.peers.controls.totals.is_none(),
        );
    }
}

fn request_mempool_refresh_if_needed(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
) {
    let needs_membership = match &state.screen {
        Screen::Mempool => true,
        Screen::Transaction { txid, .. } => state.store.transaction(txid).is_none_or(|record| {
            record
                .block_hash
                .as_deref()
                .and_then(|hash| state.store.chain.confirmations(hash))
                .is_none()
        }),
        _ => false,
    };
    if state.auxiliary.is_some() || !needs_membership || state.mempool.loading {
        return;
    }
    let full = matches!(state.screen, Screen::Mempool) && state.mempool.bulk_pending;
    let due = full
        || state
            .mempool
            .last_refresh
            .is_none_or(|last| last.elapsed() >= Duration::from_secs(MEMPOOL_REFRESH_SECONDS));
    if due {
        if full {
            state.mempool.bulk_pending = false;
        }
        state.mempool.loading = true;
        spawn_mempool_loader(config.clone(), tx.clone(), full);
    }
}

fn request_identity_offers(state: &mut AppState, config: &Config, tx: &UnboundedSender<AppEvent>) {
    if state.auxiliary.is_some() {
        return;
    }
    let Screen::Identity { target, .. } = &state.screen else {
        return;
    };
    let Some(record) = state.store.identity_for_target(target) else {
        return;
    };
    let identity = record.identity_address.clone();
    let entry = state
        .store
        .identity_offers
        .entry(identity.clone())
        .or_default();
    if entry.loading
        || entry
            .checked_at
            .is_some_and(|time| time.elapsed() < Duration::from_secs(10))
    {
        return;
    }
    entry.loading = true;
    let config = config.clone();
    let tx = revision_sender(&config, tx.clone());
    tokio::spawn(async move {
        let result = async {
            let client = RpcClient::for_config(&config)?.noninteractive();
            let output = client.get_offers(&identity, false).await?;
            if !output.result.is_object() {
                anyhow::bail!("Identity offer index is unavailable");
            }
            Ok(parse_marketplace_offers(&output.result))
        }
        .await
        .map_err(|error: anyhow::Error| error.to_string());
        let _ = tx.send(AppEvent::IdentityOffers { identity, result });
    });
}

fn identity_offer_labels(state: &AppState, identity: &str) -> Vec<&'static str> {
    let Some(entry) = state.store.identity_offers.get(identity) else {
        return Vec::new();
    };
    if !state.tip.healthy
        || entry.error.is_some()
        || entry
            .checked_at
            .is_none_or(|time| time.elapsed() > Duration::from_secs(30))
    {
        return Vec::new();
    }
    let mut buy = false;
    let mut sell = false;
    for offer in &entry.offers {
        if state
            .tip
            .height
            .is_some_and(|height| offer.expiry_height != 0 && offer.expiry_height <= height)
        {
            continue;
        }
        sell |= offer.offered.get("identityid").and_then(Value::as_str) == Some(identity);
        buy |= offer.accepted.get("identityid").and_then(Value::as_str) == Some(identity);
    }
    let mut labels = Vec::new();
    if buy {
        labels.push("Buy offers");
    }
    if sell {
        labels.push("Sell offers");
    }
    labels
}

fn request_marketplace_refresh_if_needed(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
) {
    if state.marketplace.loading || !matches!(state.screen, Screen::Marketplace) {
        return;
    }
    if state.marketplace.attempted && !state.marketplace.refresh_requested {
        return;
    }
    let Some(tip) = state.dashboard.recent_blocks.first().cloned() else {
        return;
    };
    state.marketplace.loading = true;
    state.marketplace.attempted = true;
    state.marketplace.refresh_requested = false;
    spawn_marketplace_loader(
        config.clone(),
        tx.clone(),
        state.marketplace.subject.clone(),
        tip,
    );
}

fn request_log_refresh_if_needed(state: &mut AppState, tx: &UnboundedSender<AppEvent>) {
    if !matches!(state.auxiliary, Some(AuxiliaryScreen::PeerDetail { .. }))
        || state.log_feed.loading
    {
        return;
    }
    let due = state
        .log_feed
        .last_refresh
        .is_none_or(|last| last.elapsed() >= Duration::from_millis(LOG_REFRESH_MILLISECONDS));
    if !due {
        return;
    }
    let Some(path) = state.log_feed.path.clone() else {
        state.log_feed.error =
            Some("No daemon configuration file is available to locate debug.log.".to_string());
        state.log_feed.last_refresh = Some(Instant::now());
        return;
    };
    state.log_feed.loading = true;
    spawn_log_reader(path, state.log_feed.offset, tx.clone());
}

fn is_open_key(code: KeyCode) -> bool {
    matches!(code, KeyCode::Enter | KeyCode::Right | KeyCode::Char('l'))
}

fn is_back_key(code: KeyCode) -> bool {
    matches!(
        code,
        KeyCode::Left | KeyCode::Char('h') | KeyCode::Esc | KeyCode::Backspace
    )
}

fn cancel_search(state: &mut AppState) {
    state.search.pending_request = None;
    if let Some(task) = state.search.task.take() {
        task.abort();
    }
}

fn begin_search(state: &mut AppState) {
    cancel_search(state);
    state.search.active = true;
    state.search.query.clear();
    state.search.status = None;
}

/// Only the base view's currently visible text-entry field suppresses q.
fn quit_input_active(state: &AppState) -> bool {
    if let Some(prompt) = &state.peers.controls.prompt {
        return matches!(prompt, peer_controls::Prompt::Add { .. });
    }
    if state.cache_clear_prompt.is_some() {
        return false;
    }
    if state.save_prompt.is_some() {
        return true;
    }
    if state.auxiliary.is_some() {
        return false;
    }
    if state.search.active {
        return true;
    }
    match &state.screen {
        Screen::Currency(view) => view.height_input.is_some(),
        Screen::Identity {
            historical_height_input,
            ..
        } => historical_height_input.is_some(),
        Screen::Currencies(view) => view.editing && !view.options_open && !view.launch_open,
        Screen::Charts(_) => chart_ui::prompt_active(state),
        _ => false,
    }
}

/// Close the foremost base-view interface before considering navigation history.
fn close_base_interface(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
) -> bool {
    if state.peers.controls.prompt.take().is_some() {
        return true;
    } else if state.cache_clear_prompt.is_some() {
        handle_cache_clear_prompt_key(state, config, tx, KeyCode::Esc);
    } else if state.save_prompt.is_some() {
        handle_save_prompt_key(state, KeyCode::Esc);
    } else if state.auxiliary.is_some() {
        close_auxiliary(state);
    } else if state.search.active {
        handle_search_key(state, config, tx, KeyCode::Esc);
    } else if let Screen::Currencies(view) = &mut state.screen
        && view.edit_key(KeyCode::Esc)
    {
    } else if let Screen::Currency(view) = &mut state.screen
        && view.height_input.is_some()
    {
        view.height_input = None;
    } else if let Screen::Identity {
        historical_height_input: input,
        ..
    } = &mut state.screen
        && input.is_some()
    {
        *input = None;
    } else if chart_ui::prompt_active(state) {
        chart_ui::prompt_key(
            state,
            event::KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE),
        );
    } else if state.search.pending_request.is_some() {
        cancel_search(state);
        state.search.status = None;
    } else {
        return false;
    }
    true
}

fn navigate_back_or_dashboard(state: &mut AppState) {
    if !state.screen_stack.is_empty() {
        pop_screen(state);
    } else if !matches!(state.screen, Screen::Dashboard) {
        jump_to_dashboard(state);
    }
}

fn text_input_active(state: &AppState) -> bool {
    state.search.active
        || state.save_prompt.is_some()
        || matches!(&state.screen, Screen::Currency(view) if view.height_input.is_some())
        || matches!(
            &state.screen,
            Screen::Identity {
                historical_height_input: Some(_),
                ..
            }
        )
        || matches!(&state.screen, Screen::Currencies(view) if view.editing || view.options_open || view.launch_open)
        || chart_ui::prompt_active(state)
}

fn paste_into_active_input(state: &mut AppState, text: &str) {
    if let Some(prompt) = &mut state.peers.controls.prompt {
        prompt.paste(text);
    } else if state.save_prompt.is_some() {
        for ch in text.chars() {
            handle_save_prompt_key(state, KeyCode::Char(ch));
        }
    } else if state.search.active {
        state.search.query.push_str(text);
        state.search.status = None;
    } else if state.auxiliary.is_none() {
        match &mut state.screen {
            Screen::Currency(view) => {
                if let Some(input) = &mut view.height_input {
                    input.extend(
                        text.chars()
                            .filter(|ch| !ch.is_control())
                            .take(160_usize.saturating_sub(input.len())),
                    );
                }
            }
            Screen::Identity {
                historical_height_input: Some(input),
                ..
            } => input::paste_height(input, text),
            Screen::Currencies(view) => view.paste(text),
            Screen::Charts(_) => {
                chart_ui::paste_prompt(state, text);
            }
            _ => {}
        }
    }
}

fn handle_search_key(
    state: &mut AppState,
    config: &Config,
    event_tx: &UnboundedSender<AppEvent>,
    key: KeyCode,
) {
    match key {
        KeyCode::Esc => {
            cancel_search(state);
            state.search.active = false;
        }
        KeyCode::Enter => submit_search(state, config, event_tx),
        KeyCode::Backspace => {
            state.search.query.pop();
            state.search.status = None;
        }
        KeyCode::Char(ch) => {
            state.search.query.push(ch);
            state.search.status = None;
        }
        _ => {}
    }
}

fn submit_search(state: &mut AppState, config: &Config, event_tx: &UnboundedSender<AppEvent>) {
    let query = state.search.query.trim().to_string();
    if query.is_empty() {
        state.search.active = false;
        return;
    }
    if query.eq_ignore_ascii_case("currencies") || query == "currency:" {
        state.search.active = false;
        state.search.status = None;
        let screen = currency_browser::screen(&state.store);
        let request = if let Screen::Currencies(view) = &screen {
            view.request
        } else {
            unreachable!()
        };
        set_navigation_root(state, screen);
        currency_browser::spawn(config.clone(), event_tx.clone(), request);
        return;
    }
    if let Some(currency) = crate::resolver::currency_query(&query) {
        let currency = currency.to_string();
        state.search.active = false;
        state.search.status = None;
        set_navigation_root(
            state,
            currency_ui::screen(currency.clone(), config.chain_cache_key()),
        );
        currency_ui::spawn_load(config.clone(), event_tx.clone(), currency, false);
        return;
    }

    match classify_start_object(Some(&query)) {
        ObjectRoute::Estimate(input) => {
            // Freeze relative date targets at submission just like historical lookup.
            let target = crate::block_forecast::parse_target(&input).map(|target| match target {
                crate::block_forecast::Target::Height(h) => h.to_string(),
                crate::block_forecast::Target::Time(t) => {
                    chrono::DateTime::from_timestamp(t, 0).unwrap().to_rfc3339()
                }
            });
            match target {
                Ok(target) => {
                    start_daemon_search(state, config, event_tx, ObjectRoute::Estimate(target))
                }
                Err(error) => {
                    state.search.active = false;
                    state.search.status = Some(SearchStatus {
                        message: error.to_string(),
                        error: true,
                    });
                }
            }
        }
        ObjectRoute::BlockTime(time) => {
            match crate::block_time::parse(&time) {
                Ok(parsed) => {
                    // Freeze relative expressions at submission, not after RPC admission.
                    start_daemon_search(
                        state,
                        config,
                        event_tx,
                        ObjectRoute::BlockTime(parsed.to_rfc3339()),
                    );
                    state.search.status = Some(SearchStatus {
                        message: format!(
                            "Finding block near {} (UTC); Esc cancels",
                            parsed.to_rfc3339()
                        ),
                        error: false,
                    });
                }
                Err(error) => {
                    state.search.active = false;
                    state.search.status = Some(SearchStatus {
                        message: error.to_string(),
                        error: true,
                    });
                }
            }
        }
        ObjectRoute::Tip => {
            state.search.active = false;
            state.search.status = None;
            set_navigation_root(state, Screen::Dashboard);
        }
        ObjectRoute::Address(address) => {
            state.search.active = false;
            state.search.status = None;
            set_navigation_root(state, address_screen(address.clone()));
            request_address_detail(state, config, event_tx, &address);
        }
        ObjectRoute::Identity(identity) => {
            let target = IdentityTarget {
                query: identity.clone(),
                height: None,
            };
            if state
                .store
                .identity_for_target(&target)
                .is_some_and(|version| {
                    !state
                        .store
                        .stale_identities
                        .contains(&version.identity_address)
                })
            {
                cancel_search(state);
                state.search.active = false;
                state.search.status = None;
                set_navigation_root(state, identity_screen(identity, None));
            } else {
                start_daemon_search(state, config, event_tx, ObjectRoute::Identity(identity));
            }
        }
        ObjectRoute::BlockHeight(height) => {
            let target = BlockTarget::Height(height);
            if let Some(hash) = state
                .store
                .block_for_target(&target)
                .map(|block| block.summary.hash.clone())
            {
                state.search.active = false;
                state.search.status = None;
                let target = BlockTarget::Hash(hash);
                push_block_search_result(state, target.clone());
                request_block_detail(state, config, event_tx, target);
            } else {
                start_daemon_search(state, config, event_tx, ObjectRoute::BlockHeight(height));
            }
        }
        ObjectRoute::Hash(hash) => match cached_hash_match(&state.store, &hash) {
            Some(CachedHashMatch::Transaction) => {
                state.search.active = false;
                state.search.status = None;
                push_transaction_search_result(state, hash);
            }
            Some(CachedHashMatch::Block) => {
                state.search.active = false;
                state.search.status = None;
                let target = BlockTarget::Hash(hash);
                push_block_search_result(state, target.clone());
                request_block_detail(state, config, event_tx, target);
            }
            None => {
                start_daemon_search(state, config, event_tx, ObjectRoute::Hash(hash));
            }
        },
        ObjectRoute::Unknown(_) => {
            state.search.status = Some(SearchStatus {
                message: "unrecognized block, transaction, address, or identity".to_string(),
                error: true,
            });
        }
    }
}

fn cached_hash_match(store: &ObjectStore, hash: &str) -> Option<CachedHashMatch> {
    if store.contains_transaction(hash) {
        Some(CachedHashMatch::Transaction)
    } else if store.contains_block(hash) {
        Some(CachedHashMatch::Block)
    } else {
        None
    }
}

fn start_daemon_search(
    state: &mut AppState,
    config: &Config,
    event_tx: &UnboundedSender<AppEvent>,
    route: ObjectRoute,
) {
    cancel_search(state);
    let request_id = state.search.next_request;
    state.search.next_request = state.search.next_request.wrapping_add(1);
    state.search.pending_request = Some(request_id);
    state.search.active = false;
    state.search.status = Some(SearchStatus {
        message: "resolving with local daemon".to_string(),
        error: false,
    });
    state.search.task = Some(spawn_search_loader(
        config.clone(),
        event_tx.clone(),
        request_id,
        route,
    ));
}

fn push_block_search_result(state: &mut AppState, target: BlockTarget) {
    set_navigation_root(state, block_screen(target));
}

fn push_transaction_search_result(state: &mut AppState, txid: Txid) {
    set_navigation_root(state, transaction_screen(txid));
}

/// Leave queued updates for the next UI turn so input and drawing get time.
#[cfg(test)]
fn drain_app_events(state: &mut AppState, rx: &mut UnboundedReceiver<AppEvent>) -> usize {
    drain_app_events_until(
        state,
        rx,
        Instant::now(),
        &ui_timing::Timing::default(),
        || false,
    )
}

fn drain_app_events_until(
    state: &mut AppState,
    rx: &mut UnboundedReceiver<AppEvent>,
    started: Instant,
    timing: &ui_timing::Timing,
    mut input_ready: impl FnMut() -> bool,
) -> usize {
    let mut processed = 0;
    for _ in 0..APP_EVENT_BATCH_LIMIT {
        if started.elapsed() >= APP_EVENT_BUDGET || input_ready() {
            break;
        }
        let Ok(update) = rx.try_recv() else { break };
        let _timing = timing.measure(background_stage(&update));
        handle_app_event(state, update);
        processed += 1;
    }
    processed
}

fn background_stage(event: &AppEvent) -> &'static str {
    match event {
        AppEvent::Guarded { event, .. } | AppEvent::RevisionGuarded { event, .. } => {
            background_stage(event)
        }
        AppEvent::Reconciled(_) => "reconciliation",
        AppEvent::Transactions { .. } | AppEvent::TransactionRefresh { .. } => "transaction_update",
        AppEvent::Observation(_) => "daemon_observation",
        AppEvent::IdentityDetail { .. } | AppEvent::IdentityEvidence { .. } => "identity_update",
        _ => "background_event",
    }
}

fn can_change_flow_links(event: &AppEvent) -> bool {
    match event {
        AppEvent::Guarded { event, .. } | AppEvent::RevisionGuarded { event, .. } => {
            can_change_flow_links(event)
        }
        AppEvent::BlockDetail { .. }
        | AppEvent::Transactions { .. }
        | AppEvent::TransactionRefresh { .. }
        | AppEvent::DashboardRefresh(_)
        | AppEvent::DashboardPage { .. }
        | AppEvent::IdentityDetail { .. }
        | AppEvent::IdentityEvidence { .. }
        | AppEvent::MarketplaceRefresh { .. }
        | AppEvent::StoredRelationships { .. } => true,
        _ => false,
    }
}

fn handle_app_event(state: &mut AppState, app_event: AppEvent) {
    // Hydrating a preceding input can add address links. Keep the selected link
    // on its original flow instead of letting the flattened index move it.
    let selection = if can_change_flow_links(&app_event)
        && let Screen::Transaction {
            txid,
            selected_flow,
            ..
        } = &state.screen
    {
        state.store.transaction(txid).and_then(|record| {
            let flows = transaction_flows(&record.raw_json, &state.store);
            transaction_flow_selections(&flows)
                .get(*selected_flow)
                .copied()
                .map(|link| (txid.clone(), link))
        })
    } else {
        None
    };
    apply_app_event(state, app_event);
    if let Some((selected_txid, link)) = selection
        && let Screen::Transaction {
            txid,
            selected_flow,
            ..
        } = &mut state.screen
        && *txid == selected_txid
        && let Some(record) = state.store.transaction(txid)
    {
        let flows = transaction_flows(&record.raw_json, &state.store);
        if let Some(index) = transaction_flow_selections(&flows)
            .iter()
            .position(|item| *item == link)
        {
            *selected_flow = index;
        }
    }
}

fn apply_app_event(state: &mut AppState, app_event: AppEvent) {
    match app_event {
        AppEvent::NotarizationChanged => {}
        AppEvent::MarketplaceSearch {
            funding,
            request,
            page,
        } => {
            marketplace_trace::search::finish(state, funding, request, *page);
        }
        AppEvent::TokenControl {
            identity,
            request,
            status,
        } => {
            if let Some(lookup) = state.store.token_control.get_mut(&identity)
                && Arc::ptr_eq(&lookup.request, &request)
            {
                lookup.status = status;
            }
        }
        AppEvent::RegistrationPayments {
            key,
            request,
            report,
        } => {
            if let Some((current, status)) = state.store.registration_payments.get_mut(&key)
                && Arc::ptr_eq(current, &request)
            {
                *status = report.map_or(
                    registration_payments::Status::Unverified,
                    registration_payments::Status::Verified,
                );
            }
        }
        AppEvent::ChartSeeked { request, result } => chart_ui::apply_seek(state, request, result),
        AppEvent::ChartSeekProgress { request, message } => {
            chart_ui::seek_progress(state, request, message)
        }
        AppEvent::ChartUpdated {
            request,
            page,
            done,
            error,
        } => chart_ui::apply(state, request, page, done, error),
        AppEvent::ReconciliationStarted => {
            state.store.chain.reconciling = true;
        }
        AppEvent::RevisionGuarded {
            control,
            epoch,
            generation,
            event,
        } => {
            if control.generation() == generation
                && control
                    .branch_epoch
                    .load(std::sync::atomic::Ordering::Acquire)
                    == epoch
            {
                apply_app_event(state, *event);
            } else {
                match *event {
                    AppEvent::DashboardPage { started, .. }
                        if state.dashboard.loading_older_since == Some(started) =>
                    {
                        state.dashboard.loading_older_since = None;
                    }
                    AppEvent::SearchResolved { request_id, .. }
                        if state.search.pending_request == Some(request_id) =>
                    {
                        state.search.pending_request = None;
                        state.search.task = None;
                        state.search.status = Some(SearchStatus {
                            message: "Source changed during search; try again".into(),
                            error: true,
                        });
                    }
                    _ => {}
                }
            }
        }
        AppEvent::Reconciled(Ok(branch)) => {
            let previous = state.store.chain.reconciled.as_ref();
            let unchanged = previous.is_some_and(|current| Arc::ptr_eq(current, &branch));
            let incremental = previous.is_some_and(|current| {
                current.tip().is_some()
                    && current.revision.checked_add(1) == Some(branch.revision)
                    && !branch.changed_hashes.is_empty()
            });
            state.store.chain.apply_reconciliation(branch.clone());
            if unchanged {
                return;
            }
            let reorg = branch.detached_count != 0;
            state
                .store
                .project_transaction_inclusions(&branch, incremental);
            if reorg {
                invalidate_reorg_views(state);
                rebuild_dashboard_transactions(state);
                state.notice = Some(TransientNotice {
                    message: format!(
                        "Reorg: {} blocks detached, {} adopted",
                        branch.detached_count, branch.attached_count
                    ),
                    until: Instant::now() + Duration::from_secs(15),
                });
            }
        }
        AppEvent::Reconciled(Err(error)) => {
            state.store.chain.reconciling = false;
            state.store.chain.online = false;
            state.notice = Some(TransientNotice {
                message: format!("Reconciliation incomplete: {error}"),
                until: Instant::now() + Duration::from_secs(10),
            });
        }
        AppEvent::Guarded {
            control,
            generation,
            event,
        } => {
            if control.accepts(generation) {
                apply_app_event(state, *event);
            } else {
                match *event {
                    AppEvent::MempoolRefresh(_) => state.mempool.loading = false,
                    AppEvent::Peers(_) => {
                        state.peers.loading = false;
                        state.peers.controls.refresh_after_action = false;
                    }
                    AppEvent::PeerAction(_) => state.peers.controls.busy = false,
                    _ => {}
                }
            }
        }

        AppEvent::Observation(observation) => {
            if matches!(
                observation.method.as_str(),
                "getaddressbalance" | "getcurrency" | "getcurrencystate"
            ) {
                state.store.learn_currency_data(&observation.result);
            }
            state.store.chain.observe(observation);
            state.tip.height = state.store.chain.height.or(state.tip.height);
            state.tip.longest_chain = state.store.chain.longest.or(state.tip.longest_chain);
            state.tip.last_block_time = state
                .store
                .chain
                .last_block_time
                .or(state.tip.last_block_time);
            state.tip.healthy = state.store.chain.online;
        }
        AppEvent::DashboardRefresh(Ok(snapshot)) => {
            state.tip = snapshot.tip;
            if state.store.chain.height.is_some() {
                state.tip.height = state.store.chain.height;
                state.tip.longest_chain = state.store.chain.longest;
                state.tip.last_block_time = state
                    .store
                    .chain
                    .last_block_time
                    .or(state.tip.last_block_time);
            }
            merge_dashboard_blocks(state, snapshot.blocks);
        }
        AppEvent::DashboardRefresh(Err(error)) => {
            state.store.chain.online = false;
            state.tip = ChainTipStatus {
                label: format!("tip unavailable: {error}"),
                healthy: false,
                height: state.tip.height,
                longest_chain: state.tip.longest_chain,
                last_block_time: state.tip.last_block_time,
                connections: state.tip.connections,
            };
        }
        AppEvent::DashboardPage { started, result } => {
            if state.dashboard.loading_older_since != Some(started) {
                return;
            }
            state.dashboard.loading_older_since = None;
            match result {
                Ok(blocks) => {
                    let reached_genesis =
                        blocks.iter().map(|block| block.summary.height).min() == Some(0);
                    state.dashboard.has_more = !blocks.is_empty() && !reached_genesis;
                    state.dashboard.older_retry_after = None;
                    state.dashboard.older_error = None;
                    merge_dashboard_blocks(state, blocks);
                }
                Err(error) => {
                    state.dashboard.older_retry_after =
                        Some(Instant::now() + Duration::from_secs(LOAD_RETRY_SECONDS));
                    state.dashboard.older_error = Some(error.clone());
                    state.notice = Some(TransientNotice {
                        message: format!("Older block load failed: {error}"),
                        until: Instant::now() + Duration::from_secs(4),
                    });
                }
            }
        }
        AppEvent::MempoolRefresh(result) => {
            state.mempool.loading = false;
            state.mempool.last_refresh = Some(Instant::now());
            match result {
                Ok(snapshot) => {
                    let selected_txid = state
                        .mempool
                        .entries
                        .get(state.mempool.selected)
                        .map(|entry| entry.txid.clone());
                    let members: HashSet<_> = snapshot
                        .entries
                        .iter()
                        .map(|entry| entry.txid.as_str())
                        .collect();
                    state
                        .mempool
                        .previews
                        .retain(|txid, _| members.contains(txid.as_str()));
                    for preview in snapshot.previews.values() {
                        if let Some(raw) = &preview.raw {
                            state.store.learn_currency_data(raw);
                        }
                    }
                    state.mempool.previews.extend(snapshot.previews);
                    state.mempool.entries = snapshot.entries;
                    state.mempool.total_bytes = snapshot.total_bytes;
                    state.mempool.selected = selected_txid
                        .and_then(|txid| {
                            state
                                .mempool
                                .entries
                                .iter()
                                .position(|entry| entry.txid == txid)
                        })
                        .unwrap_or(0)
                        .min(state.mempool.entries.len().saturating_sub(1));
                    state.mempool.error = None;
                }
                Err(error) => state.mempool.error = Some(error),
            }
        }
        AppEvent::IdentityOffers { identity, result } => {
            let entry = state.store.identity_offers.entry(identity).or_default();
            entry.loading = false;
            entry.checked_at = Some(Instant::now());
            match result {
                Ok(offers) => {
                    entry.offers = offers;
                    entry.error = None;
                }
                Err(error) => {
                    entry.error = Some(error);
                }
            }
        }
        AppEvent::MarketplaceRefresh { tip, result } => {
            state.marketplace.loading = false;
            state.marketplace.last_checked_tip = Some(tip);
            match result {
                Ok(snapshot) => {
                    let selected_txid = state
                        .marketplace
                        .offers
                        .get(state.marketplace.selected)
                        .map(|offer| offer.txid.clone());
                    state.marketplace.subject = snapshot.subject;
                    state.marketplace.offers = snapshot.offers;
                    for offer in &state.marketplace.offers {
                        state
                            .store
                            .add_marketplace_tag(offer.txid.clone(), MarketplaceTag::Offer);
                    }
                    state.marketplace.selected = selected_txid
                        .and_then(|txid| {
                            state
                                .marketplace
                                .offers
                                .iter()
                                .position(|offer| offer.txid == txid)
                        })
                        .unwrap_or(0)
                        .min(state.marketplace.offers.len().saturating_sub(1));
                    restore_marketplace_link(state);
                    state.marketplace.error = None;
                }
                Err(error) => state.marketplace.error = Some(error),
            }
        }
        AppEvent::BlockDetail { key, result } => {
            state.loading_blocks.remove(&key);
            if let Ok(block) = result {
                state.store.insert_block(block);
            }
        }
        AppEvent::TransactionRefresh { txid, tip, result } => {
            state.loading_transactions.remove(&txid);
            match result {
                Ok(record) => {
                    state.transaction_retry_after.remove(&txid);
                    state.transaction_verified_tip.insert(txid.clone(), tip);
                    // This response was explicitly fetched live, including mutable metadata.
                    state.store.transactions_by_txid.remove(&txid);
                    state.store.insert_transaction(record);
                }
                Err(error) => {
                    state.transaction_retry_after.insert(
                        txid,
                        Instant::now() + Duration::from_secs(LOAD_RETRY_SECONDS),
                    );
                    state.notice = Some(TransientNotice {
                        message: format!("Transaction refresh failed: {error}"),
                        until: Instant::now() + Duration::from_secs(4),
                    });
                }
            }
        }
        AppEvent::Transactions {
            requested,
            records,
            failed,
        } => {
            for txid in &requested {
                state.loading_transactions.remove(txid);
            }
            for record in records {
                if record.block_hash.is_none() {
                    state.transaction_retry_after.insert(
                        record.summary.txid.clone(),
                        Instant::now() + Duration::from_secs(LOAD_RETRY_SECONDS),
                    );
                } else {
                    state.transaction_retry_after.remove(&record.summary.txid);
                }
                state.store.insert_transaction(record);
            }
            let retry_at = Instant::now() + Duration::from_secs(LOAD_RETRY_SECONDS);
            for txid in failed {
                state.transaction_retry_after.insert(txid, retry_at);
            }
        }
        AppEvent::AddressDetail { address, result } => {
            state.loading_addresses.remove(&address);
            match result {
                Ok(result) => {
                    state.address_errors.remove(&address);
                    state.store.stale_addresses.remove(&address);
                    state.address_retry_after.remove(&address);
                    insert_address_load(&mut state.store, result);
                }
                Err(error) => {
                    state.address_errors.insert(address.clone(), error.clone());
                    state.address_retry_after.insert(
                        address,
                        Instant::now() + Duration::from_secs(LOAD_RETRY_SECONDS),
                    );
                    state.notice = Some(TransientNotice {
                        message: format!("Address load failed: {error}"),
                        until: Instant::now() + Duration::from_secs(4),
                    });
                }
            }
        }
        AppEvent::AddressHistory { address, result } => {
            state.loading_address_histories.remove(&address);
            match result {
                Ok(all_txids) => {
                    state.address_history_errors.remove(&address);
                    state.address_history_retry_after.remove(&address);
                    if let Some(record) = state.store.address_mut(&address) {
                        record.transaction_count = all_txids.len();
                        record.transaction_ids = all_txids
                            .iter()
                            .rev()
                            .take(ADDRESS_TRANSACTION_LIMIT)
                            .cloned()
                            .collect();
                        record.first_txid = all_txids.first().cloned();
                        record.latest_txid = all_txids.last().cloned();
                        record.history_complete = true;
                    }
                }
                Err(error) => {
                    state.address_history_errors.insert(address.clone(), error);
                    state.address_history_retry_after.insert(
                        address,
                        Instant::now() + Duration::from_secs(LOAD_RETRY_SECONDS),
                    );
                }
            }
        }
        AppEvent::AddressUtxos { address, result } => {
            state.loading_address_utxos.remove(&address);
            match result {
                Ok(unspent_outpoints) => {
                    state.address_utxo_errors.remove(&address);
                    state.address_utxo_retry_after.remove(&address);
                    for outpoint in &unspent_outpoints {
                        state.store.output_unspent.insert(outpoint.clone(), true);
                    }
                    if let Some(record) = state.store.address_mut(&address) {
                        record.unspent_outpoints = unspent_outpoints;
                        record.utxos_complete = true;
                    }
                }
                Err(error) => {
                    state.address_utxo_errors.insert(address.clone(), error);
                    state.address_utxo_retry_after.insert(
                        address,
                        Instant::now() + Duration::from_secs(LOAD_RETRY_SECONDS),
                    );
                }
            }
        }
        AppEvent::AddressBalance { address, result } => {
            state.loading_address_balances.remove(&address);
            match result {
                Ok(balance_response) => {
                    state.address_balance_errors.remove(&address);
                    state.address_balance_retry_after.remove(&address);
                    if let Some(record) = state.store.address_mut(&address) {
                        record.balance_response = Some(balance_response);
                    }
                }
                Err(error) => {
                    state.address_balance_errors.insert(address.clone(), error);
                    state.address_balance_retry_after.insert(
                        address,
                        Instant::now() + Duration::from_secs(LOAD_RETRY_SECONDS),
                    );
                }
            }
        }
        AppEvent::IdentityDetail { query, result } => {
            state.loading_identities.remove(&query);
            match result {
                Ok(result) => {
                    insert_identity_load(&mut state.store, result);
                }
                Err(error) => {
                    state.notice = Some(TransientNotice {
                        message: format!("Identity load failed: {error}"),
                        until: Instant::now() + Duration::from_secs(4),
                    });
                }
            }
        }
        AppEvent::IdentityEvidence {
            address,
            height,
            cached,
            result,
        } => {
            if !cached {
                state.store.loading_identity_evidence.remove(&address);
            }
            match result {
                Ok(mut report) => {
                    for transaction in &report.transactions {
                        state.store.insert_transaction(transaction.clone());
                    }
                    for (other_address, (_, other_report)) in &mut state.store.identity_evidence {
                        if other_address != &address {
                            identity_evidence::connect_shared_evidence(
                                &address,
                                &mut report,
                                other_address,
                                other_report,
                            );
                        }
                    }
                    state
                        .store
                        .identity_evidence
                        .insert(address, (height, report));
                }
                Err(error) => {
                    if let Some((_, report)) = state.store.identity_evidence.get_mut(&address) {
                        report.notes.push(format!("Refresh unavailable: {error}"));
                        return;
                    }
                    let mut report = identity_evidence::IdentityEvidence::default();
                    report.notes.push(format!("Evidence unavailable: {error}"));
                    state
                        .store
                        .identity_evidence
                        .insert(address, (height, report));
                }
            }
        }
        AppEvent::IdentityHistoricalEvidence {
            address,
            height,
            result,
        } => {
            state
                .store
                .loading_historical_identity_evidence
                .remove(&address);
            if let Some((loaded_height, report)) = state.store.identity_evidence.get_mut(&address)
                && *loaded_height == height
            {
                match result {
                    Ok(updated) => *report = updated,
                    Err(error) => report
                        .notes
                        .push(format!("Historical lookup unavailable: {error}")),
                }
            }
        }
        AppEvent::AddressIdentities { address, result } => {
            state.loading_address_identities.remove(&address);
            if let Ok(Some(associations)) = result {
                state
                    .store
                    .insert_identity_associations(address, associations);
            }
        }
        AppEvent::IdentityNames {
            requested,
            resolved,
        } => {
            for address in requested {
                state.loading_identity_names.remove(&address);
            }
            for (address, name) in resolved {
                state.store.insert_resolved_identity_name(address, name);
            }
        }
        AppEvent::CurrencyDefinitions { requested, records } => {
            for currency_id in requested {
                state.loading_currencies.remove(&currency_id);
            }
            for record in records {
                state.store.insert_currency_definition(record);
            }
        }
        AppEvent::CurrencyBrowserLoaded(loaded) => {
            for entry in &loaded.entries {
                state
                    .store
                    .insert_currency_definition(CurrencyDefinitionRecord {
                        currency_id: entry.id.clone(),
                        name: entry.name.clone(),
                        definition_json: entry.definition.clone(),
                    });
            }
            for screen in std::iter::once(&mut state.screen).chain(state.screen_stack.iter_mut()) {
                if let Screen::Currencies(view) = screen {
                    currency_browser::apply(view, &loaded);
                }
            }
        }
        AppEvent::CurrencyViewLoaded(loaded) => {
            for snapshot in loaded.snapshots.iter().take(32) {
                state.store.learn_currency_data(&snapshot.response);
            }
            if let (Some(id), Some(name), Some(definition)) =
                (&loaded.id, &loaded.name, &loaded.definition)
            {
                state
                    .store
                    .insert_currency_definition(CurrencyDefinitionRecord {
                        currency_id: id.clone(),
                        name: name.clone(),
                        definition_json: definition.clone(),
                    });
            }
            for screen in std::iter::once(&mut state.screen).chain(state.screen_stack.iter_mut()) {
                if let Screen::Currency(view) = screen {
                    currency_ui::apply(view, loaded.clone());
                }
            }
        }
        AppEvent::CurrencyHistoryLoaded {
            query,
            request_id,
            note,
            result,
        } => {
            for screen in std::iter::once(&mut state.screen).chain(state.screen_stack.iter_mut()) {
                if let Screen::Currency(view) = screen
                    && view.query == query
                    && view.history_request == Some(request_id)
                {
                    view.history_request = None;
                    view.loading = false;
                    match &result {
                        Ok(snapshot) => {
                            currency_ui::clear_activity(view);
                            view.snapshots.push(snapshot.clone());
                            view.historical = Some(snapshot.clone());
                            view.error = None;
                            view.history_note = note.clone();
                            currency_ui::rebuild_document(view);
                            currency_ui::focus_section(view, "Historical state");
                        }
                        Err(error) => {
                            view.error = Some(format!("Historical lookup: {error}"));
                            currency_ui::focus_section(view, "Source: Historical lookup:");
                        }
                    }
                }
            }
        }
        AppEvent::CurrencyActivityLoaded {
            query,
            request_id,
            result,
        } => {
            for screen in std::iter::once(&mut state.screen).chain(state.screen_stack.iter_mut()) {
                if let Screen::Currency(view) = screen
                    && view.query == query
                    && view.activity_request == Some(request_id)
                {
                    view.activity_request = None;
                    match &result {
                        Ok(page) => {
                            view.activity = Some(crate::currency_activity::StoredPage {
                                page: page.clone(),
                                membership: "anchor checked during query".into(),
                            });
                            view.activity_retained = false;
                            view.retained_activity.retain(|stored| {
                                stored.page.range != page.range
                                    || stored.page.anchor != page.anchor
                                    || stored.page.source != page.source
                            });
                            view.retained_activity.insert(
                                0,
                                crate::currency_activity::StoredPage {
                                    page: page.clone(),
                                    membership: "last checked anchor".into(),
                                },
                            );
                            view.retained_activity.truncate(8);
                            view.transfer_request = None;
                            view.transfer_inputs.clear();
                            view.transfer_export = None;
                            view.error = None;
                            currency_ui::rebuild_document(view);
                            currency_ui::focus_section(view, "Currency activity heights");
                        }
                        Err(error) => {
                            view.error = Some(format!("Activity: {error}"));
                            currency_ui::focus_section(view, "Source: Activity:");
                        }
                    }
                }
            }
        }
        AppEvent::CurrencyTransfersLoaded {
            query,
            request_id,
            result,
        } => {
            for screen in std::iter::once(&mut state.screen).chain(state.screen_stack.iter_mut()) {
                if let Screen::Currency(view) = screen
                    && view.query == query
                    && view.transfer_request == Some(request_id)
                {
                    view.transfer_request = None;
                    match &result {
                        Ok((inputs, truncated)) => {
                            view.transfer_inputs = inputs.clone();
                            view.transfer_truncated = *truncated;
                            view.error = None;
                            currency_ui::rebuild_document(view);
                            currency_ui::focus_section(view, "Export inputs from");
                        }
                        Err(error) => {
                            view.error = Some(format!("Export inputs: {error}"));
                            currency_ui::focus_section(view, "Source: Export inputs:");
                        }
                    }
                }
            }
        }
        AppEvent::OutputStates { requested, states } => {
            for outpoint in requested {
                state.loading_outpoints.remove(&outpoint);
                state.store.output_status_attempted.insert(outpoint);
            }
            state.store.output_unspent.extend(states);
        }
        AppEvent::SearchResolved { request_id, result } => {
            if state.search.pending_request != Some(request_id) {
                return;
            }
            state.search.pending_request = None;
            state.search.task = None;
            match result {
                Ok(SearchResolution::Identity { query, version }) => {
                    state
                        .store
                        .stale_identities
                        .remove(&version.identity_address);
                    state.store.insert_identity_version(&query, version);
                    state.identity_load_attempted.remove(&query);
                    state.search.status = None;
                    set_navigation_root(state, identity_screen(query, None));
                }
                Ok(SearchResolution::Estimate(estimate)) => {
                    state.search.status = None;
                    push_screen(state, Screen::Estimate(Box::new(estimate)));
                }
                Ok(SearchResolution::TimeBlock { block, found }) => {
                    let hash = state.store.insert_block(block);
                    push_block_search_result(state, BlockTarget::Hash(hash));
                    state.search.status = Some(SearchStatus {
                        message: found.description(),
                        error: false,
                    });
                }
                Ok(SearchResolution::Block(block)) => {
                    let hash = state.store.insert_block(block);
                    state.search.status = None;
                    push_block_search_result(state, BlockTarget::Hash(hash));
                }
                Ok(SearchResolution::Transaction { txid, records }) => {
                    for record in records {
                        state.store.insert_transaction(record);
                    }
                    state.search.status = None;
                    push_transaction_search_result(state, txid);
                }
                Err(error) => {
                    state.search.status = Some(SearchStatus {
                        message: error,
                        error: true,
                    });
                }
            }
        }
        AppEvent::Peers(result) => {
            state.peers.loading = false;
            state.peers.last_refresh =
                (!std::mem::take(&mut state.peers.controls.refresh_after_action))
                    .then(Instant::now);
            state.peers.controls.banned = Some(result.banned);
            if let Some(totals) = result.totals {
                state.peers.controls.totals = Some(totals);
            }
            match result.peers {
                Ok(values) => update_peers(state, values),
                Err(error) => {
                    state.notice = Some(TransientNotice {
                        message: format!("Peer refresh failed: {error}"),
                        until: Instant::now() + Duration::from_secs(4),
                    });
                }
            }
        }
        AppEvent::PeerAction(result) => peer_controls::finish(state, result),
        AppEvent::GeoIpDownloaded(result) => {
            state.peers.geoip_downloading = false;
            match result {
                Ok(release) => match GeoIpDatabase::open(&state.peers.geoip_directory) {
                    Ok(database) => {
                        state.peers.geoip_database = Some(Arc::new(database));
                        state.peers.geoip_status = GeoIpStatus::Current(release);
                        state.peers.map_enabled = true;
                        enrich_peer_locations(&mut state.peers);
                        state.notice = Some(TransientNotice {
                            message: "GeoIP City and ASN databases installed".to_string(),
                            until: Instant::now() + Duration::from_secs(4),
                        });
                    }
                    Err(error) => {
                        state.peers.geoip_status = GeoIpStatus::Invalid(error.to_string());
                    }
                },
                Err(error) => {
                    state.notice = Some(TransientNotice {
                        message: format!("GeoIP download failed: {error}"),
                        until: Instant::now() + Duration::from_secs(5),
                    });
                }
            }
        }
        AppEvent::LogChunk(result) => {
            state.log_feed.loading = false;
            state.log_feed.last_refresh = Some(Instant::now());
            match result {
                Ok(chunk) => append_log_chunk(&mut state.log_feed, chunk),
                Err(error) => state.log_feed.error = Some(error),
            }
        }
        AppEvent::ClipboardCopied {
            label,
            bytes,
            result,
        } => {
            state.clipboard_busy = false;
            let message = match result
                .and_then(|prepared| prepared.finish().map_err(|error| error.to_string()))
            {
                Ok(route) => format!("Sent {label} ({bytes} bytes) via {route}; paste to verify"),
                Err(error) => format!("Copy failed: {error}"),
            };
            state.notice = Some(TransientNotice {
                message,
                until: Instant::now() + Duration::from_secs(8),
            });
        }
        AppEvent::CacheCleared(result) => {
            state.cache_clear_prompt = None;
            match result {
                Ok(size_bytes) => {
                    state.cache_size_bytes = Some(size_bytes);
                    state.notice = Some(TransientNotice {
                        message: format!(
                            "Cache cleared; database now uses {}",
                            format_data_size(size_bytes)
                        ),
                        until: Instant::now() + Duration::from_secs(5),
                    });
                }
                Err(error) => {
                    state.cache_size_bytes = None;
                    state.notice = Some(TransientNotice {
                        message: format!("Cache clear failed: {error}"),
                        until: Instant::now() + Duration::from_secs(6),
                    });
                }
            }
        }
        AppEvent::StoredRelationships { txid, result } => match result {
            Ok(links) => {
                state.store.reverse_relationships.insert(txid, links);
            }
            Err(error) => {
                state.notice = Some(TransientNotice {
                    message: format!("Stored relationships unavailable: {error}"),
                    until: Instant::now() + Duration::from_secs(8),
                })
            }
        },
    }
}

fn merge_dashboard_blocks(state: &mut AppState, blocks: Vec<BlockRecord>) {
    let selected_hash = state.dashboard.selected_block_hash().cloned();
    let mut hashes_by_height = state
        .dashboard
        .recent_blocks
        .iter()
        .filter_map(|hash| {
            state
                .store
                .block(hash)
                .map(|block| (block.summary.height, hash.clone()))
        })
        .collect::<BTreeMap<_, _>>();
    for block in blocks {
        let height = block.summary.height;
        let hash = state.store.insert_block(block);
        if state.store.chain.reconciled.is_none()
            || state.store.block_hash_at_height(height) == Some(&hash)
        {
            hashes_by_height.insert(height, hash);
        }
    }
    state.dashboard.recent_blocks = hashes_by_height
        .into_iter()
        .rev()
        .map(|(_, hash)| hash)
        .collect();
    state.dashboard.restore_block_selection(selected_hash);
    rebuild_dashboard_transactions(state);
}

fn rebuild_dashboard_transactions(state: &mut AppState) {
    let dashboard = &mut state.dashboard;
    let selected = dashboard
        .recent_transactions
        .get(dashboard.selected_transaction)
        .filter(|_| dashboard.selected_transaction != 0)
        .map(|entry| entry.txid.clone());
    let mut seen = HashSet::new();
    dashboard.recent_transactions = dashboard
        .recent_blocks
        .iter()
        .filter_map(|hash| state.store.block(hash))
        .filter(|block| {
            state.store.chain.block_badge(&block.summary.hash) != Some(StatusBadge::Orphaned)
        })
        .flat_map(|block| {
            block
                .summary
                .tx
                .iter()
                .enumerate()
                .rev()
                .map(|(position, summary)| (&block.summary.hash, position, &summary.txid))
        })
        .filter(|(_, _, txid)| seen.insert(*txid))
        .take(DASHBOARD_TRANSACTION_COUNT)
        .map(|(block, position, txid)| DashboardTransaction {
            block: block.clone(),
            position,
            txid: txid.clone(),
        })
        .collect();
    dashboard.selected_transaction = selected
        .and_then(|txid| {
            dashboard
                .recent_transactions
                .iter()
                .position(|entry| entry.txid == txid)
        })
        .unwrap_or(0);
}

fn update_peers(state: &mut AppState, values: Vec<Value>) {
    let selected_id = state
        .peers
        .records
        .get(state.peers.selected)
        .map(|peer| peer.id);
    state.peers.records = values
        .into_iter()
        .enumerate()
        .map(|(index, raw_json)| PeerRecord {
            id: raw_json
                .get("id")
                .and_then(Value::as_i64)
                .unwrap_or(index as i64),
            ip: raw_json
                .get("addr")
                .and_then(Value::as_str)
                .and_then(peer_ip),
            raw_json,
            geo: None,
        })
        .collect();
    enrich_peer_locations(&mut state.peers);
    state.peers.selected = selected_id
        .and_then(|id| state.peers.records.iter().position(|peer| peer.id == id))
        .unwrap_or(0)
        .min(state.peers.records.len().saturating_sub(1));
}

fn enrich_peer_locations(peers: &mut PeerState) {
    for peer in &mut peers.records {
        peer.geo = peer
            .ip
            .and_then(|ip| peers.geoip_database.as_ref()?.lookup(ip));
    }
}

fn peer_ip(address: &str) -> Option<IpAddr> {
    address
        .parse::<SocketAddr>()
        .map(|socket| socket.ip())
        .ok()
        .or_else(|| address.parse::<IpAddr>().ok())
        .or_else(|| {
            let (host, _) = address.rsplit_once(':')?;
            host.trim_matches(['[', ']']).parse().ok()
        })
}

fn append_log_chunk(feed: &mut LogFeedState, chunk: LogChunk) {
    if chunk.reset {
        feed.lines.clear();
        feed.partial.clear();
    }
    feed.offset = Some(chunk.offset);
    feed.error = None;
    let mut text = std::mem::take(&mut feed.partial);
    text.push_str(&chunk.text);
    let terminated = text.ends_with('\n');
    let mut lines = text.lines().map(str::to_string).collect::<Vec<_>>();
    if !terminated {
        feed.partial = lines.pop().unwrap_or_default();
    }
    feed.lines.extend(lines);
    while feed.lines.len() > LOG_LINE_LIMIT {
        feed.lines.pop_front();
    }
}

fn request_block_detail(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
    target: BlockTarget,
) {
    if state
        .store
        .block_for_target(&target)
        .is_some_and(|block| block.detail_loaded)
    {
        return;
    }

    let key = block_load_key(&target);
    if state.loading_blocks.insert(key) {
        let cached_block = state.store.block_for_target(&target).cloned();
        spawn_block_loader(config.clone(), tx.clone(), target, cached_block);
    }
}

fn transaction_refresh_target(state: &AppState) -> Option<(Txid, BlockHash)> {
    let Screen::Transaction { txid, .. } = &state.screen else {
        return None;
    };
    if !state.tip.healthy
        || state.loading_transactions.contains(txid)
        || state
            .transaction_retry_after
            .get(txid)
            .is_some_and(|retry| *retry > Instant::now())
    {
        return None;
    }
    let tip = state
        .store
        .chain
        .tip()
        .map(|block| &block.hash)
        .or_else(|| {
            if state.store.chain.height.is_some() {
                None
            } else {
                state
                    .tip
                    .height
                    .and_then(|height| state.store.block_hash_at_height(height))
            }
        })?;
    if state.transaction_verified_tip.get(txid) == Some(tip) {
        return None;
    }
    Some((txid.clone(), tip.clone()))
}

fn request_current_transaction_refresh(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
) {
    let Some((txid, tip)) = transaction_refresh_target(state) else {
        return;
    };
    state.loading_transactions.insert(txid.clone());
    let config = config.noninteractive();
    let tx = tx.clone();
    tokio::spawn(async move {
        let result = fetch_transaction(&config, &txid)
            .await
            .map_err(|error| error.to_string());
        let _ = tx.send(AppEvent::TransactionRefresh { txid, tip, result });
    });
}

fn request_transaction_context(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
    txid: &str,
) {
    let Some(transaction) = state.store.transaction(txid) else {
        return;
    };
    let source_txids = transaction
        .raw_json
        .get("vin")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|input| input.get("txid").and_then(Value::as_str))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let spending_confirmed = transaction.block_hash.is_some();
    let mut requested = source_txids.clone();
    if marketplace_funding_shape(&transaction.raw_json) {
        requested.extend(marketplace_related_spenders(
            &transaction.raw_json,
            state.store.marketplace_offer(txid),
        ));
    }
    if let Some(offer) = state.store.marketplace_publication(txid) {
        requested.push(offer.funding.clone());
    }
    for source_txid in source_txids {
        if let Some(source) = state.store.transaction(&source_txid)
            && marketplace_funding_shape(&source.raw_json)
        {
            requested.extend(marketplace_related_spenders(&source.raw_json, false));
        }
    }
    requested.sort();
    requested.dedup();
    requested.retain(|related_txid| {
        state
            .store
            .transaction(related_txid)
            .is_none_or(|source| spending_confirmed && source.block_hash.is_none())
            && state
                .transaction_retry_after
                .get(related_txid)
                .is_none_or(|retry| *retry <= Instant::now())
            && state.loading_transactions.insert(related_txid.clone())
    });
    if !requested.is_empty() {
        spawn_transaction_loader(config.clone(), tx.clone(), requested);
    }
}

fn marketplace_related_spenders(
    raw: &Value,
    include_spend: bool,
) -> impl Iterator<Item = String> + '_ {
    raw.get("vout")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .take(2)
        .skip(usize::from(!include_spend))
        .filter_map(|output| output.get("spentTxId").and_then(Value::as_str))
        .map(str::to_string)
}

fn request_address_detail(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
    address: &str,
) {
    if state.store.address(address).is_some() && !state.store.stale_addresses.contains(address) {
        return;
    }
    if state
        .address_retry_after
        .get(address)
        .is_some_and(|retry_at| *retry_at > Instant::now())
    {
        return;
    }
    if state.loading_addresses.insert(address.to_string()) {
        spawn_address_loader(config.clone(), tx.clone(), address.to_string());
    }
}

fn request_current_address_transactions(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
) {
    let address = match &state.screen {
        Screen::Address { address, .. } => Some(address.clone()),
        Screen::Identity { target, .. } => state
            .store
            .identity_address_for_query(&target.query)
            .map(str::to_string),
        _ => None,
    };
    let Some(address) = address else {
        return;
    };
    let Some(record) = state.store.address(&address).cloned() else {
        return;
    };
    let source_txids = record
        .transaction_ids
        .iter()
        .filter_map(|txid| state.store.transaction(txid))
        .flat_map(|transaction| source_transaction_ids(&transaction.raw_json))
        .collect::<Vec<_>>();
    let address_work_in_flight = record
        .transaction_ids
        .iter()
        .chain(record.first_txid.iter())
        .chain(record.latest_txid.iter())
        .chain(
            record
                .unspent_outpoints
                .iter()
                .map(|outpoint| &outpoint.txid),
        )
        .chain(source_txids.iter())
        .any(|txid| state.loading_transactions.contains(txid));
    if address_work_in_flight {
        return;
    }

    let recent = loadable_transaction_ids(state, record.transaction_ids.iter().cloned());
    if start_transaction_batch(state, config, tx, recent) {
        return;
    }

    let metadata = record
        .first_txid
        .iter()
        .chain(record.latest_txid.iter())
        .cloned()
        .chain(
            record
                .unspent_outpoints
                .iter()
                .map(|outpoint| outpoint.txid.clone()),
        );
    let metadata = loadable_transaction_ids(state, metadata);
    if start_transaction_batch(state, config, tx, metadata) {
        return;
    }

    let sources = loadable_transaction_ids(state, source_txids);
    let _ = start_transaction_batch(state, config, tx, sources);
}

fn request_current_address_utxos(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
) {
    let address = match &state.screen {
        Screen::Address { address, .. } => Some(address.clone()),
        Screen::Identity { target, .. } => state
            .store
            .identity_address_for_query(&target.query)
            .map(str::to_string),
        _ => None,
    };
    let Some(address) = address else {
        return;
    };
    if state
        .store
        .address(&address)
        .is_none_or(|record| record.utxos_complete)
        || state.loading_address_utxos.contains(&address)
        || state
            .address_utxo_retry_after
            .get(&address)
            .is_some_and(|retry_at| *retry_at > Instant::now())
    {
        return;
    }
    state.loading_address_utxos.insert(address.clone());
    spawn_address_utxo_loader(config.clone(), tx.clone(), address);
}

fn request_current_address_balance(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
) {
    let address = match &state.screen {
        Screen::Address { address, .. } => Some(address.clone()),
        Screen::Identity { target, .. } => state
            .store
            .identity_address_for_query(&target.query)
            .map(str::to_string),
        _ => None,
    };
    let Some(address) = address else {
        return;
    };
    if state
        .store
        .address(&address)
        .is_none_or(|record| record.balance_response.is_some())
        || state.loading_address_balances.contains(&address)
        || state
            .address_balance_retry_after
            .get(&address)
            .is_some_and(|retry_at| *retry_at > Instant::now())
    {
        return;
    }
    state.loading_address_balances.insert(address.clone());
    spawn_address_balance_loader(config.clone(), tx.clone(), address);
}

fn request_current_address_history(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
) {
    let address = match &state.screen {
        Screen::Address { address, .. } => Some(address.clone()),
        Screen::Identity { target, .. } => state
            .store
            .identity_address_for_query(&target.query)
            .map(str::to_string),
        _ => None,
    };
    let Some(address) = address else {
        return;
    };
    let Some(record) = state.store.address(&address) else {
        return;
    };
    if record.history_complete
        || record.transaction_ids.iter().any(|txid| {
            !state.store.contains_transaction(txid)
                && !state.transaction_retry_after.contains_key(txid)
        })
        || state.loading_address_histories.contains(&address)
        || state
            .address_history_retry_after
            .get(&address)
            .is_some_and(|retry_at| *retry_at > Instant::now())
    {
        return;
    }
    state.loading_address_histories.insert(address.clone());
    spawn_address_history_loader(config.clone(), tx.clone(), address);
}

fn loadable_transaction_ids(state: &AppState, txids: impl IntoIterator<Item = Txid>) -> Vec<Txid> {
    let now = Instant::now();
    let mut requested = Vec::new();
    for txid in txids {
        if state.store.contains_transaction(&txid)
            || state.loading_transactions.contains(&txid)
            || state
                .transaction_retry_after
                .get(&txid)
                .is_some_and(|retry_at| *retry_at > now)
            || requested.contains(&txid)
        {
            continue;
        }
        requested.push(txid);
        if requested.len() == TRANSACTION_LOAD_BATCH_SIZE {
            break;
        }
    }
    requested
}

fn start_transaction_batch(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
    requested: Vec<Txid>,
) -> bool {
    if requested.is_empty() {
        return false;
    }
    state.loading_transactions.extend(requested.iter().cloned());
    spawn_transaction_loader(config.clone(), tx.clone(), requested);
    true
}

fn request_current_identity_dependencies(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
) {
    let Screen::Identity { target, .. } = &state.screen else {
        return;
    };
    let query = target.query.clone();
    let canonical = state
        .store
        .identity_address_for_query(&query)
        .map(str::to_string);
    let history_complete = canonical
        .as_ref()
        .is_some_and(|address| state.store.identity_history_complete.contains(address));
    if !history_complete
        && !state.identity_load_attempted.contains(&query)
        && state.loading_identities.insert(query.clone())
    {
        state.identity_load_attempted.insert(query.clone());
        spawn_identity_loader(config.clone(), tx.clone(), query);
    }
    if let Some(address) = canonical {
        if history_complete {
            let mut versions = state
                .store
                .identity_heights
                .get(&address)
                .into_iter()
                .flatten()
                .rev()
                .filter_map(|height| {
                    state
                        .store
                        .identity_versions
                        .get(&(address.clone(), *height))
                });
            if let Some(current) = versions.next() {
                let height = current.block_height;
                let already_loaded = state
                    .store
                    .identity_evidence
                    .get(&address)
                    .is_some_and(|(loaded_height, _)| *loaded_height == height);
                if !already_loaded
                    && state
                        .store
                        .loading_identity_evidence
                        .insert(address.clone())
                {
                    spawn_identity_evidence_loader(
                        config.clone(),
                        tx.clone(),
                        address.clone(),
                        height,
                        std::iter::once(current).chain(versions).cloned().collect(),
                    );
                }
            }
        }
        request_address_detail(state, &config.noninteractive(), tx, &address);
    }
}

fn request_historical_identity_evidence(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
    to_height: u64,
) {
    let Screen::Identity { target, .. } = &state.screen else {
        return;
    };
    let Some(version) = state.store.identity_for_target(target) else {
        return;
    };
    let address = version.identity_address.clone();
    let Some((height, report)) = state.store.identity_evidence.get(&address).cloned() else {
        return;
    };
    if state.store.loading_identity_evidence.contains(&address)
        || !state
            .store
            .loading_historical_identity_evidence
            .insert(address.clone())
    {
        return;
    }
    let versions = state
        .store
        .identity_heights
        .get(&address)
        .into_iter()
        .flatten()
        .rev()
        .filter_map(|height| {
            state
                .store
                .identity_versions
                .get(&(address.clone(), *height))
                .cloned()
        })
        .collect::<Vec<_>>();
    spawn_historical_identity_evidence_loader(
        config.clone(),
        tx.clone(),
        address,
        height,
        versions,
        report,
        to_height,
    );
}

fn request_current_address_identities(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
) {
    let Screen::Address { address, .. } = &state.screen else {
        return;
    };
    if !address.starts_with('R')
        || state.store.identity_associations(address).is_some()
        || state.address_identities_attempted.contains(address)
    {
        return;
    }
    if state.store.address(address).is_some_and(|record| {
        record.transaction_ids.iter().any(|txid| {
            !state.store.contains_transaction(txid)
                && !state.transaction_retry_after.contains_key(txid)
        })
    }) {
        return;
    }
    let address = address.clone();
    if state.loading_address_identities.insert(address.clone()) {
        state.address_identities_attempted.insert(address.clone());
        spawn_address_identities_loader(config.clone(), tx.clone(), address);
    }
}

fn evidence_identity_references(report: &identity_evidence::IdentityEvidence) -> BTreeSet<String> {
    let mut references = BTreeSet::new();
    for link in &report.links {
        if let identity_evidence::EvidenceTarget::Identity(id) = &link.target
            && looks_like_identity_address(id)
        {
            references.insert(id.clone());
        }
        references.extend(
            link.detail
                .split(|c: char| !c.is_ascii_alphanumeric())
                .filter(|value| looks_like_identity_address(value))
                .map(str::to_string),
        );
    }
    references
}

fn request_current_evidence_identity_names(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
) {
    let Screen::Identity { target, .. } = &state.screen else {
        return;
    };
    let Some(identity) = state.store.identity_for_target(target) else {
        return;
    };
    let mut references = state
        .store
        .identity_evidence
        .get(&identity.identity_address)
        .map(|(_, report)| evidence_identity_references(report))
        .unwrap_or_default();
    if let Some(address) = identity_token_holder(&state.store, identity)
        && looks_like_identity_address(address)
    {
        references.insert(address.to_owned());
    }
    request_identity_names(state, config, tx, references);
}

fn evidence_detail_display(state: &AppState, detail: &str) -> String {
    if state.unmask_identity_addresses {
        return detail.to_string();
    }
    let mut display = detail.to_string();
    for id in detail
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|value| looks_like_identity_address(value))
    {
        display = display.replace(id, &identity_display_value(&state.store, id, false));
    }
    display
}

fn evidence_note_visible(note: &str) -> bool {
    let text = note.to_ascii_lowercase();
    if text.starts_with("reservation salt:") {
        return false;
    }
    ![
        "rpc error",
        "unavailable",
        "-idindex",
        "not retained",
        "storage",
    ]
    .iter()
    .any(|error| text.contains(error))
}

fn request_current_activity_identity_names(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
) {
    if config.daemon.control.state() == crate::sources::ConnectionState::Disabled
        && !config
            .daemon
            .api_pool
            .endpoints
            .iter()
            .any(|endpoint| endpoint.control.state() != crate::sources::ConnectionState::Disabled)
    {
        return;
    }
    if let Screen::Block { target, .. } = &state.screen {
        let Some(block) = state.store.block_for_target(target) else {
            return;
        };
        let mut references = BTreeSet::new();
        collect_identity_references(&block.raw_json, &mut references);
        request_identity_names(state, config, tx, references);
        return;
    }
    if let Screen::Transaction { txid, .. } = &state.screen {
        let Some(transaction) = state.store.transaction(txid) else {
            return;
        };
        let mut references = BTreeSet::new();
        extend_transaction_identity_references(&state.store, transaction, &mut references);
        request_identity_names(state, config, tx, references);
        return;
    }
    let activity_address = match &state.screen {
        Screen::Address { address, .. } => Some(address.clone()),
        Screen::Identity { target, .. } => state
            .store
            .identity_for_target(target)
            .map(|identity| identity.identity_address.clone()),
        _ => None,
    };
    let Some(activity_address) = activity_address else {
        return;
    };
    let Some(record) = state.store.address(&activity_address) else {
        return;
    };
    if record.transaction_ids.iter().any(|txid| {
        !state.store.contains_transaction(txid) && !state.transaction_retry_after.contains_key(txid)
    }) {
        return;
    }
    let mut references = BTreeSet::new();
    for transaction in record
        .transaction_ids
        .iter()
        .filter_map(|txid| state.store.transaction(txid))
    {
        extend_transaction_identity_references(&state.store, transaction, &mut references);
    }
    request_identity_names(state, config, tx, references);
}

fn extend_transaction_identity_references(
    store: &ObjectStore,
    transaction: &TransactionRecord,
    references: &mut BTreeSet<String>,
) {
    if let Some(prepared) = transaction.prepared.as_deref() {
        references.extend(prepared.identities.iter().cloned());
        for parent in prepared
            .parents
            .iter()
            .filter_map(|txid| store.transaction(txid))
        {
            if let Some(data) = parent.prepared.as_deref() {
                references.extend(data.identities.iter().cloned());
            }
        }
    }
}

/// Resolve configured system identities through the already permitted source.
/// Currency labels are useful fallbacks but do not replace identity names here.
fn request_chain_identity_names(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
    ids: Vec<String>,
) {
    if config.daemon.control.state() == crate::sources::ConnectionState::Disabled
        && !config
            .daemon
            .api_pool
            .endpoints
            .iter()
            .any(|endpoint| endpoint.control.state() != crate::sources::ConnectionState::Disabled)
    {
        return;
    }
    let mut seen = HashSet::new();
    let requested = ids
        .into_iter()
        .filter(|id| seen.insert(id.clone()))
        .filter(|id| state.store.identity_name(id).is_none())
        .filter(|id| {
            !state.identity_name_attempted.contains(id)
                && !state.loading_identity_names.contains(id)
        })
        .take(24usize.saturating_sub(state.loading_identity_names.len()))
        .collect::<Vec<_>>();
    if requested.is_empty() {
        return;
    }
    state
        .identity_name_attempted
        .extend(requested.iter().cloned());
    state
        .loading_identity_names
        .extend(requested.iter().cloned());
    spawn_identity_name_loader(config.clone(), tx.clone(), requested);
}

fn request_identity_names(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
    references: BTreeSet<String>,
) {
    let requested = references
        .into_iter()
        .filter(|address| state.store.identity_name(address).is_none())
        .filter(|address| state.store.currency_name(address).is_none())
        .filter(|address| !state.loading_currencies.contains(address))
        .filter(|address| !state.identity_name_attempted.contains(address))
        .filter(|address| !state.loading_identity_names.contains(address))
        .take(24usize.saturating_sub(state.loading_identity_names.len()))
        .collect::<Vec<_>>();
    if requested.is_empty() {
        return;
    }
    for address in &requested {
        state.identity_name_attempted.insert(address.clone());
        state.loading_identity_names.insert(address.clone());
    }
    spawn_identity_name_loader(config.clone(), tx.clone(), requested);
}

fn update_witness_view(sessions: &mut [ChainSession], active: usize) -> bool {
    let Screen::Witnesses(view) = &mut sessions[active].state.screen else {
        return false;
    };
    let changed = view.poll();
    let mut references = BTreeMap::<_, BTreeSet<_>>::new();
    for (system, identity) in view.signer_ids() {
        references
            .entry(system.clone())
            .or_default()
            .insert(identity.to_string());
    }
    let mut names = HashMap::new();
    for session in sessions.iter_mut() {
        let Some(system) = session.config.chain.system_id.get() else {
            continue;
        };
        let Some(ids) = references.remove(system) else {
            continue;
        };
        for id in &ids {
            let name = identity_display_value(&session.state.store, id, false);
            if name != *id {
                names.insert((system.clone(), id.clone()), name);
            }
        }
        let enabled = session.config.daemon.control.state()
            != crate::sources::ConnectionState::Disabled
            || session
                .config
                .daemon
                .api_pool
                .endpoints
                .iter()
                .any(|api| api.control.state() != crate::sources::ConnectionState::Disabled);
        if enabled {
            request_identity_names(&mut session.state, &session.config, &session.tx, ids);
        }
    }
    let Screen::Witnesses(view) = &mut sessions[active].state.screen else {
        return changed;
    };
    if view.names == names {
        changed
    } else {
        view.names = names;
        true
    }
}

fn insert_identity_load(store: &mut ObjectStore, result: IdentityLoadResult) {
    let canonical = result
        .versions
        .first()
        .map(|version| version.identity_address.clone());
    if let Some(address) = &canonical {
        store.stale_identities.remove(address);
        // Replace the live index; historical bodies remain available by version.
        store.identity_heights.remove(address);
    }
    for transaction in result.transactions {
        store.insert_transaction(transaction);
    }
    for version in result.versions {
        store.insert_identity_version(&result.query, version);
    }
    for (address, name) in result.resolved_names {
        store.insert_resolved_identity_name(address, name);
    }
    if result.history_complete
        && let Some(canonical) = canonical
    {
        store.identity_history_complete.insert(canonical);
    }
}

fn request_current_address_output_states(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
) {
    let Some((address, selected_tx)) = current_address_selection(state) else {
        return;
    };
    let Some(txid) = state
        .store
        .address(&address)
        .and_then(|record| record.transaction_ids.get(selected_tx))
        .cloned()
    else {
        return;
    };
    let Some(transaction) = state.store.transaction(&txid) else {
        return;
    };
    let mut outputs = transaction
        .raw_json
        .get("vout")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
        .map(|(index, output)| {
            let index = output
                .get("n")
                .and_then(Value::as_u64)
                .unwrap_or(index as u64);
            (
                addresses_in(output).iter().any(|item| item == &address),
                index,
            )
        })
        .collect::<Vec<_>>();
    outputs.sort_by_key(|(matches_address, index)| (!*matches_address, *index));
    let mut requested = outputs
        .into_iter()
        .take(ADDRESS_FLOW_LIMIT)
        .map(|(_, index)| OutPoint {
            txid: txid.clone(),
            index,
        })
        .filter(|outpoint| !state.store.output_unspent.contains_key(outpoint))
        .filter(|outpoint| !state.store.output_status_attempted.contains(outpoint))
        .collect::<Vec<_>>();
    requested.retain(|outpoint| state.loading_outpoints.insert(outpoint.clone()));
    if !requested.is_empty() {
        spawn_output_state_loader(config.clone(), tx.clone(), requested);
    }
}

fn current_address_selection(state: &AppState) -> Option<(String, usize)> {
    match &state.screen {
        Screen::Address {
            address,
            selected_tx,
            selected_identity,
        } if selected_identity.is_none() => Some((address.clone(), *selected_tx)),
        Screen::Identity {
            target,
            selected_tx,
            selected_primary_address,
            ..
        } if selected_primary_address.is_none() => state
            .store
            .identity_address_for_query(&target.query)
            .map(|address| (address.to_string(), *selected_tx)),
        _ => None,
    }
}

fn select_next_address_transaction(state: &mut AppState) {
    let (link_count, tx_count) = address_view_item_counts(state);
    match &mut state.screen {
        Screen::Address {
            selected_tx,
            selected_identity,
            ..
        } => advance_link_or_transaction(selected_identity, selected_tx, link_count, tx_count),
        Screen::Identity {
            selected_tx,
            selected_primary_address,
            ..
        } => {
            advance_link_or_transaction(selected_primary_address, selected_tx, link_count, tx_count)
        }
        _ => {}
    }
}

fn select_previous_address_transaction(state: &mut AppState) {
    let (link_count, _) = address_view_item_counts(state);
    match &mut state.screen {
        Screen::Address {
            selected_tx,
            selected_identity,
            ..
        } => retreat_link_or_transaction(selected_identity, selected_tx, link_count),
        Screen::Identity {
            selected_tx,
            selected_primary_address,
            ..
        } => retreat_link_or_transaction(selected_primary_address, selected_tx, link_count),
        _ => {}
    }
}

fn overview_summary_list_active(state: &AppState) -> bool {
    matches!(
        state.screen,
        Screen::Dashboard
            | Screen::Mempool
            | Screen::Marketplace
            | Screen::Address { .. }
            | Screen::Block {
                mode: ViewMode::Overview,
                ..
            }
            | Screen::Transaction {
                mode: ViewMode::Overview,
                ..
            }
            | Screen::Identity {
                mode: ViewMode::Overview,
                ..
            }
    )
}

fn summary_page_size(state: &AppState, content_height: u16) -> usize {
    let tile_height = match &state.screen {
        Screen::Dashboard | Screen::Mempool => MEMPOOL_TILE_HEIGHT,
        Screen::Marketplace => MARKETPLACE_TILE_HEIGHT,
        Screen::Bytes(_)
        | Screen::Notarizations(_)
        | Screen::Witnesses(_)
        | Screen::Currencies(_)
        | Screen::Currency(_)
        | Screen::Charts(_)
        | Screen::Estimate(_) => 1,
        Screen::Block { .. } => TRANSACTION_TILE_HEIGHT,
        Screen::Transaction { .. } => TRANSACTION_FLOW_TILE_HEIGHT,
        Screen::Address { .. } | Screen::Identity { .. } => ADDRESS_TRANSACTION_TILE_MIN_HEIGHT,
    };
    usize::from(content_height / tile_height).max(1)
}

fn move_summary_page(state: &mut AppState, amount: usize, direction: isize) {
    let screen_kind = match &state.screen {
        Screen::Dashboard => 0,
        Screen::Mempool => 1,
        Screen::Marketplace => 2,
        Screen::Bytes(_)
        | Screen::Notarizations(_)
        | Screen::Witnesses(_)
        | Screen::Currencies(_)
        | Screen::Currency(_)
        | Screen::Charts(_)
        | Screen::Estimate(_) => 6,
        Screen::Block { .. } => 3,
        Screen::Transaction { .. } => 4,
        Screen::Address { .. } | Screen::Identity { .. } => 5,
    };
    for _ in 0..amount {
        if direction > 0 {
            match screen_kind {
                0 => move_dashboard_selection(&mut state.dashboard, 1),
                1 => select_next_mempool_transaction(state),
                2 => select_next_marketplace_offer(state),
                3 => select_next_transaction(state),
                4 => select_next_transaction_flow(state),
                _ => select_next_address_transaction(state),
            }
        } else {
            match screen_kind {
                0 => move_dashboard_selection(&mut state.dashboard, -1),
                1 => select_previous_mempool_transaction(state),
                2 => select_previous_marketplace_offer(state),
                3 => select_previous_transaction(state),
                4 => select_previous_transaction_flow(state),
                _ => select_previous_address_transaction(state),
            }
        }
    }
}

fn select_summary_boundary(state: &mut AppState, end: bool) {
    let (link_count, tx_count) = address_view_item_counts(state);
    match &mut state.screen {
        Screen::Dashboard => {
            let (selected, count) = state.dashboard.selection();
            *selected = if end { count.saturating_sub(1) } else { 0 };
        }
        Screen::Mempool => {
            state.mempool.selected = if end {
                state.mempool.entries.len().saturating_sub(1)
            } else {
                0
            };
        }
        Screen::Marketplace => {
            state.marketplace.selected = if end {
                state.marketplace.offers.len().saturating_sub(1)
            } else {
                0
            };
            restore_marketplace_link(state);
        }
        Screen::Bytes(_)
        | Screen::Notarizations(_)
        | Screen::Witnesses(_)
        | Screen::Currencies(_)
        | Screen::Charts(_)
        | Screen::Estimate(_) => {}
        Screen::Currency(view) => {
            view.selected = if end {
                currency_ui::rows(view).len().saturating_sub(1)
            } else {
                0
            };
            view.scroll = 0;
        }
        Screen::Block {
            target,
            selected_tx,
            ..
        } => {
            *selected_tx = if end {
                state
                    .store
                    .block_for_target(target)
                    .map_or(0, |block| block.summary.tx.len().saturating_sub(1))
            } else {
                0
            };
        }
        Screen::Transaction {
            txid,
            selected_flow,
            ..
        } => {
            *selected_flow = if end {
                state.store.transaction(txid).map_or(0, |transaction| {
                    transaction_flow_selections(&transaction_flows(
                        &transaction.raw_json,
                        &state.store,
                    ))
                    .len()
                    .saturating_sub(1)
                })
            } else {
                0
            };
        }
        Screen::Address {
            selected_tx,
            selected_identity,
            ..
        } => set_activity_selection_boundary(
            selected_identity,
            selected_tx,
            link_count,
            tx_count,
            end,
        ),
        Screen::Identity {
            selected_tx,
            selected_primary_address,
            ..
        } => set_activity_selection_boundary(
            selected_primary_address,
            selected_tx,
            link_count,
            tx_count,
            end,
        ),
    }
}

fn set_activity_selection_boundary(
    selected_link: &mut Option<usize>,
    selected_tx: &mut usize,
    link_count: usize,
    tx_count: usize,
    end: bool,
) {
    if end && tx_count > 0 {
        *selected_link = None;
        *selected_tx = tx_count - 1;
    } else if link_count > 0 {
        *selected_link = Some(if end { link_count - 1 } else { 0 });
        *selected_tx = 0;
    } else {
        *selected_link = None;
        *selected_tx = if end { tx_count.saturating_sub(1) } else { 0 };
    }
}

fn address_view_item_counts(state: &AppState) -> (usize, usize) {
    match &state.screen {
        Screen::Address { address, .. } => (
            state
                .store
                .identity_associations(address)
                .map_or(0, |associations| associations.identities.len()),
            state
                .store
                .address(address)
                .map_or(0, |record| record.transaction_ids.len()),
        ),
        Screen::Identity { target, .. } => {
            let Some(version) = state.store.identity_for_target(target) else {
                return (0, 0);
            };
            (
                identity_header_item_count(version)
                    + state
                        .store
                        .identity_evidence
                        .get(&version.identity_address)
                        .map_or(0, |(_, report)| report.links.len()),
                state
                    .store
                    .address(&version.identity_address)
                    .map_or(0, |record| record.transaction_ids.len()),
            )
        }
        _ => (0, 0),
    }
}

fn advance_link_or_transaction(
    selected_link: &mut Option<usize>,
    selected_tx: &mut usize,
    link_count: usize,
    tx_count: usize,
) {
    if let Some(selected) = selected_link {
        if *selected + 1 < link_count {
            *selected += 1;
        } else if tx_count > 0 {
            *selected_link = None;
            *selected_tx = 0;
        }
    } else if tx_count > 0 {
        *selected_tx = (*selected_tx + 1).min(tx_count - 1);
    } else if link_count > 0 {
        *selected_link = Some(0);
    }
}

fn retreat_link_or_transaction(
    selected_link: &mut Option<usize>,
    selected_tx: &mut usize,
    link_count: usize,
) {
    if let Some(selected) = selected_link {
        *selected = selected.saturating_sub(1);
    } else if *selected_tx > 0 {
        *selected_tx -= 1;
    } else if link_count > 0 {
        *selected_link = Some(link_count - 1);
    }
}

fn open_selected_address_transaction(
    state: &mut AppState,
    config: &Config,
    event_tx: &UnboundedSender<AppEvent>,
) {
    match &state.screen {
        Screen::Address {
            address,
            selected_identity: Some(selected),
            ..
        } => {
            let Some(identity_address) = state
                .store
                .identity_associations(address)
                .and_then(|associations| associations.identities.get(*selected))
                .map(|identity| identity.identity_address.clone())
            else {
                return;
            };
            push_screen(state, identity_screen(identity_address, None));
            return;
        }
        Screen::Identity {
            target,
            selected_primary_address: Some(selected),
            ..
        } => {
            let Some(version) = state.store.identity_for_target(target) else {
                return;
            };
            let addresses = identity_primary_addresses(version);
            if let Some(address) = addresses.get(*selected).cloned() {
                push_screen(state, address_screen(address.clone()));
                request_address_detail(state, config, event_tx, &address);
                return;
            }
            if token_control::enabled(version) && *selected == addresses.len() {
                let identity = version.identity_address.clone();
                if let Some(address) =
                    identity_token_holder(&state.store, version).map(str::to_owned)
                {
                    if looks_like_identity_address(&address) {
                        push_screen(state, identity_screen(address, None));
                    } else {
                        push_screen(state, address_screen(address.clone()));
                        request_address_detail(state, config, event_tx, &address);
                    }
                } else {
                    token_control::request(state, config, event_tx, &identity, true);
                }
                return;
            }
            let Some(link) = selected
                .checked_sub(identity_header_item_count(version))
                .and_then(|index| {
                    state
                        .store
                        .identity_evidence
                        .get(&version.identity_address)
                        .and_then(|(_, report)| report.links.get(index))
                })
            else {
                return;
            };
            match link.target.clone() {
                identity_evidence::EvidenceTarget::Identity(address) => {
                    push_screen(state, identity_screen(address, None))
                }
                identity_evidence::EvidenceTarget::Address(address) => {
                    push_screen(state, address_screen(address.clone()));
                    request_address_detail(state, config, event_tx, &address);
                }
                identity_evidence::EvidenceTarget::Transaction(txid) => {
                    push_screen(state, transaction_screen(txid.clone()));
                    request_transaction_context(state, config, event_tx, &txid);
                }
            }
            return;
        }
        _ => {}
    }
    let Some((address, selected)) = current_address_selection(state) else {
        return;
    };
    let Some(txid) = state
        .store
        .address(&address)
        .and_then(|record| record.transaction_ids.get(selected))
        .cloned()
    else {
        return;
    };
    if !state.store.contains_transaction(&txid) {
        let requested = loadable_transaction_ids(state, [txid.clone()]);
        let _ = start_transaction_batch(state, config, event_tx, requested);
    }
    push_screen(state, transaction_screen(txid.clone()));
    request_transaction_context(state, config, event_tx, &txid);
}

fn block_load_key(target: &BlockTarget) -> String {
    match target {
        BlockTarget::Hash(hash) => format!("hash:{hash}"),
        BlockTarget::Height(height) => format!("height:{height}"),
    }
}

fn navigate_adjacent_block(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
    direction: i8,
) {
    let Screen::Block { target, mode, .. } = state.screen.clone() else {
        return;
    };
    let Some(block) = state.store.block_for_target(&target) else {
        return;
    };
    let branch_target = if let Some(chain) = &state.store.chain.reconciled {
        if direction < 0 {
            block
                .summary
                .previousblockhash
                .clone()
                .map(BlockTarget::Hash)
        } else {
            let children: Vec<_> = chain
                .canonical
                .values()
                .chain(chain.detached.values())
                .filter(|h| h.parent.as_deref() == Some(block.summary.hash.as_str()))
                .collect();
            let selected = children
                .iter()
                .find(|h| Some(&h.hash) == block.summary.nextblockhash.as_ref())
                .copied()
                .or_else(|| (children.len() == 1).then(|| children[0]));
            selected.map(|h| BlockTarget::Hash(h.hash.clone()))
        }
    } else {
        adjacent_block_target(block, state.tip.height, direction)
    };
    let Some(next_target) = branch_target else {
        if direction > 0
            && state
                .tip
                .height
                .is_some_and(|tip| block.summary.height >= tip)
        {
            state.notice = Some(TransientNotice {
                message: "Already at chain tip".to_string(),
                until: Instant::now() + Duration::from_millis(750),
            });
        }
        return;
    };
    state.screen = Screen::Block {
        target: next_target.clone(),
        selected_tx: 0,
        mode,
        json: JsonState::new(),
    };
    request_block_detail(state, config, tx, next_target);
}

fn adjacent_block_target(
    block: &BlockRecord,
    tip_height: Option<u64>,
    direction: i8,
) -> Option<BlockTarget> {
    match direction {
        -1 => block
            .summary
            .previousblockhash
            .clone()
            .map(BlockTarget::Hash)
            .or_else(|| block.summary.height.checked_sub(1).map(BlockTarget::Height)),
        1 if tip_height.is_none_or(|tip| block.summary.height < tip) => block
            .summary
            .nextblockhash
            .clone()
            .map(BlockTarget::Hash)
            .or_else(|| block.summary.height.checked_add(1).map(BlockTarget::Height)),
        _ => None,
    }
}

fn navigate_adjacent_identity_version(state: &mut AppState, direction: i8) {
    let Screen::Identity { target, .. } = state.screen.clone() else {
        return;
    };
    let Some(current) = state.store.identity_for_target(&target) else {
        return;
    };
    let Some(heights) = state.store.identity_heights.get(&current.identity_address) else {
        return;
    };
    let Some(next_height) = adjacent_identity_height(heights, current.block_height, direction)
    else {
        state.notice = Some(TransientNotice {
            message: if direction < 0 {
                "Already at initial registration".to_string()
            } else {
                "Already at latest identity version".to_string()
            },
            until: Instant::now() + Duration::from_millis(900),
        });
        return;
    };
    let Screen::Identity {
        target,
        selected_tx,
        selected_primary_address,
        json,
        ..
    } = &mut state.screen
    else {
        return;
    };
    target.height = Some(next_height);
    *selected_tx = 0;
    *selected_primary_address = None;
    *json = JsonState::new();
}

fn adjacent_identity_height(
    heights: &BTreeSet<u64>,
    current_height: u64,
    direction: i8,
) -> Option<u64> {
    let heights = heights.iter().copied().collect::<Vec<_>>();
    let position = heights
        .iter()
        .position(|height| *height == current_height)?;
    match direction {
        -1 => position
            .checked_sub(1)
            .and_then(|position| heights.get(position).copied()),
        1 => heights.get(position + 1).copied(),
        _ => None,
    }
}

fn push_screen(state: &mut AppState, screen: Screen) {
    let previous = state.screen.clone();
    state.screen_stack.push(previous);
    state.screen = screen;
}

fn set_navigation_root(state: &mut AppState, screen: Screen) {
    if matches!(screen, Screen::Dashboard) {
        state.dashboard.block_cursor = 0;
        state.dashboard.transactions_focused = false;
    }
    replace_navigation_root(&mut state.screen_stack, &mut state.screen, screen);
}

fn replace_navigation_root(stack: &mut Vec<Screen>, current: &mut Screen, screen: Screen) {
    stack.clear();
    *current = screen;
}

fn pop_screen(state: &mut AppState) {
    state.screen = state.screen_stack.pop().unwrap_or(Screen::Dashboard);
}

enum LinkedView<'a> {
    Currency(&'a str),
    Identity(&'a str),
}

fn linked_identity_currency(state: &AppState) -> Option<LinkedView<'_>> {
    match &state.screen {
        Screen::Identity { target, .. } => {
            let version = state.store.identity_for_target(target)?;
            let id = version.identity_address.as_str();
            (identity_has_currency(version) || state.store.currencies_by_id.contains_key(id))
                .then_some(LinkedView::Currency(id))
        }
        Screen::Currency(view) => view
            .id
            .as_deref()
            .filter(|id| looks_like_identity_address(id))
            .map(LinkedView::Identity),
        _ => None,
    }
}

fn toggle_identity_currency(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
    can_pop: bool,
) -> bool {
    match linked_identity_currency(state) {
        Some(LinkedView::Currency(id)) => {
            if can_pop
                && matches!(
                    state.screen_stack.last(),
                    Some(Screen::Currency(view)) if view.id.as_deref() == Some(id)
                )
            {
                pop_screen(state);
            } else {
                let id = id.to_owned();
                push_screen(
                    state,
                    currency_ui::screen(id.clone(), config.chain_cache_key()),
                );
                currency_ui::spawn_load(config.clone(), tx.clone(), id, false);
            }
        }
        Some(LinkedView::Identity(id)) => {
            if can_pop
                && matches!(
                    state.screen_stack.last(),
                    Some(Screen::Identity { target, .. })
                        if target.query == id
                            || state.store.identity_address_for_query(&target.query) == Some(id)
                )
            {
                pop_screen(state);
            } else {
                push_screen(state, identity_screen(id.to_owned(), None));
            }
        }
        None => return false,
    }
    state.search.status = None;
    true
}

fn toggle_view_mode(state: &mut AppState) {
    match &mut state.screen {
        Screen::Block { mode, .. }
        | Screen::Transaction { mode, .. }
        | Screen::Identity { mode, .. } => {
            *mode = match mode {
                ViewMode::Overview => ViewMode::Json,
                ViewMode::Json => ViewMode::Overview,
            };
        }
        Screen::Witnesses(view) => {
            view.mode = match view.mode {
                ViewMode::Overview => ViewMode::Json,
                ViewMode::Json => ViewMode::Overview,
            };
        }
        Screen::Currency(view) => {
            view.mode = match view.mode {
                ViewMode::Overview => ViewMode::Json,
                ViewMode::Json => ViewMode::Overview,
            };
        }
        Screen::Dashboard
        | Screen::Mempool
        | Screen::Marketplace
        | Screen::Currencies(_)
        | Screen::Notarizations(_)
        | Screen::Bytes(_)
        | Screen::Address { .. }
        | Screen::Charts(_)
        | Screen::Estimate(_) => {}
    }
}

fn move_dashboard_selection(dashboard: &mut DashboardState, delta: isize) {
    let (selected, count) = dashboard.selection();
    *selected = selected
        .saturating_add_signed(delta)
        .min(count.saturating_sub(1));
}

fn select_next_mempool_transaction(state: &mut AppState) {
    if state.mempool.entries.is_empty() {
        return;
    }
    state.mempool.selected = (state.mempool.selected + 1).min(state.mempool.entries.len() - 1);
}

fn select_previous_mempool_transaction(state: &mut AppState) {
    state.mempool.selected = state.mempool.selected.saturating_sub(1);
}

fn initialize_identity_json(state: &mut AppState) {
    if let Screen::Identity { target, json, .. } = &mut state.screen
        && let Some(path) = json.initial_focus.clone()
        && let Some(record) = state.store.identity_for_target(target)
    {
        json.expanded.insert(path.clone());
        if let Some(index) = json
            .visible_entries(&record.raw_json)
            .iter()
            .position(|entry| entry.path == path)
        {
            json.cursor = index;
        }
        json.initial_focus = None;
    }
}

fn restore_marketplace_link(state: &mut AppState) {
    if let Some(offer) = state.marketplace.offers.get(state.marketplace.selected) {
        let links = marketplace_links(offer, &state.store);
        state.marketplace.selected_link = state
            .marketplace
            .preferred_link
            .as_ref()
            .and_then(|preferred| links.iter().position(|link| link == preferred))
            .unwrap_or(
                state
                    .marketplace
                    .selected_link
                    .min(links.len().saturating_sub(1)),
            );
    }
}

fn marketplace_sort_name(offer: &MarketplaceOffer, store: &ObjectStore) -> String {
    let id = offer.subject_id.as_deref();
    for asset in [&offer.offered, &offer.accepted] {
        if asset.get("identityid").and_then(Value::as_str) == id
            && let Some(name) = asset.get("name").and_then(Value::as_str)
        {
            return name.to_lowercase();
        }
    }
    id.map(|id| resolved_marketplace_name(id, store))
        .unwrap_or_else(|| offer.category.clone())
        .to_lowercase()
}

/// Decode both traditional uppercase input and explicit Shift modifiers, only
/// after text-entry prompts have consumed their keys.
fn handle_marketplace_cycle_key(
    state: &mut AppState,
    code: KeyCode,
    modifiers: KeyModifiers,
) -> bool {
    if !matches!(state.screen, Screen::Marketplace)
        || modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
    {
        return false;
    }
    let KeyCode::Char(letter) = code else {
        return false;
    };
    let reverse = letter.is_ascii_uppercase() || modifiers.contains(KeyModifiers::SHIFT);
    match letter.to_ascii_lowercase() {
        'a' => select_marketplace_link(state, if reverse { -1 } else { 1 }),
        's' => {
            state.marketplace.sort = if reverse {
                state.marketplace.sort.previous()
            } else {
                state.marketplace.sort.next()
            };
            sort_marketplace_from_top(state);
        }
        _ => return false,
    }
    true
}

fn sort_marketplace_from_top(state: &mut AppState) {
    update_marketplace_sort(state);
    state.marketplace.selected = 0;
    restore_marketplace_link(state);
}

fn update_marketplace_sort(state: &mut AppState) {
    if !matches!(state.screen, Screen::Marketplace) {
        return;
    }
    let selected = state
        .marketplace
        .offers
        .get(state.marketplace.selected)
        .map(|offer| offer.txid.clone());
    let sort = state.marketplace.sort;
    let descending = state.marketplace.descending;
    let store = &state.store;
    state.marketplace.offers.sort_by(|a, b| {
        let order = match sort {
            MarketplaceSort::Name => {
                let order = marketplace_sort_name(a, store).cmp(&marketplace_sort_name(b, store));
                if descending { order.reverse() } else { order }
            }
            MarketplaceSort::Price => match (&a.asking_price, &b.asking_price) {
                (Some(a), Some(b)) => a.currency.cmp(&b.currency).then_with(|| {
                    if descending {
                        b.amount.cmp(&a.amount)
                    } else {
                        a.amount.cmp(&b.amount)
                    }
                }),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                _ => std::cmp::Ordering::Equal,
            },
            MarketplaceSort::Age => {
                let time = |offer: &MarketplaceOffer| {
                    store
                        .transaction(&offer.txid)
                        .filter(|tx| tx.block_hash.is_some() && tx.block_time > 0)
                        .map(|tx| tx.block_time)
                };
                match (time(a), time(b)) {
                    (Some(a), Some(b)) => {
                        if descending {
                            a.cmp(&b)
                        } else {
                            b.cmp(&a)
                        }
                    }
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    _ => std::cmp::Ordering::Equal,
                }
            }
            MarketplaceSort::Expiry => {
                let order = a.expiry_height.cmp(&b.expiry_height);
                if descending { order.reverse() } else { order }
            }
        };
        order.then_with(|| a.txid.cmp(&b.txid))
    });
    if let Some(index) = selected.and_then(|id| {
        state
            .marketplace
            .offers
            .iter()
            .position(|offer| offer.txid == id)
    }) {
        state.marketplace.selected = index;
    }
    restore_marketplace_link(state);
}

fn request_marketplace_age_context(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
) {
    if !matches!(state.screen, Screen::Marketplace)
        || state.marketplace.sort != MarketplaceSort::Age
    {
        return;
    }
    let requested: Vec<_> = state
        .marketplace
        .offers
        .iter()
        .filter(|offer| {
            !state.store.contains_transaction(&offer.txid)
                && state
                    .transaction_retry_after
                    .get(&offer.txid)
                    .is_none_or(|retry| *retry <= Instant::now())
                && !state.loading_transactions.contains(&offer.txid)
        })
        .take(8usize.saturating_sub(state.loading_transactions.len()))
        .map(|offer| offer.txid.clone())
        .collect();
    if !requested.is_empty() {
        state.loading_transactions.extend(requested.iter().cloned());
        spawn_transaction_loader(config.clone(), tx.clone(), requested);
    }
}

fn select_next_marketplace_offer(state: &mut AppState) {
    if !state.marketplace.offers.is_empty() {
        state.marketplace.selected =
            (state.marketplace.selected + 1).min(state.marketplace.offers.len().saturating_sub(1));
        restore_marketplace_link(state);
    }
}

fn select_previous_marketplace_offer(state: &mut AppState) {
    state.marketplace.selected = state.marketplace.selected.saturating_sub(1);
    restore_marketplace_link(state);
}

fn select_marketplace_link(state: &mut AppState, direction: isize) {
    let count = state
        .marketplace
        .offers
        .get(state.marketplace.selected)
        .map(|offer| marketplace_links(offer, &state.store).len())
        .unwrap_or_default();
    if count > 0 {
        let selected = state.marketplace.selected_link.min(count - 1);
        state.marketplace.selected_link = if direction < 0 {
            (selected + count - 1) % count
        } else {
            (selected + 1) % count
        };
        state.marketplace.preferred_link = state
            .marketplace
            .offers
            .get(state.marketplace.selected)
            .and_then(|offer| {
                marketplace_links(offer, &state.store)
                    .get(state.marketplace.selected_link)
                    .cloned()
            });
    }
}

fn open_selected_marketplace_offer(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
) {
    let Some(offer) = state
        .marketplace
        .offers
        .get(state.marketplace.selected)
        .cloned()
    else {
        return;
    };
    let links = marketplace_links(&offer, &state.store);
    let Some(link) = links
        .get(
            state
                .marketplace
                .selected_link
                .min(links.len().saturating_sub(1)),
        )
        .cloned()
    else {
        return;
    };
    match link {
        MarketplaceLink::Funding(txid) => {
            push_screen(state, transaction_screen(txid.clone()));
            if state.store.contains_transaction(&txid) {
                request_transaction_context(state, config, tx, &txid);
            } else if state.loading_transactions.insert(txid.clone()) {
                spawn_transaction_loader(config.clone(), tx.clone(), vec![txid]);
            }
        }
        MarketplaceLink::Identity(identity) => {
            push_screen(state, identity_screen(identity, None));
        }
        MarketplaceLink::Address(address) => {
            let screen = if address.starts_with('i') {
                identity_screen(address, None)
            } else {
                address_screen(address)
            };
            push_screen(state, screen);
        }
        MarketplaceLink::Currency(currency_id) => {
            push_screen(
                state,
                currency_ui::screen(currency_id.clone(), config.chain_cache_key()),
            );
            currency_ui::spawn_load(config.clone(), tx.clone(), currency_id, false);
        }
    }
}

fn open_selected_mempool_transaction(
    state: &mut AppState,
    config: &Config,
    tx: &UnboundedSender<AppEvent>,
) {
    let Some(txid) = state
        .mempool
        .entries
        .get(state.mempool.selected)
        .map(|entry| entry.txid.clone())
    else {
        return;
    };
    push_screen(state, transaction_screen(txid.clone()));
    if state.store.contains_transaction(&txid) {
        request_transaction_context(state, config, tx, &txid);
    } else if state.loading_transactions.insert(txid.clone()) {
        spawn_transaction_loader(config.clone(), tx.clone(), vec![txid]);
    }
}

fn select_next_transaction(state: &mut AppState) {
    let Screen::Block {
        target,
        selected_tx,
        ..
    } = &mut state.screen
    else {
        return;
    };
    let Some(block) = state.store.block_for_target(target) else {
        return;
    };
    if block.summary.tx.is_empty() {
        return;
    }
    *selected_tx = (*selected_tx + 1).min(block.summary.tx.len() - 1);
}

fn select_previous_transaction(state: &mut AppState) {
    let Screen::Block { selected_tx, .. } = &mut state.screen else {
        return;
    };
    *selected_tx = selected_tx.saturating_sub(1);
}

fn open_selected_transaction(
    state: &mut AppState,
    config: &Config,
    event_tx: &UnboundedSender<AppEvent>,
) {
    let Screen::Block {
        target,
        selected_tx,
        mode,
        ..
    } = state.screen.clone()
    else {
        return;
    };
    let Some(txid) = state
        .store
        .block_for_target(&target)
        .and_then(|block| block.summary.tx.get(selected_tx))
        .map(|tx| tx.txid.clone())
    else {
        return;
    };
    push_screen(
        state,
        Screen::Transaction {
            txid: txid.clone(),
            selected_flow: 0,
            mode,
            json: JsonState::new(),
            notarization: None,
        },
    );
    request_transaction_context(state, config, event_tx, &txid);
}

fn select_next_transaction_flow(state: &mut AppState) {
    let Screen::Transaction {
        txid,
        selected_flow,
        ..
    } = &mut state.screen
    else {
        return;
    };
    let Some(transaction) = state.store.transaction(txid) else {
        return;
    };
    let flow_count =
        transaction_flow_selections(&transaction_flows(&transaction.raw_json, &state.store)).len();
    if flow_count > 0 {
        *selected_flow = (*selected_flow + 1).min(flow_count - 1);
    }
}

fn select_previous_transaction_flow(state: &mut AppState) {
    let Screen::Transaction { selected_flow, .. } = &mut state.screen else {
        return;
    };
    *selected_flow = selected_flow.saturating_sub(1);
}

fn open_selected_transaction_flow(
    state: &mut AppState,
    config: &Config,
    event_tx: &UnboundedSender<AppEvent>,
) -> FlowNavigation {
    let Screen::Transaction {
        txid,
        selected_flow,
        mode,
        ..
    } = state.screen.clone()
    else {
        return FlowNavigation::Handled;
    };
    let Some(transaction) = state.store.transaction(&txid) else {
        return FlowNavigation::Handled;
    };
    let flows = transaction_flows(&transaction.raw_json, &state.store);
    let selections = transaction_flow_selections(&flows);
    let Some((index, link)) = selections.get(selected_flow) else {
        return FlowNavigation::Handled;
    };
    let flow = &flows[*index];
    match link {
        FlowLink::Tag(index) => {
            return flow
                .tags
                .get(*index)
                .and_then(|tag| tag_target(flow, tag))
                .unwrap_or(FlowNavigation::Handled);
        }
        FlowLink::Protocol(index) => {
            return match flow
                .references
                .links
                .get(*index)
                .map(|link| link.target.clone())
            {
                Some(crate::protocol::Target::Currency(id)) => FlowNavigation::Currency(id),
                Some(crate::protocol::Target::Destination { address, system }) => {
                    FlowNavigation::Destination { system, address }
                }
                Some(crate::protocol::Target::Outpoint(target)) => FlowNavigation::Outpoint(target),
                Some(crate::protocol::Target::LegacyNotarizedBlock { hash, .. }) => {
                    let Ok(system) =
                        crate::world::system_id("VRSC", crate::world::Network::Mainnet)
                    else {
                        return FlowNavigation::Handled;
                    };
                    FlowNavigation::Block {
                        system: system.to_string(),
                        hash,
                    }
                }
                Some(crate::protocol::Target::OpReturn(data)) => {
                    let subject = if data.script_body {
                        "script bytes"
                    } else {
                        "payload"
                    };
                    push_screen(
                        state,
                        Screen::Bytes(byte_view::View::new(
                            format!("OP_RETURN output #{} • {subject}", flow.index),
                            data.bytes,
                        )),
                    );
                    FlowNavigation::Handled
                }
                None => FlowNavigation::Handled,
            };
        }
        FlowLink::Address(index) => {
            let address = flow.addresses[*index].clone();
            if matches!(
                classify_start_object(Some(&address)),
                ObjectRoute::Identity(_)
            ) {
                push_screen(state, identity_screen(address, None));
            } else {
                push_screen(state, address_screen(address.clone()));
                request_address_detail(state, config, event_tx, &address);
            }
        }
        FlowLink::SourceTransaction => {
            let source_txid = flow
                .source_txid
                .clone()
                .expect("source link has a transaction ID");
            push_screen(
                state,
                Screen::Transaction {
                    txid: source_txid.clone(),
                    selected_flow: 0,
                    mode,
                    json: JsonState::new(),
                    notarization: None,
                },
            );
            if !state.store.contains_transaction(&source_txid)
                && state.loading_transactions.insert(source_txid.clone())
            {
                spawn_transaction_loader(
                    config.clone(),
                    event_tx.clone(),
                    vec![source_txid.clone()],
                );
            }
            request_transaction_context(state, config, event_tx, &source_txid);
        }
        FlowLink::None => {}
    }
    FlowNavigation::Handled
}

fn open_transaction_block(
    state: &mut AppState,
    config: &Config,
    event_tx: &UnboundedSender<AppEvent>,
) {
    let Screen::Transaction { txid, mode, .. } = state.screen.clone() else {
        return;
    };
    let Some(block_hash) = state
        .store
        .transaction(&txid)
        .and_then(|transaction| transaction.block_hash.clone())
    else {
        return;
    };
    let target = BlockTarget::Hash(block_hash);
    push_screen(
        state,
        Screen::Block {
            target: target.clone(),
            selected_tx: 0,
            mode,
            json: JsonState::new(),
        },
    );
    request_block_detail(state, config, event_tx, target);
}

fn current_json_document(state: &AppState) -> Option<JsonDocument<'_>> {
    if let Some(AuxiliaryScreen::PeerDetail {
        peer_id,
        mode: ViewMode::Json,
        ..
    }) = &state.auxiliary
    {
        return peer_json_document(&state.peers, *peer_id);
    }
    match &state.screen {
        Screen::Witnesses(view) if view.mode == ViewMode::Json => Some(JsonDocument {
            title: view.navigation_message.as_ref().map_or_else(
                || "Witness investigation".into(),
                |message| format!("Witness investigation · {message}"),
            ),
            root: &view.document,
            default_filename: view.filename(),
        }),
        Screen::Currency(view) if view.mode == ViewMode::Json => Some(JsonDocument {
            title: format!("currency {}", view.name.as_deref().unwrap_or(&view.query)),
            root: &view.document,
            default_filename: format!(
                "currency-{}.json",
                view.id.as_deref().unwrap_or(&view.query)
            ),
        }),
        Screen::Block {
            mode: ViewMode::Json,
            target,
            ..
        } => block_json_document(&state.store, target),
        Screen::Transaction {
            mode: ViewMode::Json,
            txid,
            ..
        } => transaction_json_document(&state.store, txid),
        Screen::Identity {
            mode: ViewMode::Json,
            target,
            ..
        } => identity_json_document(&state.store, target),
        _ => None,
    }
}

fn peer_json_document(peers: &PeerState, peer_id: i64) -> Option<JsonDocument<'_>> {
    let peer = peers.records.iter().find(|peer| peer.id == peer_id)?;
    Some(JsonDocument {
        title: format!("peer {}", peer.id),
        root: &peer.raw_json,
        default_filename: format!("peer-{}.json", peer.id),
    })
}

fn block_json_document<'a>(
    store: &'a ObjectStore,
    target: &BlockTarget,
) -> Option<JsonDocument<'a>> {
    let block = store.block_for_target(target)?;
    Some(JsonDocument {
        title: format!("block {}", block.summary.height),
        root: &block.raw_json,
        default_filename: format!("block-{}.json", block.summary.height),
    })
}

fn transaction_json_document<'a>(store: &'a ObjectStore, txid: &str) -> Option<JsonDocument<'a>> {
    let transaction = store.transaction(txid)?;
    Some(JsonDocument {
        title: format!("transaction {}", transaction.summary.txid),
        root: &transaction.raw_json,
        default_filename: format!("tx-{}.json", transaction.summary.txid),
    })
}

fn identity_json_document<'a>(
    store: &'a ObjectStore,
    target: &IdentityTarget,
) -> Option<JsonDocument<'a>> {
    let identity = store.identity_for_target(target)?;
    let filename_name = identity
        .fully_qualified_name
        .trim_end_matches('@')
        .replace(['/', '\\'], "_");
    Some(JsonDocument {
        title: format!(
            "identity {} at height {}",
            identity.fully_qualified_name, identity.block_height
        ),
        root: &identity.raw_json,
        default_filename: format!("id-{filename_name}-{}.json", identity.block_height),
    })
}

fn current_json_state(state: &AppState) -> Option<&JsonState> {
    if let Some(AuxiliaryScreen::PeerDetail {
        mode: ViewMode::Json,
        json,
        ..
    }) = &state.auxiliary
    {
        return Some(json);
    }
    match &state.screen {
        Screen::Witnesses(view) if view.mode == ViewMode::Json => Some(&view.json),
        Screen::Currency(view) if view.mode == ViewMode::Json => Some(&view.json),
        Screen::Block {
            mode: ViewMode::Json,
            json,
            ..
        }
        | Screen::Transaction {
            mode: ViewMode::Json,
            json,
            ..
        }
        | Screen::Identity {
            mode: ViewMode::Json,
            json,
            ..
        } => Some(json),
        _ => None,
    }
}

fn current_json_state_mut(state: &mut AppState) -> Option<&mut JsonState> {
    if let Some(AuxiliaryScreen::PeerDetail {
        mode: ViewMode::Json,
        json,
        ..
    }) = &mut state.auxiliary
    {
        return Some(json);
    }
    match &mut state.screen {
        Screen::Witnesses(view) if view.mode == ViewMode::Json => Some(&mut view.json),
        Screen::Currency(view) if view.mode == ViewMode::Json => Some(&mut view.json),
        Screen::Block {
            mode: ViewMode::Json,
            json,
            ..
        }
        | Screen::Transaction {
            mode: ViewMode::Json,
            json,
            ..
        }
        | Screen::Identity {
            mode: ViewMode::Json,
            json,
            ..
        } => Some(json),
        _ => None,
    }
}

fn move_json_cursor(state: &mut AppState, delta: isize) {
    if current_json_entry(state)
        .is_some_and(|entry| entry.collapsible && entry.expanded && entry.scalar_value.is_some())
    {
        let Some(json) = current_json_state_mut(state) else {
            return;
        };
        if delta < 0 {
            json.scalar_scroll = json.scalar_scroll.saturating_sub(delta.unsigned_abs());
        } else {
            json.scalar_scroll = json.scalar_scroll.saturating_add(delta as usize);
        }
        return;
    }

    let Some(visible_len) = current_json_entries(state).map(|entries| entries.len()) else {
        return;
    };
    let Some(json) = current_json_state_mut(state) else {
        return;
    };
    if visible_len == 0 {
        json.cursor = 0;
        return;
    }
    if delta < 0 {
        json.cursor = json.cursor.saturating_sub(delta.unsigned_abs());
    } else {
        json.cursor = (json.cursor + delta as usize).min(visible_len - 1);
    }
}

fn move_json_cursor_to_boundary(state: &mut AppState, end: bool) {
    let visible_len = current_json_entries(state).map_or(0, |entries| entries.len());
    let Some(json) = current_json_state_mut(state) else {
        return;
    };
    json.scalar_scroll = 0;
    json.cursor = if end {
        visible_len.saturating_sub(1)
    } else {
        0
    };
}

fn toggle_json_node(state: &mut AppState) {
    let Some(entry) = current_json_entry(state) else {
        return;
    };
    if !entry.collapsible {
        return;
    }
    let Some(json) = current_json_state_mut(state) else {
        return;
    };
    if json.expanded.contains(&entry.path) {
        json.expanded.remove(&entry.path);
    } else {
        json.expanded.insert(entry.path);
    }
    json.scalar_scroll = 0;
}

fn activate_json_entry(
    state: &mut AppState,
    config: &Config,
    event_tx: &UnboundedSender<AppEvent>,
) {
    let Some(entry) = current_json_entry(state) else {
        return;
    };

    match entry.link {
        Some(JsonLink::Txid(txid)) => {
            if state.store.contains_transaction(&txid) {
                push_screen(
                    state,
                    Screen::Transaction {
                        txid: txid.clone(),
                        selected_flow: 0,
                        mode: ViewMode::Json,
                        json: JsonState::new(),
                        notarization: None,
                    },
                );
                request_transaction_context(state, config, event_tx, &txid);
            }
        }
        Some(JsonLink::BlockHash(hash)) => {
            if state.store.contains_block(&hash) {
                push_screen(
                    state,
                    Screen::Block {
                        target: BlockTarget::Hash(hash),
                        selected_tx: 0,
                        mode: ViewMode::Json,
                        json: JsonState::new(),
                    },
                );
            }
        }
        Some(JsonLink::Identity(identity)) => {
            push_screen(state, identity_screen(identity, None));
        }
        None => toggle_json_node(state),
    }
}

fn current_json_entries(state: &AppState) -> Option<Vec<JsonEntry>> {
    let document = current_json_document(state)?;
    let json = current_json_state(state)?;
    Some(json.visible_entries(document.root))
}

fn current_json_entry(state: &AppState) -> Option<JsonEntry> {
    let entries = current_json_entries(state)?;
    let json = current_json_state(state)?;
    entries.get(json.cursor).cloned()
}

fn yank_current_json_value(state: &mut AppState, tx: &UnboundedSender<AppEvent>) {
    let selected = current_json_document(state).and_then(|document| {
        current_json_entry(state).and_then(|entry| {
            document
                .root
                .pointer(&entry.pointer)
                .map(|value| (entry.label, json_yank_text(value)))
        })
    });
    if let Some((label, value)) = selected {
        request_clipboard_copy(state, tx, label, value);
    }
}

fn copy_clicked_value(
    state: &mut AppState,
    tx: &UnboundedSender<AppEvent>,
    target: crate::copy_values::Target,
) {
    match target {
        crate::copy_values::Target::Text { label, value } => {
            request_clipboard_copy(state, tx, label, value);
        }
        crate::copy_values::Target::Json { row, pointer } => {
            let Some(entry) = current_json_entries(state)
                .and_then(|entries| entries.into_iter().nth(row))
                .filter(|entry| entry.pointer == pointer)
            else {
                return;
            };
            let Some(value) = current_json_document(state)
                .and_then(|document| document.root.pointer(&pointer))
                .map(json_yank_text)
            else {
                return;
            };
            if let Some(json) = current_json_state_mut(state)
                && json.cursor != row
            {
                json.cursor = row;
                json.scalar_scroll = 0;
            }
            request_clipboard_copy(state, tx, entry.label, value);
        }
    }
}

fn request_clipboard_copy(
    state: &mut AppState,
    tx: &UnboundedSender<AppEvent>,
    label: String,
    value: String,
) {
    if state.clipboard_busy {
        return;
    }
    state.clipboard_busy = true;
    state.notice = Some(TransientNotice {
        message: format!("Sending {label} to the terminal clipboard..."),
        until: Instant::now() + Duration::from_secs(8),
    });
    let tx = tx.clone();
    let bytes = value.len();
    tokio::spawn(async move {
        let result = tokio::task::spawn_blocking(move || crate::clipboard::prepare(value))
            .await
            .map_err(|error| error.to_string())
            .and_then(|result| result.map_err(|error| error.to_string()));
        let _ = tx.send(AppEvent::ClipboardCopied {
            label,
            bytes,
            result,
        });
    });
}

fn json_yank_text(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        _ => serde_json::to_string(value).unwrap_or_else(|_| value.to_string()),
    }
}

fn start_save_prompt(state: &mut AppState) {
    let Some(document) = current_json_document(state) else {
        return;
    };
    let basename = document
        .default_filename
        .strip_suffix(".json")
        .unwrap_or(&document.default_filename)
        .to_string();
    state.save_prompt = Some(SavePrompt {
        basename,
        extension: ".json".to_string(),
        selection_active: true,
        status: None,
    });
}

fn handle_save_prompt_key(state: &mut AppState, key: KeyCode) {
    match key {
        KeyCode::Esc => {
            state.save_prompt = None;
        }
        KeyCode::Enter => save_current_json(state),
        KeyCode::Backspace => {
            if let Some(prompt) = state.save_prompt.as_mut() {
                if prompt.selection_active {
                    prompt.basename.clear();
                    prompt.selection_active = false;
                } else {
                    prompt.basename.pop();
                }
                prompt.status = None;
            }
        }
        KeyCode::Char(ch) => {
            if let Some(prompt) = state.save_prompt.as_mut() {
                if prompt.selection_active {
                    prompt.basename.clear();
                    prompt.selection_active = false;
                }
                prompt.basename.push(ch);
                prompt.status = None;
            }
        }
        _ => {}
    }
}

fn handle_cache_clear_prompt_key(
    state: &mut AppState,
    config: &Config,
    event_tx: &UnboundedSender<AppEvent>,
    key: KeyCode,
) {
    let Some(prompt) = state.cache_clear_prompt.as_mut() else {
        return;
    };
    if prompt.clearing {
        return;
    }
    match key {
        KeyCode::Char('y') | KeyCode::Char('Y') => {
            prompt.clearing = true;
            spawn_cache_clear(config.cache_path.clone(), event_tx.clone());
        }
        KeyCode::Esc | KeyCode::Char('n') | KeyCode::Char('N') => {
            state.cache_clear_prompt = None;
        }
        _ => {}
    }
}

fn save_current_json(state: &mut AppState) {
    let Some(prompt) = state.save_prompt.as_ref() else {
        return;
    };
    let Some(document) = current_json_document(state) else {
        state.save_prompt = None;
        return;
    };
    let filename = prompt.filename();
    let contents = match serde_json::to_string_pretty(document.root) {
        Ok(contents) => contents,
        Err(error) => {
            if let Some(prompt) = state.save_prompt.as_mut() {
                prompt.status = Some(format!("format error: {error}"));
            }
            return;
        }
    };

    match fs::write(&filename, contents) {
        Ok(()) => {
            state.save_prompt = None;
        }
        Err(error) => {
            if let Some(prompt) = state.save_prompt.as_mut() {
                prompt.status = Some(format!("save failed: {error}"));
            }
        }
    }
}

fn is_json_mode(state: &AppState) -> bool {
    if let Some(auxiliary) = &state.auxiliary {
        return matches!(
            auxiliary,
            AuxiliaryScreen::PeerDetail {
                mode: ViewMode::Json,
                ..
            }
        );
    }
    matches!(&state.screen, Screen::Currency(view) if view.mode == ViewMode::Json)
        || matches!(&state.screen, Screen::Witnesses(view) if view.mode == ViewMode::Json)
        || matches!(
            state.screen,
            Screen::Block {
                mode: ViewMode::Json,
                ..
            } | Screen::Transaction {
                mode: ViewMode::Json,
                ..
            } | Screen::Identity {
                mode: ViewMode::Json,
                ..
            }
        )
}

fn main_layout(state: &AppState, theme: &Theme, area: Rect, mouse_capture: bool) -> [Rect; 5] {
    let footer = footer_paragraph(state, theme, mouse_capture);
    screen_layout(area, &footer)
}

fn screen_layout(area: Rect, footer: &Paragraph<'_>) -> [Rect; 5] {
    let footer_height = footer
        .line_count(area.width.max(1))
        .min(usize::from(area.height.saturating_sub(8)));
    Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Min(5),
        Constraint::Length(footer_height as u16),
    ])
    .areas(area)
}

fn clamp_scroll_positions(
    state: &mut AppState,
    config: &Config,
    theme: &Theme,
    area: Rect,
    mouse_capture: bool,
) {
    let content_area = main_layout(state, theme, area, mouse_capture)[3];
    let auxiliary_max = match &state.auxiliary {
        Some(AuxiliaryScreen::Help { .. }) => {
            let body = help_border(theme).inner(content_area);
            wrapped_max_scroll(
                &help_lines(theme, state, &config.cache_path),
                body.width,
                body.height,
            )
        }
        Some(AuxiliaryScreen::PeerDetail {
            peer_id,
            mode: ViewMode::Overview,
            ..
        }) => state
            .peers
            .records
            .iter()
            .find(|peer| peer.id == *peer_id)
            .map(|peer| {
                let [overview, _] = peer_detail_areas(content_area);
                let inner = detail_header_content(Block::bordered().inner(overview));
                wrapped_max_scroll(
                    &peer_detail_lines(theme, peer, inner.width),
                    inner.width,
                    inner.height,
                )
            })
            .unwrap_or(0),
        _ => 0,
    };
    match state.auxiliary.as_mut() {
        Some(AuxiliaryScreen::Help { scroll })
        | Some(AuxiliaryScreen::PeerDetail {
            mode: ViewMode::Overview,
            scroll,
            ..
        }) => *scroll = (*scroll).min(auxiliary_max),
        _ => {}
    }

    let scalar_max = current_json_entry(state)
        .and_then(|entry| entry.expanded.then_some(entry.scalar_value).flatten())
        .map(|scalar| {
            let body_area = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(2),
                    Constraint::Min(3),
                    Constraint::Length(1),
                ])
                .split(content_area)[1];
            let serialized =
                serde_json::to_string(&scalar).unwrap_or_else(|_| format!("{scalar:?}"));
            wrapped_max_scroll(&[Line::from(serialized)], body_area.width, body_area.height)
        })
        .unwrap_or(0);
    if let Some(json) = current_json_state_mut(state) {
        json.scalar_scroll = json.scalar_scroll.min(scalar_max);
    }
}

fn render(
    frame: &mut Frame<'_>,
    config: &Config,
    theme: &Theme,
    state: &AppState,
    mouse_capture: bool,
) -> Option<Range<usize>> {
    let area = frame.area();
    let now = current_unix_timestamp();
    let layout = main_layout(state, theme, area, mouse_capture);

    let notice = state
        .notice
        .as_ref()
        .filter(|notice| notice.until > Instant::now());
    let header = if let Some(notice) = notice {
        Paragraph::new(Line::from(format!(
            " {} {} ",
            theme.glyph("⏭", "!"),
            notice.message
        )))
        .style(Style::default().fg(theme.background).bg(theme.warning))
        .alignment(Alignment::Center)
    } else {
        Paragraph::new(header_line(
            config,
            &chain_display_name(config, &state.store),
            theme,
            &state.tip,
            now,
            layout[0].width,
        ))
        .style(theme.header())
    };
    frame.render_widget(header, layout[0]);
    // Repaint the complete status row so stale terminal cells cannot masquerade as
    // heights or ages after width changes or terminal/multiplexer redraw glitches.
    for x in layout[0].x..layout[0].right() {
        if let Some(cell) = frame.buffer_mut().cell_mut((x, layout[0].y)) {
            cell.diff_option = ratatui::buffer::CellDiffOption::AlwaysUpdate;
        }
    }
    if state.auxiliary.is_some() {
        frame.render_widget(
            Paragraph::new(auxiliary_location_line(theme, state)).style(theme.base()),
            layout[1],
        );
        frame.render_widget(Paragraph::new("").style(theme.base()), layout[2]);
    } else {
        frame.render_widget(
            Paragraph::new({
                let mut line = breadcrumb_line(theme, state, layout[1].width);
                if !mouse_capture {
                    line.spans.push(Span::styled(
                        " | Mouse: select/paste (Shift-M)",
                        theme.warning(),
                    ));
                }
                line
            })
            .style(theme.base()),
            layout[1],
        );
        render_search_bar(frame, theme, state, layout[2]);
    }

    frame.render_widget(Clear, layout[3]);
    let mut dashboard_transactions = None;
    if let Some(auxiliary) = &state.auxiliary {
        render_auxiliary(frame, config, theme, state, auxiliary, layout[3]);
    } else {
        match &state.screen {
            Screen::Dashboard => {
                dashboard_transactions = Some(render_dashboard(
                    frame, config, theme, state, now, layout[3],
                ));
            }
            Screen::Mempool => render_mempool(frame, config, theme, state, now, layout[3]),
            Screen::Marketplace => render_marketplace(frame, theme, state, layout[3]),
            Screen::Charts(view) => chart_ui::render(frame, theme, view, layout[3]),
            Screen::Estimate(estimate) => height_ui::render(frame, theme, estimate, layout[3]),
            Screen::Notarizations(view) => notarization_ui::render(frame, theme, view, layout[3]),
            Screen::Witnesses(view) if view.mode == ViewMode::Json => {
                if let Some(document) = current_json_document(state) {
                    render_json_view(
                        frame,
                        theme,
                        &document,
                        &view.json,
                        &state.store,
                        true,
                        layout[3],
                    );
                }
            }
            Screen::Witnesses(view) => {
                view.render(frame, theme, state.unmask_identity_addresses, layout[3])
            }
            Screen::Bytes(view) => view.render(frame, theme, layout[3]),
            Screen::Currencies(view) => {
                currency_browser::render(frame, theme, view, &state.store, layout[3])
            }
            Screen::Currency(view) => {
                if view.mode == ViewMode::Json {
                    if let Some(document) = current_json_document(state) {
                        render_json_view(
                            frame,
                            theme,
                            &document,
                            &view.json,
                            &state.store,
                            state.unmask_identity_addresses,
                            layout[3],
                        );
                    }
                } else {
                    currency_ui::render(
                        frame,
                        theme,
                        view,
                        &state.store,
                        state.unmask_identity_addresses,
                        layout[3],
                    );
                }
            }
            Screen::Block {
                target,
                selected_tx,
                mode,
                json,
            } => {
                if *mode == ViewMode::Json {
                    if let Some(document) = block_json_document(&state.store, target) {
                        render_json_view(
                            frame,
                            theme,
                            &document,
                            json,
                            &state.store,
                            state.unmask_identity_addresses,
                            layout[3],
                        );
                    } else {
                        render_missing_object(frame, theme, "Block", layout[3]);
                    }
                } else {
                    render_block_view(frame, config, theme, state, target, *selected_tx, layout[3]);
                }
            }
            Screen::Transaction {
                txid,
                selected_flow,
                mode,
                json,
                ..
            } => {
                if *mode == ViewMode::Json {
                    if let Some(document) = transaction_json_document(&state.store, txid) {
                        render_json_view(
                            frame,
                            theme,
                            &document,
                            json,
                            &state.store,
                            state.unmask_identity_addresses,
                            layout[3],
                        );
                    } else {
                        render_missing_object(frame, theme, "Transaction", layout[3]);
                    }
                } else {
                    render_transaction_view(
                        frame,
                        config,
                        theme,
                        state,
                        txid,
                        *selected_flow,
                        layout[3],
                    );
                }
            }
            Screen::Address {
                address,
                selected_tx,
                selected_identity,
            } => {
                render_address_view(
                    frame,
                    config,
                    theme,
                    state,
                    address,
                    ActivitySelection {
                        transaction: *selected_tx,
                        link: *selected_identity,
                    },
                    layout[3],
                );
            }
            Screen::Identity {
                target,
                selected_tx,
                selected_primary_address,
                historical_height_input,
                mode,
                json,
            } => {
                if *mode == ViewMode::Json {
                    if let Some(document) = identity_json_document(&state.store, target) {
                        render_json_view(
                            frame,
                            theme,
                            &document,
                            json,
                            &state.store,
                            state.unmask_identity_addresses,
                            layout[3],
                        );
                    } else {
                        render_missing_object(frame, theme, "Identity", layout[3]);
                    }
                } else {
                    render_identity_view(
                        frame,
                        config,
                        theme,
                        state,
                        target,
                        ActivitySelection {
                            transaction: *selected_tx,
                            link: *selected_primary_address,
                        },
                        historical_height_input.as_deref(),
                        layout[3],
                    );
                }
            }
        }
    }

    frame.render_widget(footer_paragraph(state, theme, mouse_capture), layout[4]);

    if let Some(prompt) = &state.save_prompt {
        render_save_prompt(frame, theme, prompt, area);
    }
    if let Some(prompt) = &state.cache_clear_prompt {
        render_cache_clear_prompt(frame, theme, prompt, area);
    }
    if let Some(prompt) = &state.peers.controls.prompt {
        peer_controls::render_prompt(frame, theme, prompt, area);
    }
    dashboard_transactions
}

fn auxiliary_location_line(theme: &Theme, state: &AppState) -> Line<'static> {
    let label = match state.auxiliary {
        Some(AuxiliaryScreen::Help { .. }) => "help".to_string(),
        Some(AuxiliaryScreen::PeerBrowser) => "peers".to_string(),
        Some(AuxiliaryScreen::PeerDetail { peer_id, .. }) => format!("peer {peer_id}"),
        None => String::new(),
    };
    Line::from(vec![
        Span::styled("view ", theme.muted()),
        Span::styled(label, theme.focused()),
        Span::styled("  auxiliary, blockchain path unchanged", theme.muted()),
    ])
}

fn help_border(theme: &Theme) -> Block<'static> {
    Block::default()
        .borders(Borders::BOTTOM)
        .border_style(theme.muted())
}

fn render_help(
    frame: &mut Frame<'_>,
    theme: &Theme,
    state: &AppState,
    cache_path: &std::path::Path,
    scroll: usize,
    area: Rect,
) {
    let border = help_border(theme);
    let body = border.inner(area);
    frame.render_widget(border, area);
    scroll_view::paragraph(
        frame,
        theme,
        Paragraph::new(help_lines(theme, state, cache_path))
            .style(theme.base())
            .wrap(Wrap { trim: false }),
        body,
        scroll,
    );
}

fn workspace_layout(area: Rect, controls: &str) -> [Rect; 3] {
    let content = detail_header_content(area);
    let footer_height = Paragraph::new(controls)
        .wrap(Wrap { trim: true })
        .line_count(content.width.max(1))
        .min(usize::from(area.height.saturating_sub(2))) as u16;
    Layout::vertical([
        Constraint::Length(2),
        Constraint::Min(0),
        Constraint::Length(footer_height),
    ])
    .areas(content)
}

fn render_workspace(frame: &mut Frame<'_>, theme: &Theme, title: &str, controls: &str) -> Rect {
    let area = frame.area();
    let [header, body, footer] = workspace_layout(area, controls);
    frame.render_widget(Clear, area);
    frame.render_widget(Paragraph::new("").style(theme.base()), area);
    frame.render_widget(Paragraph::new(title).style(theme.focused()), header);
    frame.render_widget(
        Paragraph::new(controls)
            .style(theme.muted().bg(theme.panel))
            .wrap(Wrap { trim: true }),
        footer,
    );
    body
}

fn render_scrolling_panel(
    frame: &mut Frame<'_>,
    theme: &Theme,
    title: &str,
    lines: Vec<Line<'static>>,
    scroll: u16,
    controls: &str,
) {
    let inner = render_workspace(frame, theme, title, controls);
    scroll_view::paragraph(
        frame,
        theme,
        Paragraph::new(lines)
            .style(theme.base())
            .wrap(Wrap { trim: false }),
        inner,
        usize::from(scroll),
    );
}

fn render_relationships(
    frame: &mut Frame<'_>,
    theme: &Theme,
    links: &[crate::relationships::Link],
    statuses: &[&str],
    selected: usize,
    message: &str,
) {
    let body = render_workspace(
        frame,
        theme,
        "Relationships — F3",
        "j/k select | Enter/Right/l follow reference | Esc close | q quit",
    );
    let [notice, list] = Layout::vertical([
        Constraint::Length(u16::from(!message.is_empty())),
        Constraint::Min(0),
    ])
    .areas(body);
    frame.render_widget(Paragraph::new(message).style(theme.warning()), notice);
    if links.is_empty() {
        frame.render_widget(
            Paragraph::new("No explicit transaction relationships decoded").style(theme.muted()),
            list,
        );
        return;
    }
    let items = links
        .iter()
        .enumerate()
        .map(|(index, link)| {
            vec![
                Line::from(vec![
                    Span::styled(link.label.as_str(), Style::default().fg(theme.focus)),
                    Span::raw(format!("  ·  Output {}", link.carrier_vout)),
                ]),
                Line::styled(
                    format!(
                        "{}  {}:{}",
                        link.target.system_id, link.target.txid, link.target.vout
                    ),
                    Style::default().fg(theme.link),
                ),
                Line::styled(
                    statuses.get(index).copied().unwrap_or("Unresolved"),
                    Style::default().fg(theme.muted),
                ),
                Line::default(),
            ]
        })
        .collect();
    scroll_view::list(frame, theme, items, list, selected);
}

fn draw_shutdown(terminal: &mut Tui, theme: &Theme, message: &str) -> Result<()> {
    terminal.draw(|frame| render_shutdown(frame, theme, message))?;
    Ok(())
}

fn render_shutdown(frame: &mut Frame<'_>, theme: &Theme, message: &str) {
    let area = frame.area();
    frame.render_widget(Block::default().style(theme.base()), area);
    let status = Paragraph::new(vec![
        Line::styled("Shutting down", theme.focused()),
        Line::styled(message, theme.muted()),
    ])
    .style(theme.base())
    .alignment(Alignment::Center)
    .wrap(Wrap { trim: true });
    if area.width < 16 || area.height < 4 {
        frame.render_widget(Clear, area);
        frame.render_widget(status, area);
        return;
    }
    let width = area.width.saturating_sub(2).min(52);
    let height = (status.line_count(width - 2) + 2).min(usize::from(area.height)) as u16;
    let popup = Rect::new(
        area.x + (area.width - width) / 2,
        area.y + (area.height - height) / 2,
        width,
        height,
    );
    frame.render_widget(Clear, popup);
    frame.render_widget(
        status.block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(theme.muted()),
        ),
        popup,
    );
}

fn render_disabled_daemon(frame: &mut Frame<'_>, theme: &Theme, connection: &Config, footer: Rect) {
    let area = frame.area();
    frame
        .buffer_mut()
        .set_style(area, Style::default().add_modifier(Modifier::DIM));
    frame.render_widget(Clear, footer);
    let width = area.width.saturating_sub(2).min(76);
    let lines = vec![
        Line::styled(
            format!("{} daemon is disabled", connection.chain.name),
            Style::default().fg(theme.warning),
        ),
        Line::from(format!(
            "Connect to {}:{} to load the selected transaction.",
            connection.daemon.host, connection.daemon.port
        )),
        Line::default(),
        Line::from("1 connect once | 2 enable for session | Esc cancel | q quit"),
    ];
    let body = Paragraph::new(lines)
        .style(theme.base())
        .wrap(Wrap { trim: true });
    let height = (body.line_count(width.saturating_sub(2).max(1)) as u16 + 2).min(area.height);
    let popup = Rect::new(
        area.x + (area.width - width) / 2,
        area.y + (area.height - height) / 2,
        width,
        height,
    );
    frame.render_widget(Clear, popup);
    frame.render_widget(
        body.block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Disabled daemon ")
                .border_style(theme.warning()),
        ),
        popup,
    );
}

fn help_lines(theme: &Theme, state: &AppState, cache_path: &std::path::Path) -> Vec<Line<'static>> {
    let cache_size = state
        .cache_size_bytes
        .map(format_data_size)
        .unwrap_or_else(|| "unavailable".to_string());
    let sections: &[(&str, &[&str])] = &[
        (
            "Search",
            &[
                "/ focuses the search bar. Enter submits. Esc cancels input or a pending lookup.",
                "Blocks and transactions: enter a block height or a full block hash or transaction ID.",
                "Addresses and identities: enter a transparent address, a VerusID name such as VRSC@, or its i-address.",
                "Currencies: enter VRSC@@ or append @@ to a currency i-address to open its definition. Enter currencies to browse.",
                "Unix time: enter 1700000000 in whole seconds. Use unix:1700000000 to force a timestamp.",
                "Bare numbers are block heights through the current tip. Larger numbers at or after the chain's genesis time are Unix seconds. Numbers in between estimate future heights.",
                "Date/time: enter 2026-11-07, time:Nov 7 2026 2:30 pm UTC, or a relative time such as yesterday or 2 hours ago. time:, date:, and unix: are aliases.",
                "Date-only input means midnight UTC. A time without a zone also uses UTC. Past times open the nearest block found.",
                "Estimates: a future height estimates its date, and a future date estimates its height. Open the estimator directly with estimate:5000000, estimate:in 2 hours, or estimate:unix:1857254400.",
            ],
        ),
        (
            "Bookmarks",
            &[
                "F6 opens bookmarks. Enter opens the selected bookmark. F6 or Esc closes the list.",
                "Shift-B bookmarks the current block, transaction, address, identity, currency, or notarization browser.",
                "In Bookmarks, n adds a target manually, e edits the description, and x removes a bookmark.",
                "To add a notarization browser manually, enter notarizations:vETH as the target and use the observing chain's system ID.",
            ],
        ),
        (
            "Global",
            &[
                "q quit (outside input fields and API permission prompts) | d dashboard | m mempool | o marketplace | c currencies | F1/? help | ` peers | Esc close/back (root returns to dashboard)",
            ],
        ),
        (
            "Movement",
            &[
                "j/k or Up/Down select | Home/End first/last | PageUp/PageDown page | l/Right/Enter open | h/Left back",
                "On the dashboard, Tab or Shift-Tab switches between recent blocks and recent confirmed transactions.",
            ],
        ),
        (
            "Sources and links",
            &[
                "F1 or ? opens Help. F5 refreshes the current view wherever manual refresh is available.",
                "F2 sources: choose current chain, enable endpoints, monitor daemons, save states | F3 relationships | F4 witnesses | F7 inclusions/branches | F8 DeFi trail | F9 charts | F10 estimate",
            ],
        ),
        (
            "Views",
            &[
                "Tab / Shift-Tab switch Overview/JSON | Shift-I toggles identity resolution | Shift-D toggles heights/dates (est. for future heights)",
                "c switches between an identity and its currency when linked. In ordinary browsing, c opens Currencies. In a DeFi trail, c lists its currencies.",
            ],
        ),
        (
            "JSON",
            &["Space or Enter expand/collapse | y yank value | s save JSON"],
        ),
        (
            "Mouse selection and paste",
            &[
                "Shift-M switches between mouse capture (scroll and left-click to copy displayed values) and native terminal selection and middle-click paste. Hold Shift to bypass capture in supporting terminals. Paste inserts text into the active input. Enter submits.",
            ],
        ),
        (
            "Clipboard test",
            &[
                "y in Help sends 'Verus Explorer clipboard test'. Paste locally to verify. --clipboard-test prints transport diagnostics.",
            ],
        ),
        ("Blocks", &["p or [ previous block | n or ] next block"]),
        (
            "Peers",
            &[
                "` opens Peers. a adds a node to the daemon's node list. c clears bans after confirmation. p queues a ping to all peers.",
                "j/k select | Enter opens peer | p/n adjacent peer | w map | g GeoIP | Esc back",
                "In peer detail, x disconnects the selected peer and b bans its IP, each after confirmation.",
            ],
        ),
        (
            "Mempool",
            &["j/k select | Enter/Right/l opens transaction | refreshes every 2s"],
        ),
        (
            "Marketplace",
            &[
                "j/k select offer | a / Shift-A next/previous association | s / Shift-S next/previous sort field | r reverse order | Enter opens | F5 refreshes",
            ],
        ),
        (
            "Currencies",
            &[
                "c browses liquidity pools | f text filter | Shift-F filters | Tab switches flags/launch status (failed hidden by default) | Enter opens | F5/r refreshes | g height/date | w activity window | [/] adjacent windows",
            ],
        ),
        (
            "Transactions",
            &[
                "b opens containing block | e searches historical offer proofs | F7 inclusion history | F8 DeFi trail | j/k selects each source transaction and address link",
            ],
        ),
        (
            "Identities",
            &[
                "c opens the associated currency when Currency defined is shown | [ or p previous version | ] or n next version | v queries one historical reverse-index window",
            ],
        ),
    ];
    let mut lines = vec![
        Line::styled(format!("{APP_TITLE} help"), theme.focused()),
        Line::from(vec![
            Span::styled("Version ", theme.muted()),
            Span::raw(APP_VERSION),
        ]),
    ];
    for (heading, items) in sections {
        lines.push(Line::default());
        lines.push(Line::styled(*heading, theme.focused()));
        lines.extend(items.iter().copied().map(Line::from));
    }
    lines.extend([
        Line::default(),
        Line::styled("Local settings", theme.focused()),
        Line::from(vec![
            Span::styled("Cache usage ", theme.muted()),
            theme.value("cache usage", cache_size, theme.focused()),
            Span::raw(" | "),
            theme.value("cache path", cache_path.display().to_string(), theme.link()),
        ]),
        Line::from(format!(
            "Time display: {} — press t here to toggle UTC / Local",
            theme.time_display.get().label()
        )),
        Line::from("Press c in this help view to clear the local SQLite cache."),
    ]);
    lines
}

fn render_peer_browser(frame: &mut Frame<'_>, theme: &Theme, state: &AppState, area: Rect) {
    let stack = Layout::vertical([
        Constraint::Min(1),
        Constraint::Length(area.height.saturating_sub(3).min(5)),
    ]);
    let (list, map, totals) = if area.width < 70 {
        let [body, totals] = stack.areas(area);
        let [list, map] =
            Layout::vertical([Constraint::Percentage(68), Constraint::Percentage(32)]).areas(body);
        (list, map, totals)
    } else {
        let [left, map] =
            Layout::horizontal([Constraint::Length(52), Constraint::Min(30)]).areas(area);
        let [list, totals] = stack.areas(left);
        (list, map, totals)
    };
    render_peer_list(frame, theme, state, list);
    peer_controls::render_totals(frame, theme, &state.peers.controls, totals);
    if state.peers.map_enabled {
        render_peer_map(frame, theme, state, map);
    } else {
        render_map_disabled(frame, theme, state, map);
    }
}

fn render_peer_list(frame: &mut Frame<'_>, theme: &Theme, state: &AppState, area: Rect) {
    frame.render_widget(
        Paragraph::new("").style(Style::default().bg(theme.panel)),
        area,
    );
    let header_area = Rect::new(area.x, area.y, area.width, area.height.min(1));
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(" Peers ", theme.focused()),
            Span::styled(
                format!("{} connected", state.peers.records.len()),
                theme.muted(),
            ),
            Span::styled(
                match &state.peers.controls.banned {
                    Some(Ok(count)) => format!(" · {count} banned"),
                    Some(Err(_)) => " · bans unavailable".into(),
                    None => " · bans loading".into(),
                },
                theme.muted(),
            ),
            if state.peers.loading {
                Span::styled("  refreshing...", theme.warning())
            } else {
                Span::raw("")
            },
        ]))
        .style(Style::default().bg(theme.panel)),
        header_area,
    );

    let list_area = Rect::new(
        area.x,
        area.y.saturating_add(1),
        area.width,
        area.height.saturating_sub(1),
    );
    let visible = usize::from(list_area.height / PEER_TILE_HEIGHT).max(1);
    let start = state
        .peers
        .selected
        .saturating_sub(visible.saturating_sub(1));
    let content = scroll_view::content_area(list_area, state.peers.records.len() > visible);
    for (index, peer) in state
        .peers
        .records
        .iter()
        .enumerate()
        .skip(start)
        .take(visible)
    {
        let selected = index == state.peers.selected;
        let tile_index = index - start;
        let tile_area = Rect::new(
            list_area.x,
            list_area
                .y
                .saturating_add((tile_index as u16).saturating_mul(PEER_TILE_HEIGHT)),
            content.width,
            PEER_TILE_HEIGHT.min(
                list_area
                    .height
                    .saturating_sub((tile_index as u16).saturating_mul(PEER_TILE_HEIGHT)),
            ),
        );
        render_peer_summary_tile(
            frame,
            theme,
            peer,
            selected,
            current_unix_timestamp(),
            tile_area,
        );
    }
    scroll_view::render(
        frame,
        theme,
        list_area,
        state.peers.records.len(),
        start,
        visible,
    );
}

fn render_peer_summary_tile(
    frame: &mut Frame<'_>,
    theme: &Theme,
    peer: &PeerRecord,
    selected: bool,
    now: i64,
    area: Rect,
) {
    let raw = &peer.raw_json;
    let address = raw.get("addr").and_then(Value::as_str).unwrap_or("unknown");
    let banscore = json_i64(raw, "banscore")
        .map(|score| score.to_string())
        .unwrap_or_else(|| "--".to_string());
    let last_interaction = [json_i64(raw, "lastsend"), json_i64(raw, "lastrecv")]
        .into_iter()
        .flatten()
        .filter(|timestamp| *timestamp > 0)
        .max()
        .map(|timestamp| format!("{} ago", human_duration(now.saturating_sub(timestamp))))
        .unwrap_or_else(|| "unknown".to_string());
    let connected = json_i64(raw, "conntime")
        .filter(|timestamp| *timestamp > 0)
        .map(|timestamp| human_duration(now.saturating_sub(timestamp)))
        .unwrap_or_else(|| "unknown".to_string());
    let ping = raw
        .get("pingtime")
        .and_then(Value::as_f64)
        .filter(|ping| *ping > 0.0)
        .map(format_ping_time)
        .unwrap_or_else(|| "unknown".to_string());
    let content_width = tile_content_area(area).width;
    let location = peer_location_label(peer);
    let lines = vec![
        justified_line(
            vec![Span::styled(format!("Peer {}", peer.id), theme.focused())],
            vec![Span::styled(
                truncate_end(address, usize::from(content_width.saturating_sub(10))),
                theme.link(),
            )],
            content_width,
        ),
        justified_line(
            vec![Span::styled(
                truncate_end(&location, usize::from(content_width.saturating_sub(12))),
                theme.muted(),
            )],
            vec![Span::styled(peer_direction(raw), theme.warning())],
            content_width,
        ),
        justified_line(
            vec![
                Span::styled("last interaction ", theme.muted()),
                Span::raw(last_interaction),
            ],
            vec![
                Span::styled("banscore ", theme.muted()),
                Span::raw(banscore),
            ],
            content_width,
        ),
        justified_line(
            vec![
                Span::styled("connected ", theme.muted()),
                Span::raw(connected),
            ],
            vec![Span::styled("ping ", theme.muted()), Span::raw(ping)],
            content_width,
        ),
    ];
    render_summary_tile(
        frame,
        theme,
        SummaryTile {
            title: None,
            selected,
            lines,
            alignment: Alignment::Left,
        },
        area,
    );
}

fn peer_location_label(peer: &PeerRecord) -> String {
    if let Some(geo) = peer.geo.as_ref() {
        return match (&geo.city, &geo.country_code, &geo.country) {
            (Some(city), Some(country_code), _) => format!("{city}, {country_code}"),
            (Some(city), None, _) => city.clone(),
            (None, _, Some(country)) => country.clone(),
            _ => "located".to_string(),
        };
    }
    if peer.ip.is_some_and(|ip| !geoip::is_public_ip(ip)) {
        "private/local".to_string()
    } else {
        "unresolved".to_string()
    }
}

fn json_i64(value: &Value, key: &str) -> Option<i64> {
    value
        .get(key)
        .and_then(|field| field.as_i64().or_else(|| field.as_u64()?.try_into().ok()))
}

fn human_duration(seconds: i64) -> String {
    let mut remaining = seconds.max(0) as u64;
    let days = remaining / 86_400;
    remaining %= 86_400;
    let hours = remaining / 3_600;
    remaining %= 3_600;
    let minutes = remaining / 60;
    let seconds = remaining % 60;
    let mut parts = Vec::new();
    if days > 0 {
        parts.push(format!("{days}d"));
    }
    if hours > 0 || days > 0 {
        parts.push(format!("{hours}h"));
    }
    if minutes > 0 || hours > 0 || days > 0 {
        parts.push(format!("{minutes}m"));
    }
    parts.push(format!("{seconds}s"));
    parts.join(" ")
}

fn format_ping_time(seconds: f64) -> String {
    if seconds < 1.0 {
        format!("{:.0}ms", seconds.max(0.0) * 1_000.0)
    } else {
        format!("{seconds:.2}s")
    }
}

fn render_peer_map(frame: &mut Frame<'_>, theme: &Theme, state: &AppState, area: Rect) {
    let mut pins = Vec::new();
    let mut private_index = 0;
    let mut unresolved_index = 0;
    let unresolved_columns = (usize::from(area.width) / 5).max(1);
    let private_count = state
        .peers
        .records
        .iter()
        .filter(|peer| peer.geo.is_none() && peer.ip.is_some_and(|ip| !geoip::is_public_ip(ip)))
        .count();
    let latitude_step = (180.0 / f64::from(area.height.max(1)) * 1.2).max(7.0);
    for (index, peer) in state.peers.records.iter().enumerate() {
        let (point, kind) = if let Some(geo) = peer.geo.as_ref() {
            ((geo.longitude, geo.latitude), PeerMapPinKind::Located)
        } else if peer.ip.is_some_and(|ip| !geoip::is_public_ip(ip)) {
            let point = private_peer_point(private_index, private_count);
            private_index += 1;
            (point, PeerMapPinKind::Private)
        } else {
            let point = unresolved_peer_point(unresolved_index, unresolved_columns, latitude_step);
            unresolved_index += 1;
            (point, PeerMapPinKind::Unresolved)
        };
        pins.push(PeerMapPin {
            point,
            peer_id: peer.id,
            selected: index == state.peers.selected,
            kind,
        });
    }
    let canvas = Canvas::default()
        .background_color(theme.panel)
        .marker(Marker::Braille)
        .x_bounds([-180.0, 180.0])
        .y_bounds([-90.0, 90.0])
        .paint(|context| {
            context.draw(&Map {
                resolution: MapResolution::High,
                color: theme.muted,
            });
        });
    frame.render_widget(canvas, area);
    render_peer_map_pins(frame, theme, &pins, area);
    let status = geoip_status_text(&state.peers);
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(" map ", theme.focused()),
            Span::styled(status, theme.muted()),
            Span::styled(" | GeoIP by DB-IP.com (CC BY 4.0) ", theme.muted()),
        ]))
        .alignment(Alignment::Right),
        Rect::new(area.x, area.y, area.width, 1),
    );
}

fn peer_map_pin_style(theme: &Theme, pin: &PeerMapPin) -> Style {
    if pin.selected {
        return theme.selection_cursor();
    }
    match pin.kind {
        PeerMapPinKind::Located => theme.link(),
        PeerMapPinKind::Private => theme.warning(),
        PeerMapPinKind::Unresolved => theme.negative(),
    }
}

// Project once to terminal cells. Pins and labels are rendered directly into
// these cells, avoiding Canvas::print's second, truncating projection.
fn peer_map_pin_cell(pin: &PeerMapPin, width: u16, height: u16) -> (i32, i32) {
    (
        longitude_to_column(pin.point.0.clamp(-180.0, 180.0), width),
        latitude_to_row(pin.point.1.clamp(-90.0, 90.0), height),
    )
}

fn render_peer_map_pins(frame: &mut Frame<'_>, theme: &Theme, pins: &[PeerMapPin], area: Rect) {
    if area.width == 0 || area.height == 0 {
        return;
    }
    let labels = peer_map_label_cells(pins, area.width, area.height);
    for selected in [false, true] {
        for (pin, label) in pins
            .iter()
            .zip(&labels)
            .filter(|(pin, _)| pin.selected == selected)
        {
            let (column, row) = peer_map_pin_cell(pin, area.width, area.height);
            let glyph = match pin.kind {
                PeerMapPinKind::Located => "●",
                PeerMapPinKind::Private => "◆",
                PeerMapPinKind::Unresolved => "×",
            };
            let style = peer_map_pin_style(theme, pin);
            frame.render_widget(
                Line::from(Span::styled(glyph, style)),
                Rect::new(area.x + column as u16, area.y + row as u16, 1, 1),
            );
            if let Some((column, row)) = label {
                let text = format!("[{}]", pin.peer_id);
                frame.render_widget(
                    Line::from(Span::styled(text.clone(), style)),
                    Rect::new(
                        area.x + *column as u16,
                        area.y + *row as u16,
                        text.len() as u16,
                        1,
                    ),
                );
            }
        }
    }
}

fn peer_map_label_cells(pins: &[PeerMapPin], width: u16, height: u16) -> Vec<Option<(i32, i32)>> {
    // Reserve every pin before placing any labels, including later peers and
    // multiple peers that project to the same terminal cell.
    let mut occupied: HashSet<_> = pins
        .iter()
        .map(|pin| peer_map_pin_cell(pin, width, height))
        .collect();
    pins.iter()
        .map(|pin| {
            let (marker_column, marker_row) = peer_map_pin_cell(pin, width, height);
            let label_width = pin.peer_id.to_string().len() as i32 + 2;
            let candidates = [
                (marker_column + 1, marker_row),
                (marker_column + 1, marker_row - 1),
                (marker_column + 1, marker_row + 1),
                (marker_column - label_width, marker_row),
                (marker_column - label_width, marker_row - 1),
                (marker_column - label_width, marker_row + 1),
            ];
            let position = candidates.into_iter().find(|(column, row)| {
                label_fits(*column, *row, label_width, width, height, &occupied)
            });
            // If no adjacent slot is safe, omit this label rather than covering
            // a pin or another label. Its peer remains available in the list.
            if let Some((column, row)) = position {
                for offset in 0..label_width {
                    occupied.insert((column + offset, row));
                }
            }
            position
        })
        .collect()
}

fn label_fits(
    column: i32,
    row: i32,
    label_width: i32,
    width: u16,
    height: u16,
    occupied: &HashSet<(i32, i32)>,
) -> bool {
    column >= 0
        && row >= 1
        && column + label_width <= i32::from(width)
        && row < i32::from(height)
        && (0..label_width).all(|offset| !occupied.contains(&(column + offset, row)))
}

fn longitude_to_column(longitude: f64, width: u16) -> i32 {
    (((longitude + 180.0) / 360.0) * f64::from(width.saturating_sub(1))).round() as i32
}

fn latitude_to_row(latitude: f64, height: u16) -> i32 {
    (((90.0 - latitude) / 180.0) * f64::from(height.saturating_sub(1))).round() as i32
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PeerMapPinKind {
    Located,
    Private,
    Unresolved,
}

#[derive(Debug, Clone)]
struct PeerMapPin {
    point: (f64, f64),
    peer_id: i64,
    selected: bool,
    kind: PeerMapPinKind,
}

fn render_map_disabled(frame: &mut Frame<'_>, theme: &Theme, state: &AppState, area: Rect) {
    let status = geoip_status_text(&state.peers);
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled("World map disabled", theme.focused())),
            Line::from(""),
            Line::from(status),
            Line::from("Press g to download/update DB-IP City Lite and ASN Lite."),
            Line::from("Once installed, press m to toggle the map."),
            Line::from(""),
            Line::from(Span::styled(
                "GeoIP by DB-IP.com (CC BY 4.0)",
                theme.muted(),
            )),
        ])
        .style(Style::default().bg(theme.panel).fg(theme.text))
        .alignment(Alignment::Center)
        .wrap(Wrap { trim: true }),
        area,
    );
}

fn geoip_status_text(peers: &PeerState) -> String {
    if peers.geoip_downloading {
        return "downloading GeoIP data...".to_string();
    }
    match &peers.geoip_status {
        GeoIpStatus::Missing => "GeoIP data not installed".to_string(),
        GeoIpStatus::Current(release) => format!("GeoIP {release}"),
        GeoIpStatus::UpdateAvailable(release) => {
            format!("GeoIP {release}; update available (g)")
        }
        GeoIpStatus::Invalid(error) => format!("GeoIP unavailable: {error}"),
    }
}

fn unresolved_peer_point(index: usize, columns: usize, latitude_step: f64) -> (f64, f64) {
    let column = index % columns;
    let row = index / columns;
    let longitude_step = 336.0 / columns as f64;
    (
        -168.0 + (column as f64 + 0.5) * longitude_step,
        (-68.0 - row as f64 * latitude_step).max(-88.0),
    )
}

fn private_peer_point(index: usize, total: usize) -> (f64, f64) {
    const CENTER_LONGITUDE: f64 = -130.0;
    const CENTER_LATITUDE: f64 = -32.0;
    const LONGITUDE_RADIUS: f64 = 42.0;
    const LATITUDE_RADIUS: f64 = 28.0;
    const GOLDEN_ANGLE: f64 = 2.399_963_229_728_653;

    if total <= 1 {
        return (CENTER_LONGITUDE, CENTER_LATITUDE);
    }
    let radius = (index as f64 / (total - 1) as f64).sqrt();
    let angle = index as f64 * GOLDEN_ANGLE;
    (
        CENTER_LONGITUDE + LONGITUDE_RADIUS * radius * angle.cos(),
        CENTER_LATITUDE + LATITUDE_RADIUS * radius * angle.sin(),
    )
}

fn peer_detail_areas(area: Rect) -> [Rect; 2] {
    let log_height = (area.height / 3).min(9);
    Layout::vertical([Constraint::Min(0), Constraint::Length(log_height)]).areas(area)
}

fn render_peer_detail(
    frame: &mut Frame<'_>,
    theme: &Theme,
    state: &AppState,
    peer_id: i64,
    scroll: usize,
    area: Rect,
) {
    let Some(peer) = state.peers.records.iter().find(|peer| peer.id == peer_id) else {
        render_missing_object(frame, theme, "Peer", detail_header_content(area));
        return;
    };
    let [overview, log] = peer_detail_areas(area);
    let frame_style = |title| Block::bordered().title(title).border_style(theme.muted());
    let panel = frame_style(format!(" Peer {} ", peer.id));
    let inner = detail_header_content(panel.inner(overview));
    frame.render_widget(panel, overview);
    scroll_view::paragraph(
        frame,
        theme,
        Paragraph::new(peer_detail_lines(theme, peer, inner.width))
            .style(theme.base())
            .wrap(Wrap { trim: false }),
        inner,
        scroll,
    );
    let panel = frame_style(" Peer log ".to_string());
    let inner = detail_header_content(panel.inner(log));
    frame.render_widget(panel, log);
    render_peer_log(frame, theme, state, peer, inner);
}

fn peer_detail_lines(theme: &Theme, peer: &PeerRecord, width: u16) -> Vec<Line<'static>> {
    let address = peer
        .raw_json
        .get("addr")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let mut lines = vec![Line::from(vec![
        theme.value("peer address", address, theme.link()),
        Span::raw("  "),
        Span::styled(
            format!(" {} ", peer_direction(&peer.raw_json)),
            theme.status_badge(),
        ),
    ])];
    if let Some(geo) = &peer.geo {
        let place = [geo.city.as_deref(), geo.country.as_deref()]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join(", ");
        let place = match (place.is_empty(), geo.country_code.as_deref()) {
            (true, Some(code)) => code.to_string(),
            (false, Some(code)) => format!("{place} ({code})"),
            (false, None) => place,
            (true, None) => "unknown".to_string(),
        };
        lines.push(Line::from(vec![
            Span::styled("Location  ", theme.muted()),
            theme.value("peer location", place, Style::default()),
            Span::styled("  ASN  ", theme.muted()),
            theme.value(
                "peer ASN",
                geo.asn
                    .map(|asn| format!("AS{asn}"))
                    .unwrap_or_else(|| "unknown".to_string()),
                Style::default(),
            ),
            Span::styled("  ISP  ", theme.muted()),
            theme.value(
                "peer ISP",
                geo.organization
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
                Style::default(),
            ),
        ]));
    }
    if let Some(values) = peer.raw_json.as_object() {
        let mut sections = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
        for (key, value) in values {
            let (section, label) = match key.as_str() {
                "id" | "addr" | "inbound" => continue,
                "addrlocal" => (0, "Local address"),
                "conntime" => (0, "Connected since"),
                "version" => (0, "Protocol version"),
                "subver" => (0, "User agent"),
                "services" => (0, "Services"),
                "tls_established" => (0, "TLS established"),
                "tls_verified" => (0, "TLS verified"),
                "whitelisted" => (0, "Whitelisted"),
                "bytessent" => (1, "Bytes sent"),
                "bytesrecv" => (1, "Bytes received"),
                "lastsend" => (1, "Last send"),
                "lastrecv" => (1, "Last receive"),
                "pingtime" => (1, "Last ping"),
                "pingwait" => (1, "Pending ping"),
                "timeoffset" => (1, "Clock offset (seconds)"),
                "startingheight" => (2, "Starting height"),
                "synced_headers" => (2, "Synced headers"),
                "synced_blocks" => (2, "Synced blocks"),
                "inflight" => (2, "Blocks in flight"),
                _ => (3, key.as_str()),
            };
            let raw = compact_json_value(value);
            let display = match (key.as_str(), value.as_u64()) {
                ("conntime" | "lastsend" | "lastrecv", Some(time)) if time > 0 => {
                    theme.format_time(time)
                }
                ("bytessent" | "bytesrecv", Some(bytes)) => format_data_size(bytes),
                ("pingtime" | "pingwait", _) => value
                    .as_f64()
                    .map(format_ping_time)
                    .unwrap_or_else(|| raw.clone()),
                _ => raw.clone(),
            };
            let label_width = if width >= 52 { 24 } else { 0 };
            sections[section].push(Line::from(vec![
                Span::styled(
                    format!(" {:label_width$} ", format!("{label}:")),
                    theme.muted(),
                ),
                theme.value_as(label, display, raw, theme.base()),
            ]));
        }
        for (heading, details) in [
            "Connection",
            "Traffic and latency",
            "Chain progress",
            "Other RPC fields",
        ]
        .into_iter()
        .zip(sections)
        {
            if !details.is_empty() {
                lines.push(Line::default());
                lines.push(Line::styled(
                    format!(" {heading} "),
                    theme.focused().bg(theme.panel),
                ));
                lines.extend(details);
            }
        }
    }
    lines
}

fn wrapped_max_scroll(lines: &[Line<'_>], width: u16, height: u16) -> usize {
    let paragraph = Paragraph::new(lines.to_vec()).wrap(Wrap { trim: false });
    let content = scroll_view::paragraph_area(&paragraph, Rect::new(0, 0, width, height));
    paragraph
        .line_count(content.width.max(1))
        .saturating_sub(usize::from(height))
}

fn render_peer_log(
    frame: &mut Frame<'_>,
    theme: &Theme,
    state: &AppState,
    peer: &PeerRecord,
    area: Rect,
) {
    let address = peer
        .raw_json
        .get("addr")
        .and_then(Value::as_str)
        .unwrap_or("");
    let ip = peer.ip.map(|value| value.to_string()).unwrap_or_default();
    let mut lines = state
        .log_feed
        .lines
        .iter()
        .filter(|line| {
            (!ip.is_empty() && line.contains(&ip))
                || (!address.is_empty() && line.contains(address))
        })
        .rev()
        .take(usize::from(area.height))
        .cloned()
        .collect::<Vec<_>>();
    lines.reverse();
    let feed = &state.log_feed;
    let mut content = Vec::new();
    if let Some(error) = &feed.error {
        let message = match &feed.path {
            Some(path) => format!("Could not read {}: {error}", path.display()),
            None => error.clone(),
        };
        content.push(Line::styled(message, theme.warning()));
        content.push(Line::styled("This panel reads the local daemon’s debug.log and shows recent lines containing this peer’s IP or address.", theme.muted()));
        if !lines.is_empty() {
            content.push(Line::styled("Previously read entries", theme.focused()));
        }
    } else if lines.is_empty() {
        let message = if feed.offset.is_some() {
            "No recent debug.log lines contain this peer’s IP or address."
        } else {
            "Reading the local daemon’s debug.log for this peer’s IP or address…"
        };
        content.push(Line::styled(message, theme.muted()));
    }
    content.extend(lines.into_iter().map(Line::from));
    let paragraph = Paragraph::new(content)
        .style(theme.base())
        .wrap(Wrap { trim: false });
    let offset = if feed.error.is_none() {
        paragraph
            .line_count(area.width.max(1))
            .saturating_sub(usize::from(area.height))
    } else {
        0
    };
    frame.render_widget(
        paragraph.scroll((offset.min(u16::MAX as usize) as u16, 0)),
        area,
    );
}

fn peer_direction(raw: &Value) -> &'static str {
    if raw.get("inbound").and_then(Value::as_bool).unwrap_or(false) {
        "Inbound"
    } else {
        "Outbound"
    }
}

fn compact_json_value(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        _ => serde_json::to_string(value).unwrap_or_else(|_| "?".to_string()),
    }
}

fn truncate_end(value: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    if value.chars().count() <= width {
        return value.to_string();
    }
    value
        .chars()
        .take(width.saturating_sub(1))
        .collect::<String>()
        + "…"
}

fn render_auxiliary(
    frame: &mut Frame<'_>,
    config: &Config,
    theme: &Theme,
    state: &AppState,
    auxiliary: &AuxiliaryScreen,
    area: Rect,
) {
    match auxiliary {
        AuxiliaryScreen::Help { scroll } => {
            render_help(frame, theme, state, &config.cache_path, *scroll, area)
        }
        AuxiliaryScreen::PeerBrowser => render_peer_browser(frame, theme, state, area),
        AuxiliaryScreen::PeerDetail {
            peer_id,
            mode: ViewMode::Json,
            json,
            ..
        } => {
            if let Some(document) = peer_json_document(&state.peers, *peer_id) {
                render_json_view(
                    frame,
                    theme,
                    &document,
                    json,
                    &state.store,
                    state.unmask_identity_addresses,
                    area,
                );
            } else {
                render_missing_object(frame, theme, "Peer", area);
            }
        }
        AuxiliaryScreen::PeerDetail {
            peer_id, scroll, ..
        } => render_peer_detail(frame, theme, state, *peer_id, *scroll, area),
    }
}

fn render_search_bar(frame: &mut Frame<'_>, theme: &Theme, state: &AppState, area: Rect) {
    let icon = theme.glyph("🔎", "/");
    let icon = Span::styled(format!(" {icon} "), Style::default().fg(theme.link));
    let content = if state.search.active {
        Line::from(vec![
            icon,
            Span::raw(state.search.query.clone()),
            Span::styled(" ", theme.selection_cursor()),
        ])
    } else if let Some(status) = &state.search.status {
        let color = if status.error {
            theme.negative
        } else {
            theme.warning
        };
        Line::from(vec![
            icon,
            Span::styled(
                short_id(&state.search.query),
                Style::default().fg(theme.link),
            ),
            Span::raw("  "),
            Span::styled(status.message.clone(), Style::default().fg(color)),
        ])
    } else {
        Line::from(vec![
            icon,
            Span::styled(
                "Search block height/hash/unix time/datetime, TXID, name@, r/i-address, currency@@",
                Style::default().fg(theme.search_placeholder),
            ),
            Span::styled("  /", Style::default().fg(theme.link)),
        ])
    };
    frame.render_widget(
        Paragraph::new(content).style(theme.search_field(state.search.active)),
        area,
    );
}

fn modal_controls(state: &AppState) -> Option<&'static str> {
    if let Some(prompt) = &state.peers.controls.prompt {
        return Some(prompt.controls());
    }
    if let Some(prompt) = &state.cache_clear_prompt {
        return Some(if prompt.clearing {
            "Clearing cache | q quit"
        } else {
            "y clear cache | n/Esc cancel | q quit"
        });
    }
    if state.save_prompt.is_some() {
        return Some("Enter save JSON | Esc cancel");
    }
    if state.search.active {
        return Some("Enter submit search | Esc cancel | Backspace delete");
    }
    if let Screen::Currencies(view) = &state.screen {
        if view.options_open {
            return Some(
                "j/k select | Space/Enter cycle flag | a any | p liquidity pools | Tab launch status | Shift-F/Esc close | q quit",
            );
        }
        if view.launch_open {
            return Some(
                "j/k select | Space/Enter toggle status | l/p/f/u one status | a all | d defaults | Tab flags | Shift-F/Esc close | q quit",
            );
        }
        if view.editing {
            return Some("Type to filter currencies | Enter/Esc done");
        }
    }
    if chart_ui::prompt_active(state) {
        return Some("Enter seek | Esc cancel | Ctrl-U clear");
    }
    if matches!(
        &state.screen,
        Screen::Identity {
            historical_height_input: Some(_),
            ..
        }
    ) {
        return Some("Enter look up height | Esc cancel");
    }
    if text_input_active(state) {
        return Some("Enter look up height or date | Esc cancel");
    }
    None
}

fn mouse_mode_label(mouse_capture: bool) -> &'static str {
    if mouse_capture {
        "Mouse: scroll/copy"
    } else {
        "Mouse: select/paste"
    }
}

fn footer_paragraph(state: &AppState, theme: &Theme, mouse_capture: bool) -> Paragraph<'static> {
    let mouse = mouse_mode_label(mouse_capture);
    let mut lines = if let Some(modal) = modal_controls(state) {
        let status = if mouse_capture {
            "Mouse capture on"
        } else {
            mouse
        };
        vec![Line::from(modal), Line::styled(status, theme.muted())]
    } else {
        let view = match &state.auxiliary {
            Some(AuxiliaryScreen::Help { .. }) => {
                "j/k or arrows scroll | c clear cache | t time display | y clipboard test | Esc back"
            }
            Some(AuxiliaryScreen::PeerBrowser) => {
                "a add node | c clear bans | p ping all | j/k select | Enter open | w map | g GeoIP | Esc back"
            }
            Some(AuxiliaryScreen::PeerDetail {
                mode: ViewMode::Overview,
                ..
            }) => "x disconnect | b ban | j/k scroll | p/n prev/next | Tab JSON | h back",
            Some(AuxiliaryScreen::PeerDetail { .. }) => {
                "x disconnect | b ban | j/k move | y yank | s save | Tab overview | h back"
            }
            _ if is_json_mode(state)
                && current_json_entry(state).is_some_and(|entry| entry.scalar_value.is_some()) =>
            {
                "j/k scroll value | Space/Enter collapse | y yank | s save | Left/h back"
            }
            _ if is_json_mode(state) => json_control_help(),
            _ => control_help(&state.screen),
        };
        let linked_view = if state.auxiliary.is_none() {
            linked_identity_currency(state)
        } else {
            None
        };
        let views = if state.auxiliary.is_some() {
            "d dashboard | m mempool | o marketplace | ` peers"
        } else if linked_view.is_some() {
            "/ search | d dashboard | m mempool | o marketplace | F9 charts | ` peers"
        } else {
            "/ search | d dashboard | m mempool | o marketplace | c currencies | F9 charts | ` peers"
        };
        vec![
            Line::from(vec![
                Span::styled("controls ", theme.muted()),
                Span::raw(match linked_view {
                    Some(LinkedView::Currency(_)) => "c open currency | ",
                    Some(LinkedView::Identity(_)) => "c open identity | ",
                    None => "",
                }),
                Span::raw(if bookmark_ui::supports_current(state) {
                    "Shift-B add bookmark | "
                } else {
                    ""
                }),
                Span::raw(view),
            ]),
            Line::from(vec![
                Span::styled("F2 sources | ", theme.link()),
                Span::styled(BOOKMARKS_HINT, theme.link()),
                Span::raw("F1/? help | "),
                Span::styled(format!("{mouse} (Shift-M) | "), theme.warning()),
                Span::raw(views),
                Span::raw(" | F10 estimate | Shift-I names/IDs | Shift-D dates | q quit"),
            ]),
        ]
    };
    if state.search.pending_request.is_some()
        && !state.search.active
        && modal_controls(state).is_none()
    {
        lines[0]
            .spans
            .insert(1, Span::styled("Esc cancel lookup | ", theme.warning()));
    }
    Paragraph::new(lines)
        .style(theme.base())
        .wrap(Wrap { trim: true })
}

fn control_help(screen: &Screen) -> &'static str {
    match screen {
        Screen::Dashboard => {
            "Tab switch lists | j/k select | Home/End first/last | PgUp/PgDn page | Enter open | F5 refresh"
        }
        Screen::Mempool => {
            "j/k select transaction | Home/End first/last | PgUp/PgDn page | Enter opens"
        }
        Screen::Marketplace => {
            "j/k select offer, a/Shift-A association next/prev, s/Shift-S sort next/prev, r reverse order, Enter opens, F5 refreshes"
        }
        Screen::Estimate(_) => "Left/h or Esc back",
        Screen::Currency(view) if view.mode == ViewMode::Overview => {
            "j/k selects links, Enter opens, n notarizations, g height/date, a height activity, w 1000-block window, [/] adjacent, F8 batch inputs, F5/r refreshes, Tab JSON, Left/h back"
        }
        Screen::Currency(_) => json_control_help(),
        Screen::Bytes(_) => {
            "Arrows or h/j/k/l move byte, PgUp/PgDn page, Home/End first/last byte, Esc back"
        }
        Screen::Witnesses(view) => view.controls(),
        Screen::Notarizations(view) => {
            if view.has_perspectives() {
                "j/k select | Tab perspective | Enter transaction | [/] proof root | b block | F5/r refresh | Esc back"
            } else {
                "j/k select | Enter transaction | [/] proof root | b block | F5/r refresh | Esc back"
            }
        }
        Screen::Currencies(_) => {
            "j/k selects currency, Enter opens, f text filter, Shift-F flags/status, F5/r refreshes, Esc back"
        }
        Screen::Charts(_) => {
            "1/2/3/4/5 chart, v currency/z scale/t detail (fee pool), g seek height/time, j older/k newer, -/+ span, PgUp/[ older range, PgDn/] newer, Enter opens block, Home/End, F5 refresh, Esc back"
        }
        Screen::Block {
            mode: ViewMode::Overview,
            ..
        } => {
            "j/k select tx | Enter open | p/n prev/next block | a active block | F7 branches | Tab JSON | Left/h back"
        }
        Screen::Transaction {
            mode: ViewMode::Overview,
            ..
        } => {
            "j/k select input/output | Enter follow | b block | F3 links | F4 witnesses | F7 inclusions | F8 DeFi trail | e offer proof | Tab JSON | Left/h back"
        }
        Screen::Address { .. } => {
            "j/k or arrows select identity/tx, Enter/Right/l opens, Left/h or Esc goes back"
        }
        Screen::Identity {
            mode: ViewMode::Overview,
            ..
        } => {
            "j/k selects address/tx, Enter/Right/l opens, p previous update, n next update, v historical reverse window, Tab JSON"
        }
        Screen::Block {
            mode: ViewMode::Json,
            ..
        }
        | Screen::Transaction {
            mode: ViewMode::Json,
            ..
        }
        | Screen::Identity {
            mode: ViewMode::Json,
            ..
        } => json_control_help(),
    }
}

fn json_control_help() -> &'static str {
    "j/k or arrows move, Enter/Right/l follows or expands, Space expands, y yanks value, s saves, Left/h back, Tab/Shift-Tab toggles Overview"
}

fn breadcrumb_line(theme: &Theme, state: &AppState, width: u16) -> Line<'static> {
    let mut labels = state
        .screen_stack
        .iter()
        .map(|screen| screen_label(screen, state, theme))
        .collect::<Vec<_>>();
    labels.push(screen_label(&state.screen, state, theme));
    breadcrumb_text(theme, &labels.join(" > "), width)
}

fn witness_breadcrumb(
    origin: &AppState,
    destination: &AppState,
    theme: &Theme,
    depth: usize,
) -> Option<String> {
    let trail = destination.screen_stack.get(depth..)?;
    if !matches!(origin.screen, Screen::Witnesses(_))
        && !trail
            .iter()
            .chain(std::iter::once(&destination.screen))
            .any(|screen| matches!(screen, Screen::Witnesses(_)))
    {
        return None;
    }
    let mut labels = origin
        .screen_stack
        .iter()
        .chain(std::iter::once(&origin.screen))
        .map(|screen| screen_label(screen, origin, theme))
        .collect::<Vec<_>>();
    labels.extend(
        trail
            .iter()
            .chain(std::iter::once(&destination.screen))
            .map(|screen| screen_label(screen, destination, theme)),
    );
    Some(labels.join(" > "))
}

fn breadcrumb_text(theme: &Theme, text: &str, width: u16) -> Line<'static> {
    Line::from(vec![
        Span::styled("path ", theme.muted()),
        Span::raw(truncate_middle(text, usize::from(width.saturating_sub(5)))),
    ])
}

fn screen_label(screen: &Screen, state: &AppState, theme: &Theme) -> String {
    match screen {
        Screen::Dashboard => theme.glyph("🖥", "▣").to_string(),
        Screen::Mempool => "Mempool".to_string(),
        Screen::Marketplace => "Marketplace".to_string(),
        Screen::Charts(_) => "Charts".to_string(),
        Screen::Estimate(_) => "Future estimate".to_string(),
        Screen::Bytes(_) => "Bytes".into(),
        Screen::Notarizations(_) => "Notarizations".into(),
        Screen::Witnesses(_) => "Witnesses".into(),
        Screen::Currencies(_) => "Currencies".into(),
        Screen::Currency(view) => {
            format!("Currency {}", view.name.as_deref().unwrap_or(&view.query))
        }
        Screen::Block { target, .. } => state
            .store
            .block_for_target(target)
            .map(|block| format!("Bk {}", block.summary.height))
            .unwrap_or_else(|| format!("Bk {}", block_target_label(target))),
        Screen::Transaction { txid, .. } => format!("Tx {}", short_id(txid)),
        Screen::Address { address, .. } => format!("Addr {}", short_id(address)),
        Screen::Identity { target, .. } => state
            .store
            .identity_for_target(target)
            .map(|identity| {
                format!(
                    "ID {}@{}",
                    identity.fully_qualified_name.trim_end_matches('@'),
                    identity.block_height
                )
            })
            .unwrap_or_else(|| format!("ID {}", short_id(&target.query))),
    }
}

fn block_target_label(target: &BlockTarget) -> String {
    match target {
        BlockTarget::Hash(hash) => short_id(hash),
        BlockTarget::Height(height) => height.to_string(),
    }
}

fn short_id(value: &str) -> String {
    if value.len() <= 12 {
        value.to_string()
    } else {
        format!("{}...{}", &value[..6], &value[value.len() - 6..])
    }
}

fn truncate_middle(value: &str, max_chars: usize) -> String {
    let char_count = value.chars().count();
    if char_count <= max_chars {
        return value.to_string();
    }
    if max_chars <= 5 {
        return value.chars().take(max_chars).collect();
    }

    let marker = "...";
    let keep = max_chars - marker.len();
    let start_len = keep / 2;
    let end_len = keep - start_len;
    let start = value.chars().take(start_len).collect::<String>();
    let end = value
        .chars()
        .rev()
        .take(end_len)
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>();
    format!("{start}{marker}{end}")
}

fn render_dashboard(
    frame: &mut Frame<'_>,
    config: &Config,
    theme: &Theme,
    state: &AppState,
    now: i64,
    area: Rect,
) -> Range<usize> {
    let body = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(5), Constraint::Min(3)])
        .split(area);

    let summary = Paragraph::new(vec![
        Line::from(vec![Span::styled("Dashboard", theme.focused())]),
        Line::from(vec![
            Span::styled("Start object: ", theme.muted()),
            Span::styled(state.route_label.clone(), theme.focused()),
        ]),
        Line::from(vec![
            Span::styled("Daemon tip: ", theme.muted()),
            Span::styled(
                tip_status_text(&state.tip, now),
                if state.tip.healthy {
                    theme.positive()
                } else {
                    theme.negative()
                },
            ),
        ]),
        Line::from("Use Up/Down or j/k to select a block. Enter/Right/l opens it."),
    ])
    .style(theme.base())
    .wrap(Wrap { trim: true });
    frame.render_widget(summary, body[0]);

    let lists = body[1];
    if lists.width < 72 {
        if state.dashboard.transactions_focused {
            render_dashboard_transactions(frame, config, theme, state, lists)
        } else {
            render_block_tiles(frame, theme, state, lists);
            0..0
        }
    } else {
        let [blocks, gap, transactions] = Layout::horizontal([
            Constraint::Length(lists.width / 2 - 1),
            Constraint::Length(3),
            Constraint::Min(1),
        ])
        .areas(lists);
        frame.render_widget(Block::default().style(theme.base()), gap);
        render_block_tiles(frame, theme, state, blocks);
        render_dashboard_transactions(frame, config, theme, state, transactions)
    }
}

fn render_mempool(
    frame: &mut Frame<'_>,
    config: &Config,
    theme: &Theme,
    state: &AppState,
    now: i64,
    area: Rect,
) {
    let body = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(5), Constraint::Min(3)])
        .split(area);
    let refresh = if state.mempool.loading {
        "refreshing".to_string()
    } else {
        state
            .mempool
            .last_refresh
            .map(|last| {
                format!(
                    "updated {} ago",
                    human_duration(last.elapsed().as_secs() as i64)
                )
            })
            .unwrap_or_else(|| "waiting for first refresh".to_string())
    };
    let last_block = state
        .tip
        .last_block_time
        .map(|_| {
            format!(
                "{} ago",
                last_block_age_text(state.tip.last_block_time, now)
            )
        })
        .unwrap_or_else(|| "unknown".to_string());
    let mut summary = vec![
        Line::from(vec![Span::styled("Mempool", theme.focused())]),
        Line::from(vec![
            Span::styled("Transactions ", theme.muted()),
            Span::styled(state.mempool.entries.len().to_string(), theme.focused()),
            Span::styled("  Serialized size ", theme.muted()),
            Span::styled(format_data_size(state.mempool.total_bytes), theme.focused()),
        ]),
        Line::from(vec![
            Span::styled("Last block ", theme.muted()),
            Span::raw(last_block),
            Span::styled("  Refresh ", theme.muted()),
            Span::raw(format!("2s, {refresh}")),
        ]),
    ];
    if let Some(error) = &state.mempool.error {
        summary.push(Line::from(vec![
            Span::styled("RPC error ", theme.negative()),
            Span::raw(error.clone()),
        ]));
    }
    frame.render_widget(
        Paragraph::new(summary)
            .style(theme.base())
            .wrap(Wrap { trim: true }),
        body[0],
    );

    if state.mempool.entries.is_empty() {
        let message = if state.mempool.loading && state.mempool.last_refresh.is_none() {
            "Loading mempool..."
        } else if state.mempool.error.is_some() {
            "Mempool data is unavailable."
        } else {
            "Mempool is empty."
        };
        frame.render_widget(
            Paragraph::new(message)
                .style(theme.muted())
                .alignment(Alignment::Center),
            body[1],
        );
        return;
    }

    render_mempool_tiles(frame, config, theme, state, now, body[1]);
}

fn render_marketplace(frame: &mut Frame<'_>, theme: &Theme, state: &AppState, area: Rect) {
    let body = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(5), Constraint::Min(3)])
        .split(area);
    let refresh = if state.marketplace.loading {
        "loading".to_string()
    } else if let Some(hash) = state.marketplace.last_checked_tip.as_deref() {
        state
            .store
            .block(hash)
            .map(|block| format!("checked at block {}", block.summary.height))
            .unwrap_or_else(|| format!("checked at {}", short_id(hash)))
    } else {
        "waiting for chain tip".to_string()
    };
    let mut summary = vec![
        Line::from(Span::styled("Marketplace", theme.focused())),
        Line::from(vec![
            Span::styled("Open offers involving ", theme.muted()),
            Span::styled(state.marketplace.subject.clone(), theme.focused()),
            Span::styled("  Count ", theme.muted()),
            Span::raw(state.marketplace.offers.len().to_string()),
        ]),
        Line::from(vec![
            Span::styled("Source ", theme.muted()),
            Span::raw("getoffers"),
            Span::styled("  Refresh ", theme.muted()),
            Span::raw(refresh),
            Span::styled(" (F5)", theme.focused()),
        ]),
    ];
    summary.push(Line::from(format!(
        "Sort: {} {}  (s: field, r: reverse)",
        state.marketplace.sort.label(),
        if state.marketplace.descending {
            "descending"
        } else {
            "ascending"
        }
    )));
    if let Some(error) = &state.marketplace.error {
        summary.push(Line::from(vec![
            Span::styled("RPC error ", theme.negative()),
            Span::raw(error.clone()),
        ]));
    }
    frame.render_widget(
        Paragraph::new(summary)
            .style(theme.base())
            .wrap(Wrap { trim: true }),
        body[0],
    );

    if state.marketplace.offers.is_empty() {
        let message = if state.marketplace.loading {
            "Loading open marketplace offers..."
        } else if state.marketplace.error.is_some() {
            "Marketplace data is unavailable."
        } else {
            "No open offers were returned for this currency."
        };
        frame.render_widget(
            Paragraph::new(message)
                .style(theme.muted())
                .alignment(Alignment::Center),
            body[1],
        );
        return;
    }

    render_marketplace_tiles(frame, theme, state, body[1]);
}

fn render_marketplace_tiles(frame: &mut Frame<'_>, theme: &Theme, state: &AppState, area: Rect) {
    let count = state.marketplace.offers.len();
    let visible =
        usize::from(area.height.saturating_sub(SECTION_HEADING_HEIGHT) / MARKETPLACE_TILE_HEIGHT)
            .max(1)
            .min(count);
    let first = state
        .marketplace
        .selected
        .saturating_sub(visible / 2)
        .min(count.saturating_sub(visible));
    let last = (first + visible).min(count);
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("Open offers ", theme.focused()),
            Span::styled(format!("{}-{} of {count}", first + 1, last), theme.muted()),
        ])),
        Rect { height: 1, ..area },
    );
    let list_area = Rect {
        y: area.y.saturating_add(SECTION_HEADING_HEIGHT),
        height: area.height.saturating_sub(SECTION_HEADING_HEIGHT),
        ..area
    };
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints(vec![Constraint::Length(MARKETPLACE_TILE_HEIGHT); visible])
        .split(scroll_view::content_area(list_area, count > visible));
    for (row, offer) in rows.iter().zip(&state.marketplace.offers[first..last]) {
        let selected = state
            .marketplace
            .offers
            .get(state.marketplace.selected)
            .is_some_and(|selected| selected.txid == offer.txid);
        let remaining = state
            .tip
            .height
            .map(|height| offer.expiry_height.saturating_sub(height));
        let expiry = if offer.expiry_height == 0 {
            "no expiry".into()
        } else {
            remaining
                .map(|blocks| {
                    format!(
                        "expires {} ({blocks} blocks)",
                        theme.format_height(offer.expiry_height)
                    )
                })
                .unwrap_or_else(|| format!("expires {}", theme.format_height(offer.expiry_height)))
        };
        let subject = offer
            .subject_id
            .as_deref()
            .map(|id| resolved_marketplace_name(id, &state.store))
            .unwrap_or_else(|| offer.category.clone());
        let price = marketplace_price_label(offer, &state.store);
        let link_spans = marketplace_links(offer, &state.store)
            .iter()
            .enumerate()
            .flat_map(|(index, link)| {
                let mut spans = Vec::new();
                if index > 0 {
                    spans.push(Span::styled(" | ", theme.muted()));
                }
                let style = if selected && index == state.marketplace.selected_link {
                    theme.selection_cursor()
                } else {
                    theme.link()
                };
                spans.push(theme.value(
                    "offer reference",
                    marketplace_link_label(link, &state.store),
                    style,
                ));
                spans
            })
            .collect::<Vec<_>>();
        let mut assets =
            marketplace_asset_spans(&offer.offered, &state.store, theme, theme.positive());
        assets.push(Span::styled("  ->  ", theme.muted()));
        assets.extend(marketplace_asset_spans(
            &offer.accepted,
            &state.store,
            theme,
            theme.warning(),
        ));
        render_summary_tile(
            frame,
            theme,
            SummaryTile {
                title: None,
                selected,
                lines: vec![
                    justified_line(
                        vec![
                            Span::raw("offer "),
                            theme.value_as(
                                "offer transaction ID",
                                short_id(&offer.txid),
                                offer.txid.as_str(),
                                theme.link(),
                            ),
                        ],
                        vec![theme.value("expiry", expiry, theme.muted())],
                        tile_content_area(*row).width,
                    ),
                    Line::from(assets),
                    justified_line(
                        vec![theme.value("offer subject", subject, theme.muted())],
                        vec![theme.value("asking price", price, Style::default())],
                        tile_content_area(*row).width,
                    ),
                    Line::from(link_spans),
                ],
                alignment: Alignment::Left,
            },
            *row,
        );
    }
    scroll_view::render(frame, theme, list_area, count, first, visible);
}

fn resolved_marketplace_name(value: &str, store: &ObjectStore) -> String {
    store
        .identity_name(value)
        .or_else(|| store.currency_name(value))
        .unwrap_or(value)
        .to_string()
}

/// The daemon's index price can be an inverse ratio, including native value
/// bundled with an NFT. Derive the asking amount from the actual accept vector.
fn marketplace_asking_price(accepted: &Value) -> Option<MarketplaceAsk> {
    let fields = accepted.as_object()?;
    if fields.len() != 1 || fields.contains_key("name") || fields.contains_key("identityid") {
        return None;
    }
    let (currency, value) = fields.iter().next()?;
    let amount = decimal_value_to_zat(value)?;
    (amount > 0).then(|| MarketplaceAsk {
        currency: currency.clone(),
        amount,
    })
}

fn marketplace_price_label(offer: &MarketplaceOffer, store: &ObjectStore) -> String {
    if let Some(ask) = &offer.asking_price {
        format!(
            "Ask {}",
            currency_amount_label(currency_display_value(store, &ask.currency), ask.amount)
        )
    } else if offer.accepted.get("identityid").is_some() || offer.accepted.get("name").is_some() {
        "Ask identity exchange".into()
    } else {
        "Ask asset bundle".into()
    }
}

fn marketplace_asset_spans(
    value: &Value,
    store: &ObjectStore,
    theme: &Theme,
    style: Style,
) -> Vec<Span<'static>> {
    let label = marketplace_asset_label(value, store);
    let identity = value.as_object().is_some_and(|fields| {
        ["name", "identityid"]
            .iter()
            .any(|key| fields.get(*key).is_some_and(Value::is_string))
    });
    if identity && let Some(name) = label.strip_prefix("ID ") {
        vec![
            Span::styled("ID ", theme.muted()),
            theme.value_as(
                "identity name",
                format!(" {name} "),
                name,
                theme.status_badge(),
            ),
        ]
    } else {
        vec![theme.value("offer asset", label, style)]
    }
}

fn marketplace_asset_label(value: &Value, store: &ObjectStore) -> String {
    if let Some(fields) = value.as_object() {
        if let Some(name) = fields.get("name").and_then(Value::as_str) {
            return format!("ID {name}@");
        }
        if let Some(identity) = fields.get("identityid").and_then(Value::as_str) {
            return format!("ID {}", resolved_marketplace_name(identity, store));
        }
        let amounts = fields
            .iter()
            .filter_map(|(currency, amount)| {
                amount.as_f64().map(|amount| {
                    format!("{amount:.8} {}", currency_display_value(store, currency))
                })
            })
            .collect::<Vec<_>>();
        if !amounts.is_empty() {
            return amounts.join(" + ");
        }
    }
    value.as_str().unwrap_or("asset").to_string()
}

fn marketplace_links(offer: &MarketplaceOffer, store: &ObjectStore) -> Vec<MarketplaceLink> {
    let mut links = vec![MarketplaceLink::Funding(offer.txid.clone())];
    for asset in [&offer.offered, &offer.accepted] {
        let Some(fields) = asset.as_object() else {
            continue;
        };
        if let Some(identity) = fields.get("identityid").and_then(Value::as_str) {
            let link = MarketplaceLink::Identity(identity.to_string());
            if !links.contains(&link) {
                links.push(link);
            }
        }
        for (currency, amount) in fields {
            if amount.as_f64().is_some() {
                let link = MarketplaceLink::Currency(currency.clone());
                if !links.contains(&link) {
                    links.push(link);
                }
            }
        }
    }
    if let Some(transaction) = store.transaction(&offer.txid) {
        for address in transaction
            .raw_json
            .get("vout")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .flat_map(addresses_in)
        {
            let link = MarketplaceLink::Address(address);
            if !links.contains(&link) {
                links.push(link);
            }
        }
    }
    links
}

fn marketplace_link_label(link: &MarketplaceLink, store: &ObjectStore) -> String {
    match link {
        MarketplaceLink::Funding(_) => "funding tx".to_string(),
        MarketplaceLink::Identity(identity) => {
            format!("ID {}", resolved_marketplace_name(identity, store))
        }
        MarketplaceLink::Currency(currency) => {
            format!("asset {}", currency_display_value(store, currency))
        }
        MarketplaceLink::Address(address) => format!("addr {}", short_id(address)),
    }
}

fn render_mempool_tiles(
    frame: &mut Frame<'_>,
    config: &Config,
    theme: &Theme,
    state: &AppState,
    now: i64,
    area: Rect,
) {
    let count = state.mempool.entries.len();
    let visible =
        usize::from(area.height.saturating_sub(SECTION_HEADING_HEIGHT) / MEMPOOL_TILE_HEIGHT)
            .max(1)
            .min(count);
    let max_first = count.saturating_sub(visible);
    let first = state
        .mempool
        .selected
        .saturating_sub(visible / 2)
        .min(max_first);
    let last = (first + visible).min(count);
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("Transactions ", theme.focused()),
            Span::styled(format!("{}-{} of {count}", first + 1, last), theme.muted()),
        ]))
        .style(theme.base()),
        Rect { height: 1, ..area },
    );
    let list_area = Rect {
        y: area.y.saturating_add(SECTION_HEADING_HEIGHT),
        height: area.height.saturating_sub(SECTION_HEADING_HEIGHT),
        ..area
    };
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints(vec![Constraint::Length(MEMPOOL_TILE_HEIGHT); visible])
        .split(scroll_view::content_area(list_area, count > visible));
    let currency = native_currency_name(config, &state.store);
    for (row, entry) in rows.iter().zip(&state.mempool.entries[first..last]) {
        let selected = state
            .mempool
            .entries
            .get(state.mempool.selected)
            .is_some_and(|selected| selected.txid == entry.txid);
        render_mempool_tile(
            frame,
            theme,
            entry,
            (
                &currency,
                &state.store,
                state.mempool.previews.get(&entry.txid),
            ),
            selected,
            now,
            *row,
        );
    }
    scroll_view::render(frame, theme, list_area, count, first, visible);
}

fn render_mempool_tile(
    frame: &mut Frame<'_>,
    theme: &Theme,
    entry: &MempoolEntry,
    context: (&str, &ObjectStore, Option<&TransactionSummary>),
    selected: bool,
    now: i64,
    area: Rect,
) {
    let (currency, store, preview) = context;
    let arrival = if entry.time == 0 {
        "arrival unknown".to_string()
    } else {
        format!(
            "{} ago",
            human_duration(now.saturating_sub(entry.time as i64))
        )
    };
    let fee = entry
        .fee
        .map(|fee| format!("fee {fee:.8} {currency}"))
        .unwrap_or_else(|| "fee unknown".to_string());
    let height = entry
        .height
        .map(|height| format!("entry height {}", theme.format_height(height)))
        .unwrap_or_else(|| "entry height unknown".to_string());
    let dependencies = match entry.depends.len() {
        0 => "no unconfirmed parents".to_string(),
        1 => "1 unconfirmed parent".to_string(),
        count => format!("{count} unconfirmed parents"),
    };
    let width = tile_content_area(area).width;
    let summary = store
        .transaction(&entry.txid)
        .map(|transaction| &transaction.summary)
        .or(preview);
    let tags = summary
        .map(|summary| display_transaction_tags(summary, store))
        .unwrap_or_default();
    let mut tag_line = if tags.is_empty() {
        Line::from(Span::styled(
            if summary.is_some() {
                ""
            } else {
                "classification pending"
            },
            theme.muted(),
        ))
    } else {
        Line::from(transaction_tag_spans(theme, &tags))
    };
    if store.chain.in_mempool(&entry.txid) {
        tag_line
            .spans
            .insert(0, badge_span(theme, StatusBadge::Mempool));
        tag_line.spans.insert(1, Span::raw(" "));
    }
    render_summary_tile(
        frame,
        theme,
        SummaryTile {
            title: None,
            selected,
            lines: vec![
                justified_line(
                    vec![theme.value_as(
                        "transaction ID",
                        short_id(&entry.txid),
                        entry.txid.as_str(),
                        Style::default(),
                    )],
                    vec![theme.value("arrival", arrival, Style::default())],
                    width,
                ),
                justified_line(
                    vec![theme.value("fee", fee, Style::default())],
                    vec![theme.value("size", format_data_size(entry.size), Style::default())],
                    width,
                ),
                justified_line(
                    vec![theme.value("entry height", height, theme.muted())],
                    vec![theme.value("dependencies", dependencies, theme.muted())],
                    width,
                ),
                tag_line,
            ],
            alignment: Alignment::Left,
        },
        area,
    );
}

fn format_data_size(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let bytes_float = bytes as f64;
    if bytes_float >= GIB {
        format!("{:.2} GiB", bytes_float / GIB)
    } else if bytes_float >= MIB {
        format!("{:.2} MiB", bytes_float / MIB)
    } else if bytes_float >= KIB {
        format!("{:.1} KiB", bytes_float / KIB)
    } else {
        format!("{bytes} B")
    }
}

fn dashboard_heading_style(theme: &Theme, focused: bool) -> Style {
    if focused {
        theme.selection_cursor()
    } else {
        theme.focused()
    }
}

fn render_dashboard_transactions(
    frame: &mut Frame<'_>,
    config: &Config,
    theme: &Theme,
    state: &AppState,
    area: Rect,
) -> Range<usize> {
    let dashboard = &state.dashboard;
    let count = dashboard.recent_transactions.len();
    let inner = Rect {
        y: area.y.saturating_add(SECTION_HEADING_HEIGHT),
        height: area.height.saturating_sub(SECTION_HEADING_HEIGHT),
        ..area
    };
    let content = scroll_view::content_area(inner, count > 1);
    let radius = usize::from(inner.height);
    let selected = dashboard.selected_transaction;
    let base = selected.saturating_sub(radius);
    let end = (selected + radius + 1).min(count);
    let currency = native_currency_name(config, &state.store);
    let tiles: Vec<_> = dashboard.recent_transactions[base..end]
        .iter()
        .enumerate()
        .filter_map(|(index, entry)| {
            let block = state.store.block(&entry.block)?;
            let summary = block.summary.tx.get(entry.position)?;
            let summary = state
                .store
                .transaction(&entry.txid)
                .map_or(summary, |tx| &tx.summary);
            let mut tile = transaction_summary_tile(
                theme,
                TransactionTileContext {
                    currency: &currency,
                    store: &state.store,
                    block_time: block.summary.time,
                },
                summary,
                dashboard.transactions_focused && base + index == selected,
                content.width,
            );
            tile.lines[2].spans.insert(0, Span::raw("  "));
            tile.lines[2].spans.insert(
                0,
                theme.value(
                    "block height",
                    format!("Block {}", theme.format_height(block.summary.height)),
                    theme.muted(),
                ),
            );
            Some(tile)
        })
        .collect();
    let width = tile_content_area(content).width;
    let heights: Vec<_> = tiles
        .iter()
        .map(|tile| wrapped_tile_height(&tile.lines, width))
        .collect();
    let (first, last) =
        variable_height_visible_range(&heights, selected.saturating_sub(base), inner.height);
    let range = if count == 0 {
        "0 of 0".into()
    } else {
        format!("{}-{} of {count}", base + first + 1, base + last)
    };
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(
                "Recent Transactions ",
                dashboard_heading_style(theme, dashboard.transactions_focused),
            ),
            Span::styled(range, theme.muted()),
        ]))
        .style(theme.base()),
        Rect {
            height: area.height.min(1),
            ..area
        },
    );
    if tiles.is_empty() {
        let message = if dashboard.recent_blocks.is_empty() {
            "Waiting for daemon data..."
        } else {
            "No transactions in recent blocks."
        };
        frame.render_widget(Paragraph::new(message).style(theme.muted()), inner);
        return 0..0;
    }
    let rows = Layout::vertical(heights[first..last].iter().copied().map(Constraint::Length))
        .split(content);
    for (row, tile) in rows
        .iter()
        .zip(tiles.into_iter().skip(first).take(last - first))
    {
        render_summary_tile(frame, theme, tile, *row);
    }
    scroll_view::render(frame, theme, inner, count, base + first, last - first);
    base + first..base + last
}

fn wrapped_tile_height(lines: &[Line<'_>], width: u16) -> u16 {
    u16::try_from(
        Paragraph::new(lines.to_vec())
            .wrap(Wrap { trim: true })
            .line_count(width.max(1)),
    )
    .unwrap_or(u16::MAX)
    .saturating_add(1)
}

fn render_block_tiles(frame: &mut Frame<'_>, theme: &Theme, state: &AppState, area: Rect) {
    let dashboard = &state.dashboard;
    let block_count = dashboard.recent_blocks.len();
    let row_count = block_count + 1;
    let content = scroll_view::content_area(area, row_count > 1);
    let selected = dashboard.block_cursor.min(block_count);
    let radius = usize::from(area.height);
    let base = selected.saturating_sub(radius);
    let end = (selected + radius + 1).min(row_count);
    let tile_heights = (base..end)
        .map(|row| {
            if row == 0 {
                1
            } else {
                state
                    .store
                    .block(&dashboard.recent_blocks[row - 1])
                    .map(|block| {
                        block_summary_tile_height(theme, &state.store, block, content.width)
                    })
                    .unwrap_or(4)
            }
        })
        .collect::<Vec<_>>();
    let available_height = area.height.saturating_sub(SECTION_HEADING_HEIGHT);
    let loading_line_height =
        u16::from(dashboard.loading_older_since.is_some() && available_height > 1);
    let list_height = available_height - loading_line_height;
    let (first, last) = variable_height_visible_range(&tile_heights, selected - base, list_height);
    let first_block = (base + first).max(1);
    let last_block = base + last - 1;
    let heading = Paragraph::new(Line::from(vec![
        Span::styled(
            "Recent Blocks ",
            dashboard_heading_style(theme, !state.dashboard.transactions_focused),
        ),
        Span::styled(
            if last_block < first_block {
                format!("0 of {block_count}")
            } else {
                format!("{first_block}-{last_block} of {block_count}")
            },
            theme.muted(),
        ),
        Span::styled(
            if dashboard.older_error.is_some() {
                "  older blocks unavailable; retrying"
            } else if !state.dashboard.has_more {
                "  genesis reached"
            } else {
                ""
            },
            theme.muted(),
        ),
    ]))
    .style(theme.base());
    let inner = Rect {
        y: area.y.saturating_add(SECTION_HEADING_HEIGHT),
        height: list_height,
        ..area
    };
    frame.render_widget(
        heading,
        Rect {
            height: area.height.min(1),
            ..area
        },
    );
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints(
            tile_heights[first..last]
                .iter()
                .copied()
                .map(Constraint::Length)
                .collect::<Vec<_>>(),
        )
        .split(Rect {
            width: content.width,
            ..inner
        });

    for (row, index) in rows.iter().zip(base + first..base + last) {
        let focused = !dashboard.transactions_focused && index == selected;
        if index == 0 {
            let body = render_tile_gutter(frame, theme, focused, *row);
            frame.render_widget(
                Paragraph::new("Stream Blocks").style(theme.tile_body(focused)),
                body,
            );
            continue;
        }
        let hash = &dashboard.recent_blocks[index - 1];
        let Some(block) = state.store.block(hash) else {
            continue;
        };
        render_block_tile(frame, theme, &state.store, block, focused, *row);
    }
    scroll_view::render(frame, theme, inner, row_count, base + first, last - first);
    let next_line = inner.y
        + tile_heights[first..last]
            .iter()
            .fold(0_u16, |height, tile| height.saturating_add(*tile))
            .min(inner.height);
    if block_count == 0 && next_line < inner.bottom() {
        frame.render_widget(
            Paragraph::new("Waiting for daemon data...").style(theme.muted()),
            Rect::new(inner.x, next_line, inner.width, 1),
        );
    }
    if let Some(started) = dashboard
        .loading_older_since
        .filter(|_| loading_line_height > 0)
    {
        let step = (started.elapsed().as_millis() / DASHBOARD_LOADING_FRAME.as_millis()) as usize;
        let frames = [
            ("|", "⠋"),
            ("/", "⠙"),
            ("-", "⠹"),
            ("\\", "⠸"),
            ("|", "⠼"),
            ("/", "⠴"),
            ("-", "⠦"),
            ("\\", "⠧"),
        ];
        let (ascii, unicode) = frames[step % frames.len()];
        frame.render_widget(
            Paragraph::new(format!(
                " {} loading older blocks...",
                theme.glyph(unicode, ascii)
            ))
            .style(theme.warning()),
            Rect::new(inner.x, next_line, content.width, 1),
        );
    }
}

fn badge_span(theme: &Theme, badge: StatusBadge) -> Span<'static> {
    Span::styled(format!(" {} ", badge.label()), theme.status_badge())
}

fn block_decoration_spans(
    theme: &Theme,
    block: &BlockRecord,
    store: &ObjectStore,
) -> Vec<Span<'static>> {
    let mut spans = block_tag_spans(theme, block, store);
    if let Some(badge) = store.chain.block_badge(&block.summary.hash) {
        if !spans.is_empty() {
            spans.push(Span::raw(" "));
        }
        spans.push(badge_span(theme, badge));
    }
    spans
}

fn block_summary_tile_height(
    theme: &Theme,
    store: &ObjectStore,
    block: &BlockRecord,
    width: u16,
) -> u16 {
    let width = tile_content_width(width);
    wrapped_tile_height(&block_tile_lines(theme, store, block, width), width)
}

fn render_block_tile(
    frame: &mut Frame<'_>,
    theme: &Theme,
    store: &ObjectStore,
    block: &BlockRecord,
    selected: bool,
    area: Rect,
) {
    render_summary_tile(
        frame,
        theme,
        SummaryTile {
            title: None,
            selected,
            lines: block_tile_lines(theme, store, block, tile_content_area(area).width),
            alignment: Alignment::Left,
        },
        area,
    );
}

fn block_tile_lines(
    theme: &Theme,
    store: &ObjectStore,
    block: &BlockRecord,
    content_width: u16,
) -> Vec<Line<'static>> {
    let production = block.summary.production_label();
    let production_style = match block.summary.production_kind() {
        BlockProductionKind::Mined => theme.warning(),
        BlockProductionKind::Staked => theme.positive(),
    };
    let size = block
        .summary
        .size
        .map(|size| format!("{size} bytes"))
        .unwrap_or_else(|| "unknown size".to_string());
    let fields = [
        (
            vec![
                Span::raw("height "),
                theme.value("height", block.summary.height.to_string(), theme.focused()),
            ],
            vec![
                Span::styled("time ", theme.muted()),
                theme.value(
                    "timestamp",
                    theme.format_time(block.summary.time),
                    Style::default(),
                ),
            ],
        ),
        (
            vec![
                Span::styled("hash ", theme.muted()),
                theme.value_as(
                    "block hash",
                    block.summary.short_hash(),
                    block.summary.hash.as_str(),
                    theme.link(),
                ),
            ],
            vec![theme.value("block size", size, theme.muted())],
        ),
        (
            vec![
                Span::styled("type ", theme.muted()),
                theme.value("block type", production, production_style),
            ],
            vec![
                Span::styled("tx ", theme.muted()),
                theme.value(
                    "transaction count",
                    block.summary.tx_count().to_string(),
                    Style::default(),
                ),
            ],
        ),
    ];
    let right_start = usize::from(content_width).saturating_sub(
        fields
            .iter()
            .map(|(_, right)| spans_width(right))
            .max()
            .unwrap_or(0),
    );
    let mut lines: Vec<_> = fields
        .into_iter()
        .map(|(mut left, right)| {
            let left_width = spans_width(&left);
            if left_width + 3 > right_start {
                return justified_line(left, right, content_width);
            }
            left.push(Span::raw(" ".repeat(right_start - left_width - 2)));
            left.push(Span::styled("│ ", theme.muted()));
            left.extend(right);
            Line::from(left)
        })
        .collect();
    let tags = block_decoration_spans(theme, block, store);
    if !tags.is_empty() {
        lines.push(Line::from(tags));
    }
    lines
}

fn render_block_view(
    frame: &mut Frame<'_>,
    config: &Config,
    theme: &Theme,
    state: &AppState,
    target: &BlockTarget,
    selected_tx: usize,
    area: Rect,
) {
    let Some(block) = state.store.block_for_target(target) else {
        render_missing_object(frame, theme, "Block", detail_header_content(area));
        return;
    };
    let production = block.summary.production_label();
    let size = block
        .summary
        .size
        .map(|size| format!("{size} bytes"))
        .unwrap_or_else(|| "unknown".to_string());
    let block_tags = block_decoration_spans(theme, block, &state.store);
    let header = Paragraph::new(vec![
        Line::from(vec![Span::styled("Block Header", theme.focused())]),
        Line::from(vec![
            Span::styled("Height: ", theme.muted()),
            theme.value(
                "height",
                theme.format_height_at(block.summary.height, Some(block.summary.time)),
                theme.focused(),
            ),
            Span::styled("   Timestamp: ", theme.muted()),
            theme.value(
                "timestamp",
                theme.format_time(block.summary.time),
                theme.base(),
            ),
        ]),
        Line::from(vec![
            Span::styled("Hash: ", theme.muted()),
            theme.value("block hash", block.summary.hash.clone(), theme.link()),
        ]),
        Line::from(vec![
            Span::styled("Type: ", theme.muted()),
            theme.value("block type", production, theme.base()),
            Span::styled("   Size: ", theme.muted()),
            theme.value("block size", size, theme.base()),
            Span::styled("   Transactions: ", theme.muted()),
            theme.value(
                "transaction count",
                block.summary.tx_count().to_string(),
                theme.base(),
            ),
        ]),
        Line::from(vec![
            Span::styled("Prev/Next: ", theme.muted()),
            theme.value(
                "adjacent blocks",
                adjacent_block_summary(state, block),
                theme.base(),
            ),
            Span::styled("   Detail: ", theme.muted()),
            Span::raw(if block.detail_loaded {
                "full tx cached"
            } else {
                "loading full tx"
            }),
        ]),
        Line::from(block_tags),
    ])
    .style(theme.base())
    .wrap(Wrap { trim: true });
    let header_width = detail_header_content(area).width.max(1);
    let block_header_height = u16::try_from(header.line_count(header_width)).unwrap_or(u16::MAX);
    let block_layout =
        Layout::vertical([Constraint::Length(block_header_height), Constraint::Min(4)])
            .spacing(1)
            .split(area);
    frame.render_widget(header, detail_header_content(block_layout[0]));

    render_transaction_tiles(
        frame,
        config,
        theme,
        &state.store,
        block,
        selected_tx,
        block_layout[1],
    );
}

fn adjacent_block_summary(state: &AppState, block: &BlockRecord) -> String {
    let previous = block
        .summary
        .height
        .checked_sub(1)
        .map(|height| format!("p: {height}"))
        .unwrap_or_else(|| "p: none".to_string());
    let next = if state
        .tip
        .height
        .is_none_or(|tip| block.summary.height < tip)
    {
        format!("n: {}", block.summary.height + 1)
    } else {
        "n: tip".to_string()
    };

    format!("{previous}   {next}")
}

fn render_transaction_tiles(
    frame: &mut Frame<'_>,
    config: &Config,
    theme: &Theme,
    store: &ObjectStore,
    block: &BlockRecord,
    selected_tx: usize,
    area: Rect,
) {
    let tx_count = block.summary.tx.len();
    let content = scroll_view::content_area(area, tx_count > 1);
    let currency = native_currency_name(config, store);
    // Build only a bounded neighborhood around selection; token-rich rows may
    // wrap, but a large block must not rebuild every transaction each frame.
    let radius = usize::from(area.height).saturating_add(1);
    let selected_tx = selected_tx.min(tx_count.saturating_sub(1));
    let base = selected_tx.saturating_sub(radius);
    let end = selected_tx
        .saturating_add(radius)
        .saturating_add(1)
        .min(tx_count);
    let tiles: Vec<_> = block
        .summary
        .tx
        .iter()
        .enumerate()
        .skip(base)
        .take(end - base)
        .map(|(index, tx)| {
            transaction_summary_tile(
                theme,
                TransactionTileContext {
                    currency: &currency,
                    store,
                    block_time: block.summary.time,
                },
                tx,
                selected_tx == index,
                content.width,
            )
        })
        .collect();
    let heights: Vec<_> = tiles
        .iter()
        .map(|tile| wrapped_tile_height(&tile.lines, tile_content_area(content).width))
        .collect();
    let (first, last) = variable_height_visible_range(
        &heights,
        selected_tx.saturating_sub(base),
        area.height.saturating_sub(SECTION_HEADING_HEIGHT),
    );
    let range_label = if tx_count == 0 {
        "0 of 0".to_string()
    } else {
        format!("{}-{} of {}", base + first + 1, base + last, tx_count)
    };
    let heading = Paragraph::new(Line::from(vec![
        Span::styled("Transactions ", theme.focused()),
        Span::styled(range_label, theme.muted()),
    ]))
    .style(theme.base());
    let inner = Rect {
        y: area.y.saturating_add(SECTION_HEADING_HEIGHT),
        height: area.height.saturating_sub(SECTION_HEADING_HEIGHT),
        ..area
    };
    frame.render_widget(heading, Rect { height: 1, ..area });

    if tx_count == 0 {
        frame.render_widget(
            Paragraph::new("No transactions.").style(theme.muted()),
            inner,
        );
        return;
    }

    let rows = Layout::vertical(heights[first..last].iter().copied().map(Constraint::Length))
        .split(Rect {
            width: content.width,
            ..inner
        });
    for (row, tile) in rows
        .iter()
        .zip(tiles.into_iter().skip(first).take(last - first))
    {
        render_summary_tile(frame, theme, tile, *row);
    }
    scroll_view::render(frame, theme, inner, tx_count, base + first, last - first);
}

fn transaction_summary_tile(
    theme: &Theme,
    context: TransactionTileContext<'_>,
    tx: &TransactionSummary,
    selected: bool,
    width: u16,
) -> SummaryTile {
    let tags = display_transaction_tags(tx, context.store);
    let content_width = tile_content_width(width);
    let mut tag_spans = if tx.raw.is_none() {
        vec![Span::styled(
            format!("{} details uncached", theme.glyph("○", "o")),
            theme.muted(),
        )]
    } else {
        transaction_tag_spans(theme, &tags)
    };
    if context.store.chain.in_mempool(&tx.txid) {
        if !tag_spans.is_empty() {
            tag_spans.push(Span::raw(" "));
        }
        tag_spans.push(badge_span(theme, StatusBadge::Mempool));
    }
    let value_spans = transaction_tile_value_spans(tx, context.store, context.currency, theme);
    SummaryTile {
        title: None,
        selected,
        lines: vec![
            justified_line(
                vec![
                    Span::raw("txid "),
                    theme.value_as(
                        "transaction ID",
                        short_id(&tx.txid),
                        tx.txid.as_str(),
                        theme.link(),
                    ),
                ],
                vec![
                    Span::styled("time ", theme.muted()),
                    theme.value(
                        "timestamp",
                        theme.format_time(context.block_time),
                        Style::default(),
                    ),
                ],
                content_width,
            ),
            justified_line(
                value_spans,
                vec![theme.value("size", transaction_size_label(tx), theme.muted())],
                content_width,
            ),
            Line::from(tag_spans),
        ],
        alignment: Alignment::Left,
    }
}

fn transaction_tag_spans(theme: &Theme, tags: &[String]) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    for (index, tag) in tags.iter().enumerate() {
        if index > 0 {
            spans.push(Span::raw(" "));
        }
        spans.push(Span::styled(
            format!(" {tag} "),
            Style::default()
                .fg(theme.background)
                .bg(theme.warning)
                .add_modifier(Modifier::BOLD),
        ));
    }
    spans
}

fn transaction_tags(tx: &TransactionSummary, store: &ObjectStore) -> Vec<TransactionTag> {
    let mut tags = if tx.raw.is_some() {
        store
            .transaction(&tx.txid)
            .and_then(|record| record.prepared.as_deref())
            .map_or_else(|| tx.tags(), |data| data.tags.clone())
    } else {
        Vec::new()
    };
    // Keep DeFi badges after the other protocol badges on transaction summaries.
    tags.sort_by_key(|tag| {
        matches!(
            tag,
            TransactionTag::Output(OutputTag {
                kind: OutputTagKind::CurrencyDefinition
                    | OutputTagKind::ReserveTransfer
                    | OutputTagKind::AutoArbitrage
                    | OutputTagKind::Conversion
                    | OutputTagKind::CrossChainTransfer
                    | OutputTagKind::Export
                    | OutputTagKind::Import,
                ..
            })
        )
    });
    if let Some(raw) = &tx.raw {
        if !consumed_token_burns(raw, store).is_empty() {
            tags.push(TransactionTag::ConsumptionBurn);
        }
        if stakeguard_enforcement(raw, store) {
            tags.push(TransactionTag::StakeGuardEnforcement);
        }
        if transaction_has_import_output(raw)
            && !import_auto_arbitrage_input_indexes(raw, store).is_empty()
        {
            tags.push(TransactionTag::AutoArbitrageImport);
        }
    }
    tags.extend(
        store
            .marketplace_tags(&tx.txid)
            .map(TransactionTag::Marketplace),
    );
    tags
}

fn transaction_tag_labels(
    tags: impl IntoIterator<Item = TransactionTag>,
    store: &ObjectStore,
) -> Vec<String> {
    let mut labels = Vec::new();
    for tag in tags {
        let label = match &tag {
            TransactionTag::Output(output) => display_output_tag(output, store),
            _ => tag.label().to_string(),
        };
        if !labels.contains(&label) {
            labels.push(label);
        }
    }
    labels
}

fn display_transaction_tags(tx: &TransactionSummary, store: &ObjectStore) -> Vec<String> {
    transaction_tag_labels(transaction_tags(tx, store), store)
}

fn display_output_tag(tag: &OutputTag, store: &ObjectStore) -> String {
    let include_currency = matches!(
        tag.kind,
        OutputTagKind::CurrencyDefinition
            | OutputTagKind::AutoArbitrage
            | OutputTagKind::Conversion
            | OutputTagKind::Export
            | OutputTagKind::Import
    );
    if !include_currency {
        return tag.kind.label().to_string();
    }
    let Some(currency_id) = tag.currency_id.as_deref() else {
        return tag.kind.label().to_string();
    };
    let currency = store
        .currency_name(currency_id)
        .map(str::to_string)
        .unwrap_or_else(|| short_id(currency_id));
    format!("{}: {currency}", tag.kind.label())
}

fn is_auto_arbitrage_output(output: &Value) -> bool {
    output
        .pointer("/scriptPubKey/reservetransfer/arbitrageonly")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn transaction_has_import_output(raw: &Value) -> bool {
    raw.get("vout")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .any(|output| output.pointer("/scriptPubKey/crosschainimport").is_some())
}

fn import_auto_arbitrage_input_indexes(raw: &Value, store: &ObjectStore) -> HashSet<usize> {
    let mut indexes = HashSet::new();
    let mut found_prior_import = false;
    for (index, input) in raw
        .get("vin")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
    {
        let Some(prevout) = resolved_prevout(input, store) else {
            continue;
        };
        if prevout.pointer("/scriptPubKey/crosschainimport").is_some() {
            found_prior_import = true;
            continue;
        }
        if found_prior_import
            && prevout
                .pointer("/scriptPubKey/acceptednotarization")
                .is_some()
        {
            break;
        }
        if found_prior_import && is_auto_arbitrage_output(prevout) {
            indexes.insert(index);
        }
    }
    indexes
}

fn block_tag_spans(theme: &Theme, block: &BlockRecord, store: &ObjectStore) -> Vec<Span<'static>> {
    let tags = block.summary.tx.iter().flat_map(|summary| {
        let summary = store
            .transaction(&summary.txid)
            .map(|record| &record.summary)
            .unwrap_or(summary);
        transaction_tags(summary, store)
            .into_iter()
            .filter(|tag| !matches!(tag, TransactionTag::Coinbase | TransactionTag::Stake))
    });
    transaction_tag_spans(theme, &transaction_tag_labels(tags, store))
}

fn transaction_tile_value_spans(
    tx: &TransactionSummary,
    store: &ObjectStore,
    currency: &str,
    theme: &Theme,
) -> Vec<Span<'static>> {
    let Some(raw) = tx.raw.as_ref() else {
        return vec![Span::styled("Inputs/outputs loading", theme.muted())];
    };
    let flows = transaction_flows(raw, store);
    let (values, _) = transaction_values_labels(raw, &flows, currency, store, Some(theme));
    values
}

fn tile_paragraph<'a>(theme: &Theme, selected: bool, mut lines: Vec<Line<'a>>) -> Paragraph<'a> {
    if selected {
        for line in &mut lines {
            // Ordinary text inherits the selected surface. Badges and link cursors keep their colors.
            if line.style.bg == Some(theme.background) {
                line.style.bg = None;
            }
            for span in &mut line.spans {
                if span.style.bg == Some(theme.background) {
                    span.style.bg = None;
                }
            }
        }
    }
    Paragraph::new(lines).style(theme.tile_body(selected))
}

fn render_summary_tile(frame: &mut Frame<'_>, theme: &Theme, tile: SummaryTile, area: Rect) {
    let area = render_tile_gutter(frame, theme, tile.selected, area);
    let mut lines = Vec::new();
    if let Some(title) = tile.title {
        lines.push(title);
    }
    lines.extend(tile.lines);
    let paragraph = tile_paragraph(theme, tile.selected, lines)
        .alignment(tile.alignment)
        .wrap(Wrap { trim: true });
    frame.render_widget(paragraph, area);
}

fn render_tile_gutter(frame: &mut Frame<'_>, theme: &Theme, selected: bool, area: Rect) -> Rect {
    frame.render_widget(Paragraph::new("").style(theme.tile_gutter()), area);
    let content = tile_content_area(area);
    if content.x > area.x {
        frame.render_widget(
            Block::default().style(theme.tile_body(selected)),
            Rect::new(area.x + 1, area.y, area.width - 1, content.height),
        );
        if selected {
            render_selection_gutter(frame, theme, Rect::new(area.x, area.y, 1, content.height));
        }
    }
    content
}

fn render_selection_gutter(frame: &mut Frame<'_>, theme: &Theme, area: Rect) {
    for y in area.y..area.bottom() {
        if let Some(cell) = frame.buffer_mut().cell_mut((area.x, y)) {
            cell.set_symbol(if y == area.y { SELECTION_MARKER } else { "│" })
                .set_style(theme.selection_cursor());
        }
    }
}

fn selection_line(theme: &Theme, selected: bool, label: impl Into<Span<'static>>) -> Line<'static> {
    Line::from(vec![
        theme.selection_marker(selected),
        Span::raw(" "),
        label.into(),
    ])
}

fn justified_line(
    left: Vec<Span<'static>>,
    right: Vec<Span<'static>>,
    width: u16,
) -> Line<'static> {
    let left_width = spans_width(&left);
    let right_width = spans_width(&right);
    let mut spans = left;

    if left_width + right_width + 2 <= usize::from(width) {
        spans.push(Span::raw(
            " ".repeat(usize::from(width) - left_width - right_width),
        ));
    } else {
        spans.push(Span::raw(" | "));
    }

    spans.extend(right);
    Line::from(spans)
}

fn spans_width(spans: &[Span<'_>]) -> usize {
    spans.iter().map(|span| span.content.chars().count()).sum()
}

fn wrapped_spans_height(spans: &[Span<'_>], width: u16) -> u16 {
    let width = usize::from(width.max(1));
    u16::try_from(spans_width(spans).div_ceil(width).max(1)).unwrap_or(u16::MAX)
}

fn tile_content_width(width: u16) -> u16 {
    match width {
        0..=2 => width,
        3 => 1,
        _ => width - 3,
    }
}

fn tile_content_area(area: Rect) -> Rect {
    if area.width <= 2 || area.height == 0 {
        return area;
    }

    Rect {
        x: area.x + 2,
        y: area.y,
        width: tile_content_width(area.width),
        height: area.height - u16::from(area.height > 2),
    }
}

fn transaction_size_label(tx: &TransactionSummary) -> String {
    tx.raw
        .as_ref()
        .and_then(|raw| {
            json_u64(raw, "size")
                .or_else(|| json_u64(raw, "vsize"))
                .or_else(|| transaction_hex_size(raw))
        })
        .map(|size| format!("{size} bytes"))
        .unwrap_or_else(|| "unknown".to_string())
}

fn json_u64(value: &Value, key: &str) -> Option<u64> {
    value.get(key).and_then(Value::as_u64)
}

fn transaction_hex_size(value: &Value) -> Option<u64> {
    let hex = value.get("hex")?.as_str()?;
    u64::try_from(hex.len() / 2).ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionFlowKind {
    Input,
    Output,
}

#[derive(Debug, Clone)]
struct TransactionFlow {
    kind: TransactionFlowKind,
    index: usize,
    addresses: Vec<String>,
    value_zat: Option<i128>,
    value_label: String,
    reserve_amounts: BTreeMap<String, i128>,
    source_txid: Option<String>,
    source_vout: Option<u64>,
    sequence: Option<u64>,
    script_type: Option<String>,
    spendable: Option<bool>,
    coinbase: bool,
    private: bool,
    tags: Vec<OutputTag>,
    annotations: Vec<String>,
    references: crate::protocol::References,
    routing: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FlowLink {
    None,
    SourceTransaction,
    Address(usize),
    Tag(usize),
    Protocol(usize),
}

#[derive(Debug, Clone)]
enum FlowNavigation {
    Handled,
    Block {
        system: String,
        hash: String,
    },
    Currency(String),
    Outpoint(crate::world::OutpointRef),
    Destination {
        system: Option<crate::world::SystemId>,
        address: String,
    },
    Trail(u32),
}

fn tag_target(flow: &TransactionFlow, tag: &OutputTag) -> Option<FlowNavigation> {
    if matches!(tag.kind, OutputTagKind::ExportFinalization(_)) {
        return flow
            .references
            .links
            .iter()
            .find(|link| link.label == "Export referenced by finalization")
            .and_then(|link| match &link.target {
                crate::protocol::Target::Outpoint(target) => {
                    Some(FlowNavigation::Outpoint(target.clone()))
                }
                _ => None,
            });
    }
    if let Some(id) = tag
        .currency_id
        .as_ref()
        .filter(|id| looks_like_identity_address(id))
    {
        return Some(FlowNavigation::Currency(id.clone()));
    }
    if matches!(
        tag.kind,
        OutputTagKind::ReserveTransfer | OutputTagKind::CrossChainTransfer | OutputTagKind::Burn
    ) && flow.kind == TransactionFlowKind::Output
        && flow
            .tags
            .iter()
            .any(|tag| tag.kind == OutputTagKind::ReserveTransfer)
    {
        return u32::try_from(flow.index).ok().map(FlowNavigation::Trail);
    }
    if matches!(
        tag.kind,
        OutputTagKind::NotaryEvidence | OutputTagKind::NotarizationFinalization(_)
    ) && let Some(crate::protocol::Link {
        target: crate::protocol::Target::Outpoint(target),
        ..
    }) = flow
        .references
        .links
        .iter()
        .find(|link| matches!(link.target, crate::protocol::Target::Outpoint(_)))
    {
        return Some(FlowNavigation::Outpoint(target.clone()));
    }
    None
}

fn output_selection(flows: &[TransactionFlow], vout: u32) -> Option<usize> {
    transaction_flow_selections(flows)
        .iter()
        .position(|(index, _)| {
            flows[*index].kind == TransactionFlowKind::Output
                && flows[*index].index == vout as usize
        })
}

fn transaction_flow_selections(flows: &[TransactionFlow]) -> Vec<(usize, FlowLink)> {
    let mut selections = Vec::new();
    for (index, flow) in flows.iter().enumerate() {
        let first = selections.len();
        if flow.source_txid.is_some() {
            selections.push((index, FlowLink::SourceTransaction));
        }
        for address in 0..flow.addresses.len() {
            selections.push((index, FlowLink::Address(address)));
        }
        for (tag_index, tag) in flow.tags.iter().enumerate() {
            if tag_target(flow, tag).is_some() {
                selections.push((index, FlowLink::Tag(tag_index)));
            }
        }
        for reference in 0..flow.references.links.len() {
            selections.push((index, FlowLink::Protocol(reference)));
        }
        if selections.len() == first {
            selections.push((index, FlowLink::None));
        }
    }
    selections
}

fn transaction_flows(raw: &Value, store: &ObjectStore) -> Vec<TransactionFlow> {
    let mut flows = Vec::new();
    let registration = registration_payments::report(raw, store);
    let auto_arbitrage_inputs = import_auto_arbitrage_input_indexes(raw, store);
    let market = marketplace_trace::FlowEvidence::new(raw, store);
    if let Some(inputs) = raw.get("vin").and_then(Value::as_array) {
        flows.extend(inputs.iter().enumerate().map(|(index, input)| {
            let prevout = input
                .get("prevout")
                .or_else(|| resolved_prevout(input, store));
            let addresses = {
                let direct = crate::protocol::raw_addresses(input);
                if direct.is_empty() {
                    prevout
                        .map(crate::protocol::raw_addresses)
                        .unwrap_or_default()
                } else {
                    direct
                }
            };
            let value_source = if native_value_zat(input).is_some() || input.get("value").is_some()
            {
                input
            } else {
                prevout.unwrap_or(input)
            };
            TransactionFlow {
                references: market.input_links(index),
                routing: BTreeMap::new(),
                kind: TransactionFlowKind::Input,
                index,
                addresses,
                value_zat: native_value_zat(value_source),
                value_label: native_value_label(value_source),
                reserve_amounts: monetary_reserve_amounts(
                    input.get("prevout").or(prevout).unwrap_or(input),
                    store,
                ),
                source_txid: input
                    .get("txid")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                source_vout: input.get("vout").and_then(Value::as_u64),
                sequence: input.get("sequence").and_then(Value::as_u64),
                script_type: input
                    .pointer("/prevout/scriptPubKey/type")
                    .or_else(|| prevout.and_then(|output| output.pointer("/scriptPubKey/type")))
                    .or_else(|| input.get("type"))
                    .and_then(Value::as_str)
                    .map(str::to_string),
                spendable: None,
                coinbase: input.get("coinbase").is_some(),
                private: false,
                tags: Vec::new(),
                annotations: {
                    let mut annotations = market
                        .input_annotation(index)
                        .map(str::to_owned)
                        .into_iter()
                        .collect::<Vec<_>>();
                    if auto_arbitrage_inputs.contains(&index) {
                        annotations.push("Auto-Arbitrage Input".to_string());
                    }
                    annotations
                },
            }
        }));
    }
    let transparent_inputs = flows.len();
    let sapling_input_count = case_insensitive_array(raw, "vshieldedspend").map_or(0, Vec::len);
    let sapling_output_count = case_insensitive_array(raw, "vshieldedoutput").map_or(0, Vec::len);
    let shielded_accounting = ShieldedAccounting::from_raw(raw);
    let derived_sapling_input = (sapling_input_count == 1 && sapling_output_count == 0)
        .then_some(shielded_accounting.sapling_value_balance_zat)
        .filter(|value| shielded_accounting.sapling_complete && *value >= 0);
    flows.extend((0..sapling_input_count).map(|offset| {
        TransactionFlow {
            references: crate::protocol::References::default(),
            routing: BTreeMap::new(),
            kind: TransactionFlowKind::Input,
            index: transparent_inputs + offset,
            addresses: Vec::new(),
            reserve_amounts: BTreeMap::new(),
            value_zat: derived_sapling_input,
            value_label: derived_sapling_input
                .map(format_native_zat)
                .unwrap_or_else(|| "hidden".to_string()),
            source_txid: None,
            source_vout: None,
            sequence: None,
            script_type: Some("Sapling".to_string()),
            spendable: None,
            coinbase: false,
            private: true,
            tags: Vec::new(),
            annotations: derived_sapling_input
                .map(|_| vec!["Derived from Sapling valueBalance".to_string()])
                .unwrap_or_default(),
        }
    }));
    let sprout_input_count = private_input_count(raw).saturating_sub(sapling_input_count);
    flows.extend((0..sprout_input_count).map(|offset| TransactionFlow {
        references: crate::protocol::References::default(),
        routing: BTreeMap::new(),
        kind: TransactionFlowKind::Input,
        index: transparent_inputs + sapling_input_count + offset,
        addresses: Vec::new(),
        reserve_amounts: BTreeMap::new(),
        value_zat: None,
        value_label: "hidden".to_string(),
        source_txid: None,
        source_vout: None,
        sequence: None,
        script_type: Some("Sprout".to_string()),
        spendable: None,
        coinbase: false,
        private: true,
        tags: Vec::new(),
        annotations: Vec::new(),
    }));
    let observing = store.workspace_system.clone().or_else(|| {
        store
            .chain
            .chain_id
            .as_ref()
            .and_then(|id| crate::world::SystemId::try_from(id.clone()).ok())
    });
    let mut outpoint_links = BTreeMap::<u32, Vec<crate::relationships::Link>>::new();
    if let Some(system) = observing {
        for link in crate::relationships::transaction_links(&system, raw) {
            outpoint_links
                .entry(link.carrier_vout)
                .or_default()
                .push(link);
        }
    }
    let emission_timelock = emission_timelock(raw);
    let stake = crate::domain::is_stake_transaction(raw);
    let enforcement = stakeguard_enforcement(raw, store);
    if let Some(outputs) = raw.get("vout").and_then(Value::as_array) {
        flows.extend(outputs.iter().enumerate().map(|(index, output)| {
            let script = output.get("scriptPubKey").unwrap_or(&Value::Null);
            let mut tags = output_tags(output);
            let addresses = crate::protocol::raw_addresses(output);
            let routing = addresses
                .iter()
                .filter_map(|address| {
                    crate::protocol::routing_label(script, address)
                        .map(|label| (address.clone(), label))
                })
                .collect();
            let mut references = crate::protocol::references(output);
            for link in outpoint_links
                .get(&(index as u32))
                .into_iter()
                .flatten()
                .take(crate::protocol::MAX_LINKS.saturating_sub(references.links.len()))
            {
                references.links.push(crate::protocol::Link {
                    label: link.label.clone(),
                    target: crate::protocol::Target::Outpoint(link.target.clone()),
                });
            }

            let mut annotations = Vec::new();
            market.output_evidence(index, &mut references, &mut annotations);
            annotations.extend(
                registration
                    .and_then(|report| report.annotation(index))
                    .map(str::to_owned),
            );
            if let Some(legacy) = crate::domain::legacy_notarization(output) {
                references.links.push(crate::protocol::Link {
                    label: "Notarized VRSC block".into(),
                    target: crate::protocol::Target::LegacyNotarizedBlock {
                        hash: legacy.block_hash,
                        height: legacy.height,
                    },
                });
                annotations.push(format!(
                    "Destination transaction {}",
                    legacy.destination_txid
                ));
                annotations.push(format!("Merkle-of-Merkle-roots depth {}", legacy.depth));
                annotations.push(format!("Merkle-of-Merkle-roots {}", legacy.merkle_root));
            } else {
                let known_data = (stake && index == 1)
                    || (enforcement && index + 1 == outputs.len())
                    || emission_timelock.is_some_and(|lock| index == lock.metadata_output_index)
                    || market.is_publication_output(index);
                if !known_data && let Some(data) = crate::domain::arbitrary_op_return(output) {
                    references.links.push(crate::protocol::Link {
                        label: "View OP_RETURN bytes".into(),
                        target: crate::protocol::Target::OpReturn(data),
                    });
                }
            }
            if let Some(timelock) = emission_timelock
                && index == timelock.locked_output_index
            {
                tags.push(OutputTag {
                    kind: OutputTagKind::EmissionTimelock,
                    currency_id: None,
                });
                annotations.push(format!("Unlock height {}", timelock.unlock_height));
            }
            TransactionFlow {
                references,
                routing,
                kind: TransactionFlowKind::Output,
                index: output
                    .get("n")
                    .and_then(Value::as_u64)
                    .and_then(|index| usize::try_from(index).ok())
                    .unwrap_or(index),
                addresses,
                value_zat: native_value_zat(output),
                value_label: native_value_label(output),
                reserve_amounts: monetary_reserve_amounts(output, store),
                source_txid: None,
                source_vout: None,
                sequence: None,
                script_type: script
                    .get("type")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                spendable: script.get("spendableoutput").and_then(Value::as_bool),
                coinbase: false,
                private: false,
                tags,
                annotations,
            }
        }));
    }
    let transparent_outputs = raw
        .get("vout")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    let derived_sapling_output = (sapling_output_count == 1 && sapling_input_count == 0)
        .then(|| -shielded_accounting.sapling_value_balance_zat)
        .filter(|value| shielded_accounting.sapling_complete && *value >= 0);
    flows.extend((0..sapling_output_count).map(|offset| {
        TransactionFlow {
            references: crate::protocol::References::default(),
            routing: BTreeMap::new(),
            kind: TransactionFlowKind::Output,
            index: transparent_outputs + offset,
            addresses: Vec::new(),
            reserve_amounts: BTreeMap::new(),
            value_zat: derived_sapling_output,
            value_label: derived_sapling_output
                .map(format_native_zat)
                .unwrap_or_else(|| "hidden".to_string()),
            source_txid: None,
            source_vout: None,
            sequence: None,
            script_type: Some("Sapling".to_string()),
            spendable: None,
            coinbase: false,
            private: true,
            tags: Vec::new(),
            annotations: {
                let mut annotations = derived_sapling_output
                    .map(|_| vec!["Derived from Sapling valueBalance".to_string()])
                    .unwrap_or_default();
                annotations.extend(market.shielded_annotation().map(str::to_owned));
                annotations
            },
        }
    }));
    let sprout_output_count = private_output_count(raw).saturating_sub(sapling_output_count);
    flows.extend((0..sprout_output_count).map(|offset| TransactionFlow {
        references: crate::protocol::References::default(),
        routing: BTreeMap::new(),
        kind: TransactionFlowKind::Output,
        index: transparent_outputs + sapling_output_count + offset,
        addresses: Vec::new(),
        reserve_amounts: BTreeMap::new(),
        value_zat: None,
        value_label: "hidden".to_string(),
        source_txid: None,
        source_vout: None,
        sequence: None,
        script_type: Some("Sprout".to_string()),
        spendable: None,
        coinbase: false,
        private: true,
        tags: Vec::new(),
        annotations: Vec::new(),
    }));
    flows
}

/// Only value-bearing token payloads, never embedded import/export accounting.
fn monetary_reserve_amounts(output: &Value, store: &ObjectStore) -> BTreeMap<String, i128> {
    let mut amounts = BTreeMap::new();
    for kind in ["reserveoutput", "reservetransfer", "reservedeposit"] {
        let Some(payload) = output.pointer(&format!("/scriptPubKey/{kind}")) else {
            continue;
        };
        // Match CReserveTransfer::TotalCurrencyOut: minted principal has no
        // custody here, while both fee budgets are held in the fee currency.
        let minted = kind == "reservetransfer"
            && payload["flags"]
                .as_u64()
                .is_some_and(|flags| flags & 0x20 != 0);
        if !minted && let Some(values) = payload["currencyvalues"].as_object() {
            for (id, amount) in values {
                if let Some(amount) = decimal_value_to_zat(amount) {
                    *amounts.entry(id.clone()).or_default() += amount;
                }
            }
        }
        if kind == "reservetransfer" {
            let fee_currency = payload["feecurrencyid"]
                .as_str()
                .or(store.chain.chain_id.as_deref());
            if let Some(id) = fee_currency {
                if let Some(fees) = decimal_value_to_zat(&payload["fees"]) {
                    *amounts.entry(id.to_owned()).or_default() += fees;
                }
                let destination = &payload["destination"];
                if destination["type"]
                    .as_u64()
                    .is_some_and(|kind| kind & 128 != 0)
                    && destination["gateway"]
                        .as_str()
                        .is_some_and(|id| !id.is_empty())
                    && let Some(fees) = decimal_value_to_zat(&destination["fees"])
                {
                    *amounts.entry(id.to_owned()).or_default() += fees;
                }
            }
        }
    }
    if let Some(native) = store.chain.chain_id.as_deref() {
        amounts.remove(native);
    }
    amounts
}

/// A conservative proof for ordinary transparent token consumption. Protocol
/// operations can account for value elsewhere; incomplete prevouts prove nothing.
fn consumed_token_burns(raw: &Value, store: &ObjectStore) -> BTreeMap<String, i128> {
    let prove = || -> Option<BTreeMap<String, i128>> {
        if private_input_count(raw) > 0 || private_output_count(raw) > 0 {
            return None;
        }
        let mut totals = BTreeMap::<String, i128>::new();
        let inputs = raw.get("vin")?.as_array()?;
        let outputs = raw.get("vout")?.as_array()?;
        if inputs.is_empty() || outputs.is_empty() {
            return None;
        }
        let mut account = |output: &Value, sign: i128| -> Option<()> {
            let script = output.get("scriptPubKey")?.as_object()?;
            // CC payloads other than a plain reserve output require their own
            // protocol accounting. Reject invalid payload strings as well.
            if script.iter().any(|(key, value)| {
                (value.is_object() && !matches!(key.as_str(), "reserveoutput" | "reserve_balance"))
                    || matches!(
                        key.as_str(),
                        "reservetransfer"
                            | "reservedeposit"
                            | "crosschainimport"
                            | "crosschainexport"
                            | "currencydefinition"
                            | "identityprimary"
                            | "identityreservation"
                            | "identityadvancedreservation"
                    )
            }) {
                return None;
            }
            if script.get("type").and_then(Value::as_str) == Some("cryptocondition")
                && !script.get("reserveoutput").is_some_and(Value::is_object)
                && script.get("evalnone").and_then(Value::as_str) != Some("valid")
            {
                return None;
            }
            if let Some(reserve) = script.get("reserveoutput") {
                for (id, value) in reserve.get("currencyvalues")?.as_object()? {
                    let amount = decimal_value_to_zat(value)?;
                    if amount < 0 {
                        return None;
                    }
                    let entry = totals.entry(id.clone()).or_default();
                    *entry = entry.checked_add(amount.checked_mul(sign)?)?;
                }
            }
            Some(())
        };
        for input in inputs {
            if input.get("coinbase").is_some() {
                return None;
            }
            account(
                resolved_prevout(input, store).or_else(|| input.get("prevout"))?,
                1,
            )?;
        }
        for output in outputs {
            account(output, -1)?;
        }
        // Newly created tokens mean this is not ordinary token accounting.
        if totals.values().any(|value| *value < 0) {
            return None;
        }
        totals.retain(|id, amount| {
            *amount > 0 && store.chain.chain_id.as_deref() != Some(id.as_str())
        });
        Some(totals)
    };
    prove().unwrap_or_default()
}

fn resolved_prevout<'a>(input: &Value, store: &'a ObjectStore) -> Option<&'a Value> {
    let txid = input.get("txid")?.as_str()?;
    let output_index = input.get("vout")?.as_u64()?;
    let output_index = usize::try_from(output_index).ok()?;
    store
        .transaction(txid)?
        .raw_json
        .get("vout")?
        .as_array()?
        .get(output_index)
}

fn addresses_in(value: &Value) -> Vec<String> {
    let mut addresses = Vec::new();
    collect_addresses(value, &mut addresses);
    let mut seen = HashSet::new();
    addresses.retain(|address| seen.insert(address.clone()));
    addresses
}

fn collect_addresses(value: &Value, addresses: &mut Vec<String>) {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                if key.eq_ignore_ascii_case("address") {
                    if let Some(address) = child.as_str() {
                        addresses.push(address.to_string());
                    }
                } else if key.eq_ignore_ascii_case("addresses") {
                    if let Some(items) = child.as_array() {
                        addresses
                            .extend(items.iter().filter_map(Value::as_str).map(str::to_string));
                    }
                } else {
                    collect_addresses(child, addresses);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_addresses(item, addresses);
            }
        }
        _ => {}
    }
}

fn native_value_zat(value: &Value) -> Option<i128> {
    ["valueZat", "valueSat", "satoshis"]
        .iter()
        .find_map(|key| value.get(key).and_then(json_i128))
}

fn json_i128(value: &Value) -> Option<i128> {
    value
        .as_i64()
        .map(i128::from)
        .or_else(|| value.as_u64().map(i128::from))
}

fn native_value_label(value: &Value) -> String {
    if let Some(zat) = native_value_zat(value) {
        return format_native_zat(zat);
    }
    value
        .get("value")
        .and_then(Value::as_f64)
        .map(|amount| format!("{amount:.8}"))
        .unwrap_or_else(|| "value unavailable".to_string())
}

fn format_native_zat(zat: i128) -> String {
    let sign = if zat < 0 { "-" } else { "" };
    let magnitude = zat.unsigned_abs();
    format!(
        "{sign}{}.{:08}",
        magnitude / 100_000_000,
        magnitude % 100_000_000
    )
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ShieldedAccounting {
    sapling_present: bool,
    sapling_value_balance_zat: i128,
    sapling_complete: bool,
    sprout_present: bool,
    sprout_complete: bool,
    sprout_to_transparent_zat: i128,
    transparent_to_sprout_zat: i128,
    complete: bool,
}

impl ShieldedAccounting {
    fn from_raw(raw: &Value) -> Self {
        let sapling_fields_valid = ["vshieldedspend", "vshieldedoutput"]
            .iter()
            .all(|key| case_insensitive_field(raw, key).is_none_or(Value::is_array));
        let sprout_fields_valid =
            case_insensitive_field(raw, "vjoinsplit").is_none_or(Value::is_array);
        let sapling_present = !sapling_fields_valid
            || case_insensitive_array(raw, "vshieldedspend").is_some_and(|items| !items.is_empty())
            || case_insensitive_array(raw, "vshieldedoutput")
                .is_some_and(|items| !items.is_empty());
        let sapling_value = case_insensitive_field(raw, "valueBalanceZat")
            .map(json_i128)
            .or_else(|| case_insensitive_field(raw, "valueBalance").map(decimal_value_to_zat))
            .unwrap_or(if sapling_present { None } else { Some(0) })
            .filter(|value| (i128::from(i64::MIN)..=i128::from(i64::MAX)).contains(value));

        let joinsplits = case_insensitive_array(raw, "vjoinsplit");
        let sprout_present =
            !sprout_fields_valid || joinsplits.is_some_and(|items| !items.is_empty());
        let mut sprout_to_transparent_zat = 0_i128;
        let mut transparent_to_sprout_zat = 0_i128;
        let mut sprout_complete = sprout_fields_valid;
        for joinsplit in joinsplits.into_iter().flatten() {
            let vpub_new = shielded_amount_field(joinsplit, "vpub_newZat", "vpub_new");
            let vpub_old = shielded_amount_field(joinsplit, "vpub_oldZat", "vpub_old");
            match (vpub_new, vpub_old) {
                (Some(vpub_new), Some(vpub_old)) if vpub_new >= 0 && vpub_old >= 0 => {
                    sprout_to_transparent_zat = sprout_to_transparent_zat
                        .checked_add(vpub_new)
                        .unwrap_or_else(|| {
                            sprout_complete = false;
                            0
                        });
                    transparent_to_sprout_zat = transparent_to_sprout_zat
                        .checked_add(vpub_old)
                        .unwrap_or_else(|| {
                            sprout_complete = false;
                            0
                        });
                }
                _ => sprout_complete = false,
            }
        }

        Self {
            sapling_present,
            sapling_value_balance_zat: sapling_value.unwrap_or_default(),
            sapling_complete: sapling_value.is_some() && sapling_fields_valid,
            sprout_present,
            sprout_complete,
            sprout_to_transparent_zat,
            transparent_to_sprout_zat,
            complete: sapling_value.is_some() && sapling_fields_valid && sprout_complete,
        }
    }

    fn present(self) -> bool {
        self.sapling_present || self.sprout_present
    }

    fn net_to_transparent_zat(self) -> Option<i128> {
        if !self.complete {
            return None;
        }
        self.sapling_value_balance_zat
            .checked_add(self.sprout_to_transparent_zat)?
            .checked_sub(self.transparent_to_sprout_zat)
    }
}

fn case_insensitive_field<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    value
        .as_object()?
        .iter()
        .find_map(|(candidate, value)| candidate.eq_ignore_ascii_case(key).then_some(value))
}

fn case_insensitive_array<'a>(value: &'a Value, key: &str) -> Option<&'a Vec<Value>> {
    case_insensitive_field(value, key).and_then(Value::as_array)
}

fn shielded_amount_field(value: &Value, zat_key: &str, decimal_key: &str) -> Option<i128> {
    case_insensitive_field(value, zat_key)
        .map(json_i128)
        .unwrap_or_else(|| {
            case_insensitive_field(value, decimal_key).and_then(decimal_value_to_zat)
        })
}

fn shielded_accounting_spans(
    accounting: ShieldedAccounting,
    currency: &str,
    theme: &Theme,
) -> Option<Vec<Span<'static>>> {
    accounting.present().then(|| {
        let mut spans = vec![Span::styled("Shielded: ", theme.muted())];
        let mut parts = Vec::new();
        if accounting.sapling_present && !accounting.sapling_complete {
            parts.push("Sapling public balance unavailable".to_string());
        } else if accounting.sapling_present {
            parts.push(pool_transfer_label(
                "Sapling",
                accounting.sapling_value_balance_zat,
                currency,
            ));
        }
        if accounting.sprout_present && !accounting.sprout_complete {
            parts.push("Sprout public balance unavailable".to_string());
        } else if accounting.sprout_present {
            if accounting.transparent_to_sprout_zat > 0 {
                parts.push(format!(
                    "Sprout {} {currency} transparent -> shielded",
                    format_native_zat(accounting.transparent_to_sprout_zat)
                ));
            }
            if accounting.sprout_to_transparent_zat > 0 {
                parts.push(format!(
                    "Sprout {} {currency} shielded -> transparent",
                    format_native_zat(accounting.sprout_to_transparent_zat)
                ));
            }
            if accounting.transparent_to_sprout_zat == 0
                && accounting.sprout_to_transparent_zat == 0
            {
                parts.push("Sprout public net 0 (individual values unknown)".to_string());
            }
        }
        if !accounting.complete {
            parts.push("accounting unavailable".to_string());
        }
        for (index, part) in parts.into_iter().enumerate() {
            if index > 0 {
                spans.push(Span::raw("   "));
            }
            spans.push(theme.value("shielded public balance", part, Style::default()));
        }
        spans
    })
}

fn pool_transfer_label(pool: &str, net_to_transparent_zat: i128, currency: &str) -> String {
    match net_to_transparent_zat.cmp(&0) {
        std::cmp::Ordering::Greater => format!(
            "{pool} {} {currency} shielded -> transparent",
            format_native_zat(net_to_transparent_zat)
        ),
        std::cmp::Ordering::Less => format!(
            "{pool} {} {currency} transparent -> shielded",
            format_native_zat(-net_to_transparent_zat)
        ),
        std::cmp::Ordering::Equal => {
            format!("{pool} public net 0 (individual values unknown)")
        }
    }
}

fn render_transaction_view(
    frame: &mut Frame<'_>,
    config: &Config,
    theme: &Theme,
    state: &AppState,
    txid: &str,
    selected_flow: usize,
    area: Rect,
) {
    let Some(transaction) = state.store.transaction(txid) else {
        render_missing_object(frame, theme, "Transaction", detail_header_content(area));
        return;
    };
    let raw = &transaction.raw_json;
    let flows = transaction_flows(raw, &state.store);
    let observed_confirmations = transaction
        .block_hash
        .as_deref()
        .and_then(|hash| state.store.chain.confirmations(hash));
    let confirmations = observed_confirmations.map(i128::from).unwrap_or_else(|| {
        if state.store.chain.height.is_none() && state.store.chain.reconciled.is_none() {
            raw.get("confirmations")
                .and_then(json_i128)
                .unwrap_or_default()
        } else {
            0
        }
    });
    let status = if state.store.chain.in_mempool(txid) {
        "unconfirmed".into()
    } else if confirmations > 0 {
        if theme.heights.borrow().dates {
            transaction
                .block_height
                .map(|h| {
                    format!(
                        "confirmed since {}",
                        theme.format_height_at(h, Some(transaction.block_time))
                    )
                })
                .unwrap_or_else(|| format!("confirmed ({confirmations})"))
        } else {
            format!("confirmed ({confirmations})")
        }
    } else if transaction
        .block_hash
        .as_deref()
        .is_some_and(|hash| state.store.chain.block_badge(hash) == Some(StatusBadge::Orphaned))
    {
        "orphaned inclusion; current mempool membership unverified".into()
    } else if transaction.block_hash.is_some() {
        "confirmations unverified".into()
    } else {
        "unconfirmed".into()
    };
    let metadata = transaction_metadata_label(raw, &transaction.summary, theme);
    let currency = native_currency_name(config, &state.store);
    let accounting = ShieldedAccounting::from_raw(raw);
    let (values, fee) =
        transaction_values_labels(raw, &flows, &currency, &state.store, Some(theme));
    let shielded = shielded_accounting_spans(accounting, &currency, theme);
    let tags = display_transaction_tags(&transaction.summary, &state.store);
    let mut fee_and_tags = vec![
        Span::styled("Fee: ", theme.muted()),
        theme.value("fee", fee, theme.base()),
    ];
    if transaction
        .source_id
        .as_deref()
        .is_some_and(|source| source.starts_with("public-api:"))
    {
        fee_and_tags.push(Span::styled("  API", theme.muted()));
    }
    if state.store.chain.in_mempool(txid) {
        fee_and_tags.push(Span::raw("   "));
        fee_and_tags.push(badge_span(theme, StatusBadge::Mempool));
    }
    if !tags.is_empty() {
        fee_and_tags.push(Span::raw("   "));
        fee_and_tags.extend(transaction_tag_spans(theme, &tags));
    }
    let block_height = transaction
        .block_height
        .map(|h| theme.format_height_at(h, Some(transaction.block_time)))
        .unwrap_or_else(|| "unknown".into());
    let expiry = raw
        .get("expiryheight")
        .and_then(Value::as_u64)
        .filter(|h| *h > 0)
        .map(|h| theme.format_height(h))
        .unwrap_or_else(|| "none".into());
    let number_width = block_height.len().max(expiry.len());
    let header_width = detail_header_content(area).width.max(1);
    let block_label = "Block:  ";
    let hash_label = "   Hash: ";
    let mut header_lines = vec![
        Line::from(vec![Span::styled("Transaction Overview", theme.focused())]),
        Line::from(vec![
            Span::styled("Txid: ", theme.muted()),
            theme.value(
                "transaction ID",
                transaction.summary.txid.clone(),
                theme.link(),
            ),
        ]),
        Line::from(vec![
            Span::styled("Status: ", theme.muted()),
            theme.value(
                "status",
                status,
                if confirmations > 0 {
                    theme.positive()
                } else {
                    theme.negative()
                },
            ),
            Span::styled("   Block time: ", theme.muted()),
            theme.value(
                "block time",
                if transaction.block_hash.is_some() && transaction.block_time > 0 {
                    theme.format_time(transaction.block_time)
                } else {
                    "not yet confirmed".into()
                },
                theme.base(),
            ),
        ]),
        Line::from(vec![
            Span::styled(block_label, theme.muted()),
            theme.value_as(
                "block height",
                format!("{block_height:>number_width$}"),
                block_height.clone(),
                theme.focused(),
            ),
            Span::styled(hash_label, theme.muted()),
            theme.value_as(
                "block hash",
                transaction
                    .block_hash
                    .as_deref()
                    .map(|hash| {
                        if block_label.len() + number_width + hash_label.len() + hash.len()
                            <= usize::from(header_width)
                        {
                            hash.to_string()
                        } else {
                            short_id(hash)
                        }
                    })
                    .unwrap_or_else(|| "unconfirmed".to_string()),
                transaction.block_hash.as_deref().unwrap_or("unconfirmed"),
                theme.link(),
            ),
        ]),
        Line::from(vec![
            Span::styled("Expiry: ", theme.muted()),
            theme.value_as(
                "expiry",
                format!("{expiry:>number_width$}"),
                expiry,
                theme.base(),
            ),
        ]),
        Line::from(metadata),
        Line::from(values),
    ];
    if let Screen::Transaction {
        notarization: Some(context),
        ..
    } = &state.screen
    {
        header_lines.insert(2, Line::from(context.status_line()));
    }
    if let Some(shielded) = shielded {
        header_lines.push(Line::from(shielded));
    }
    for detail in identity_evidence::transaction_registration_lines(raw, &state.store) {
        header_lines.push(Line::from(vec![
            Span::styled("ID: ", theme.muted()),
            Span::raw(detail),
        ]));
    }
    header_lines.extend(registration_payments::lines(raw, &state.store, theme));
    if raw["vout"].as_array().is_some_and(|outputs| {
        outputs
            .iter()
            .any(|output| output["scriptPubKey"]["feepool"].is_object())
    }) {
        header_lines.push(Line::from(vec![Span::styled(
            "Fee pool: unspendable protocol checkpoint; F8 compares adjacent coinbase states",
            theme.muted(),
        )]));
    }
    header_lines.push(Line::from(fee_and_tags));
    let header = Paragraph::new(header_lines)
        .style(theme.base())
        .wrap(Wrap { trim: true });
    let header_height = u16::try_from(header.line_count(header_width)).unwrap_or(u16::MAX);
    let layout = Layout::vertical([Constraint::Length(header_height), Constraint::Min(1)])
        .spacing(1)
        .split(area);
    frame.render_widget(header, detail_header_content(layout[0]));
    render_transaction_flows(
        frame,
        TransactionFlowRenderContext {
            transaction,
            theme,
            store: &state.store,
            currency: &currency,
            unmask_identity_addresses: state.unmask_identity_addresses,
        },
        &flows,
        selected_flow,
        layout[1],
    );
}

fn native_currency_name(config: &Config, store: &ObjectStore) -> String {
    if let Some(name) = store.chain.chain_name.as_deref().or_else(|| {
        config
            .chain
            .system_id
            .get()
            .and_then(|id| store.currency_name(id.as_ref()))
    }) {
        return name.to_string();
    }
    if config.chain.name.eq_ignore_ascii_case("VRSC") {
        "VRSC".into()
    } else {
        config.chain.name.clone()
    }
}

fn transaction_metadata_label(
    raw: &Value,
    summary: &TransactionSummary,
    theme: &Theme,
) -> Vec<Span<'static>> {
    let size = transaction_size_label(summary);
    let version = raw
        .get("version")
        .map(Value::to_string)
        .unwrap_or_else(|| "?".to_string());
    let locktime = raw
        .get("locktime")
        .map(|v| match v.as_u64() {
            Some(h) if h > 0 && h < 500_000_000 => theme.format_height(h),
            Some(t) if t >= 500_000_000 => theme.format_time(t),
            _ => v.to_string(),
        })
        .unwrap_or_else(|| "?".to_string());
    vec![
        Span::raw("Size "),
        theme.value("size", size, theme.base()),
        Span::raw("   Version "),
        theme.value("version", version, theme.base()),
        Span::raw("   Locktime "),
        theme.value("locktime", locktime, theme.base()),
    ]
}

// These operations can introduce implicit value or converted reserves. Decoded
// transparent inputs/outputs do not expose the consensus descriptor's NativeFees.
fn protocol_value_accounting(raw: &Value, store: &ObjectStore) -> bool {
    let protocol_output = |output: &Value| {
        output["scriptPubKey"].as_object().is_some_and(|script| {
            [
                "crosschainimport",
                "crosschainexport",
                "currencydefinition",
                "reservetransfer",
                "reservedeposit",
                "feepool",
            ]
            .iter()
            .any(|key| script.contains_key(*key))
        })
    };
    raw["vout"]
        .as_array()
        .is_some_and(|outputs| outputs.iter().any(protocol_output))
        || raw["vin"].as_array().is_some_and(|inputs| {
            inputs.iter().any(|input| {
                resolved_prevout(input, store)
                    .or_else(|| input.get("prevout"))
                    .is_some_and(protocol_output)
            })
        })
}

fn transaction_values_labels(
    raw: &Value,
    flows: &[TransactionFlow],
    currency: &str,
    store: &ObjectStore,
    theme: Option<&Theme>,
) -> (Vec<Span<'static>>, String) {
    let inputs = flows
        .iter()
        .filter(|flow| flow.kind == TransactionFlowKind::Input)
        .collect::<Vec<_>>();
    let outputs = flows
        .iter()
        .filter(|flow| flow.kind == TransactionFlowKind::Output)
        .collect::<Vec<_>>();
    let output_total = transaction_side_value_label(&outputs, currency, store);
    let coinbase = inputs.iter().any(|input| input.coinbase);
    let input_total = if coinbase {
        TransactionTag::Coinbase.label().to_owned()
    } else {
        transaction_side_value_label(&inputs, currency, store)
    };
    let transparent_inputs = inputs
        .iter()
        .copied()
        .filter(|flow| !flow.private)
        .collect::<Vec<_>>();
    let transparent_outputs = outputs
        .iter()
        .copied()
        .filter(|flow| !flow.private)
        .collect::<Vec<_>>();
    let shielded_net = ShieldedAccounting::from_raw(raw).net_to_transparent_zat();
    let public_delta = sum_known_values(&transparent_inputs)
        .zip(sum_known_values(&transparent_outputs))
        .zip(shielded_net)
        .and_then(|((inputs, outputs), net)| inputs.checked_sub(outputs)?.checked_add(net));
    let protocol = protocol_value_accounting(raw, store);
    let fee = if coinbase {
        "n/a".to_string()
    } else if protocol {
        "unknown (protocol accounting)".to_string()
    } else {
        sum_known_values(&transparent_inputs)
            .zip(sum_known_values(&transparent_outputs))
            .zip(shielded_net)
            .map(|((inputs, outputs), shielded_net)| {
                format!(
                    "{} {currency}",
                    format_native_zat(inputs - outputs + shielded_net)
                )
            })
            .unwrap_or_else(|| "unknown".to_string())
    };
    let value = |label: &str, content: String| match theme {
        Some(theme) => theme.value(label, content, Style::default()),
        None => Span::raw(content),
    };
    let mut values = vec![
        Span::raw("Inputs "),
        value("input count", inputs.len().to_string()),
        Span::raw(": "),
        value("input value", input_total),
        Span::raw("   Outputs "),
        value("output count", outputs.len().to_string()),
        Span::raw(": "),
        value("output value", output_total),
    ];
    if protocol && !coinbase {
        values.push(Span::raw("   Public native delta: "));
        values.push(value(
            "public native delta",
            public_delta
                .map(|delta| format!("{} {currency}", format_native_zat(delta)))
                .unwrap_or_else(|| "unknown".into()),
        ));
    }
    (values, fee)
}

fn transaction_side_value_label(
    flows: &[&TransactionFlow],
    currency: &str,
    store: &ObjectStore,
) -> String {
    let hidden = flows
        .iter()
        .any(|flow| flow.private && flow.value_zat.is_none());
    let derived = flows
        .iter()
        .any(|flow| flow.private && flow.value_zat.is_some());
    let known = flows
        .iter()
        .copied()
        .filter(|flow| flow.value_zat.is_some())
        .collect::<Vec<_>>();
    let Some(total) = sum_known_values(&known) else {
        return "unknown".to_string();
    };
    let native = if hidden {
        format!("{} {currency} known + hidden", format_native_zat(total))
    } else if derived {
        format!("{} {currency} (partly derived)", format_native_zat(total))
    } else {
        format!("{} {currency}", format_native_zat(total))
    };
    let mut amounts = BTreeMap::<String, i128>::new();
    for flow in flows {
        for (id, amount) in &flow.reserve_amounts {
            *amounts.entry(id.clone()).or_default() += amount;
        }
    }
    let mut labels = vec![native];
    labels.extend(
        amounts
            .iter()
            .filter(|(_, amount)| **amount != 0)
            .map(|(id, amount)| currency_amount_label(currency_display_value(store, id), *amount)),
    );
    if flows
        .iter()
        .any(|flow| !flow.private && !flow.coinbase && flow.value_zat.is_none())
    {
        labels.push("remaining inputs/outputs unknown".into());
    }
    labels.join(" + ")
}

fn sum_known_values(flows: &[&TransactionFlow]) -> Option<i128> {
    flows
        .iter()
        .try_fold(0_i128, |total, flow| Some(total + flow.value_zat?))
}

fn render_transaction_flows(
    frame: &mut Frame<'_>,
    context: TransactionFlowRenderContext<'_>,
    flows: &[TransactionFlow],
    selected: usize,
    area: Rect,
) {
    let selections = transaction_flow_selections(flows);
    let Some(&(selected_flow, selected_link)) =
        selections.get(selected.min(selections.len().saturating_sub(1)))
    else {
        frame.render_widget(
            Paragraph::new("No inputs or outputs in this transaction.")
                .style(context.theme.muted()),
            area,
        );
        return;
    };
    let content = scroll_view::content_area(area, flows.len() > 1);
    let width = tile_content_area(content).width.max(1);
    let tiles: Vec<_> = flows
        .iter()
        .enumerate()
        .map(|(index, flow)| {
            transaction_flow_tile(
                &context,
                flow,
                (index == selected_flow).then_some(selected_link),
                width,
            )
        })
        .collect();
    let heights: Vec<_> = tiles
        .iter()
        .map(|tile| {
            let mut lines = tile.title.iter().cloned().collect::<Vec<_>>();
            lines.extend(tile.lines.iter().cloned());
            u16::try_from(
                Paragraph::new(lines)
                    .wrap(Wrap { trim: true })
                    .line_count(width),
            )
            .unwrap_or(u16::MAX)
            .saturating_add(1)
        })
        .collect();
    let (first, last) = variable_height_visible_range(&heights, selected_flow, area.height);
    let rows = Layout::vertical(heights[first..last].iter().copied().map(Constraint::Length))
        .split(content);
    for (row, tile) in rows
        .iter()
        .zip(tiles.into_iter().skip(first).take(last - first))
    {
        render_summary_tile(frame, context.theme, tile, *row);
    }
    scroll_view::render(frame, context.theme, area, flows.len(), first, last - first);
}

fn source_outpoint_label(flow: &TransactionFlow, width: u16) -> String {
    let Some(txid) = &flow.source_txid else {
        return String::new();
    };
    let suffix = flow
        .source_vout
        .map(|index| format!(":{index}"))
        .unwrap_or_default();
    let full = format!("from {txid}{suffix}");
    if Line::from(full.as_str()).width() <= usize::from(width) {
        full
    } else {
        format!("from {}{suffix}", short_id(txid))
    }
}

fn input_context_lines(
    context: &TransactionFlowRenderContext<'_>,
    flow: &TransactionFlow,
    width: u16,
) -> Vec<Line<'static>> {
    let Some(txid) = flow.source_txid.as_deref() else {
        return Vec::new();
    };
    let source = context.store.transaction(txid);
    let block_time = source
        .filter(|record| record.block_hash.is_some() && record.block_time > 0)
        .map(|record| context.theme.format_time(record.block_time))
        .unwrap_or_else(|| "unknown".into());
    let confirmations = if context.transaction.block_hash.is_none() {
        "not yet confirmed".into()
    } else {
        context
            .transaction
            .block_height
            .zip(source.and_then(|record| record.block_height))
            .and_then(|(spent, created)| spent.checked_sub(created))
            .map(|depth| depth.to_string())
            .unwrap_or_else(|| "unknown".into())
    };
    let main = if context.theme.heights.borrow().dates && context.transaction.block_hash.is_some() {
        source
            .filter(|s| s.block_hash.is_some())
            .and_then(|s| s.block_height.map(|h| (s, h)))
            .map(|(s, h)| {
                format!(
                    "Block time {block_time}   Confirmed since {} (when spent)",
                    context.theme.format_height_at(h, Some(s.block_time))
                )
            })
            .unwrap_or_else(|| {
                format!("Block time {block_time}   Confirmations when spent {confirmations}")
            })
    } else {
        format!("Block time {block_time}   Confirmations when spent {confirmations}")
    };
    let with_height = source.and_then(|record| record.block_height).map(|height| {
        format!(
            "{main}   Height {}",
            context
                .theme
                .format_height_at(height, source.map(|s| s.block_time))
        )
    });
    vec![Line::from(
        with_height
            .filter(|text| Line::from(text.as_str()).width() <= usize::from(width))
            .unwrap_or(main),
    )]
}

fn output_status_badges(
    transaction: &TransactionRecord,
    flow: &TransactionFlow,
    store: &ObjectStore,
) -> Vec<StatusBadge> {
    if flow.kind != TransactionFlowKind::Output || flow.private {
        return Vec::new();
    }
    let raw_output = transaction
        .raw_json
        .get("vout")
        .and_then(Value::as_array)
        .and_then(|outputs| {
            outputs
                .iter()
                .enumerate()
                .find(|(index, output)| {
                    output
                        .get("n")
                        .and_then(Value::as_u64)
                        .unwrap_or(*index as u64)
                        == flow.index as u64
                })
                .map(|(_, output)| output)
        });
    let Some(output) = raw_output else {
        return Vec::new();
    };
    let script = &output["scriptPubKey"];
    let monetary = flow.value_zat.is_some_and(|value| value > 0)
        || script
            .get("reserve_balance")
            .and_then(Value::as_object)
            .is_some_and(|balances| {
                balances
                    .values()
                    .any(|amount| amount.as_f64().is_some_and(|value| value > 0.0))
            });
    if !monetary || flow.script_type.as_deref() == Some("nulldata") {
        return Vec::new();
    }
    let depth = transaction
        .block_hash
        .as_deref()
        .and_then(|hash| store.chain.confirmations(hash))
        .or_else(|| {
            store
                .chain
                .in_mempool(&transaction.summary.txid)
                .then_some(0)
        });
    let Some(depth) = depth else {
        return Vec::new();
    };
    let coinbase = transaction
        .raw_json
        .get("vin")
        .and_then(Value::as_array)
        .is_some_and(|inputs| inputs.iter().any(|input| input.get("coinbase").is_some()));
    let chain_name = store
        .chain
        .chain_name
        .as_deref()
        .unwrap_or("")
        .trim_end_matches('@')
        .trim_end_matches('.')
        .to_ascii_lowercase();
    let is_verus = matches!(chain_name.as_str(), "vrsc" | "vrsctest");
    let definition = store
        .chain
        .chain_id
        .as_ref()
        .and_then(|id| store.currencies_by_id.get(id));
    let maturity = if is_verus {
        Some(100)
    } else {
        definition.and_then(|definition| {
            let name = definition
                .name
                .trim_end_matches('@')
                .trim_end_matches('.')
                .to_ascii_lowercase();
            coinbase_maturity(
                definition
                    .definition_json
                    .get("blocktime")
                    .and_then(Value::as_u64)?,
                name == "chips777.vrsctest",
            )
        })
    };
    let mut badges = Vec::new();
    // PBaaS block-one allocations bypass ordinary coinbase maturity. Instant-spend
    // protocol threads carry no currency value and were excluded above.
    if coinbase
        && (is_verus || transaction.block_height != Some(1))
        && maturity.is_some_and(|maturity| depth < maturity)
    {
        badges.push(StatusBadge::Immature);
    }
    let stake_candidate = flow.value_zat.is_some_and(|value| value > 0)
        && match flow.script_type.as_deref() {
            Some("pubkey" | "pubkeyhash") => flow.spendable != Some(false),
            Some("cryptocondition") => flow.spendable == Some(true),
            _ => false,
        };
    if stake_candidate && depth < 150 {
        badges.push(StatusBadge::StakeAgePending);
    }
    badges
}

fn transaction_flow_tile(
    context: &TransactionFlowRenderContext<'_>,
    flow: &TransactionFlow,
    selected_link: Option<FlowLink>,
    width: u16,
) -> SummaryTile {
    let theme = context.theme;
    let selected = selected_link.is_some();
    let (marker, kind, direction_style) = match (flow.kind, flow.private) {
        (TransactionFlowKind::Input, true) => ("←", "PRIVATE INPUT", private_flow_style(theme)),
        (TransactionFlowKind::Output, true) => ("→", "PRIVATE OUTPUT", private_flow_style(theme)),
        (TransactionFlowKind::Input, false) => ("←", "INPUT", theme.warning()),
        (TransactionFlowKind::Output, false) => ("→", "OUTPUT", theme.positive()),
    };
    let title = format!("{marker} {kind} {}", flow.index);
    let link_style = |link| {
        if selected_link == Some(link) {
            theme.selection_cursor()
        } else {
            theme.link()
        }
    };
    let mut lines = Vec::new();
    if let Some(txid) = &flow.source_txid {
        lines.push(Line::from(
            theme.value_as(
                "source outpoint",
                source_outpoint_label(flow, width),
                flow.source_vout
                    .map_or_else(|| txid.clone(), |n| format!("{txid}:{n}")),
                link_style(FlowLink::SourceTransaction),
            ),
        ));
    } else if flow.coinbase {
        lines.push(Line::from(TransactionTag::Coinbase.label()));
    } else if flow.private {
        lines.push(Line::from("shielded pool"));
    }
    if flow.addresses.is_empty() {
        lines.push(Line::from(Span::styled(
            if flow.private {
                "address unknown (shielded)"
            } else {
                "address unavailable"
            },
            theme.muted(),
        )));
    } else {
        for (index, address) in flow.addresses.iter().enumerate() {
            if let Some(label) = flow.routing.get(address) {
                lines.push(Line::from(Span::styled(label.clone(), theme.muted())));
            }
            lines.push(Line::from(vec![
                Span::styled(
                    if flow.routing.contains_key(address) {
                        "raw activity "
                    } else {
                        "address "
                    },
                    theme.muted(),
                ),
                theme.value(
                    "address",
                    identity_display_value(
                        context.store,
                        address,
                        context.unmask_identity_addresses,
                    ),
                    link_style(FlowLink::Address(index)),
                ),
            ]));
        }
    }
    let amount = || {
        theme.value(
            "amount",
            currency_value_label(&flow.value_label, context.currency),
            Style::default(),
        )
    };
    let script = || {
        theme.value(
            "script type",
            flow.script_type.as_deref().unwrap_or(if flow.private {
                "shielded"
            } else {
                "unknown"
            }),
            Style::default(),
        )
    };
    let mut details = if flow.private && flow.value_zat.is_some() {
        vec![
            Span::raw("derived value "),
            amount(),
            Span::raw(" ("),
            script(),
            Span::raw(")"),
        ]
    } else if flow.private {
        vec![Span::raw("individual value unknown   pool "), script()]
    } else {
        vec![
            Span::raw("value "),
            amount(),
            Span::raw("   script "),
            script(),
        ]
    };
    if let Some(sequence) = flow.sequence {
        details.extend([
            Span::raw("   sequence "),
            theme.value("sequence", sequence.to_string(), Style::default()),
        ]);
    }
    if let Some(spendable) = flow.spendable {
        details.extend([
            Span::raw("   "),
            theme.value(
                "output type",
                if spendable {
                    "spendable output type"
                } else {
                    "protocol output type"
                },
                Style::default(),
            ),
        ]);
    }
    lines.push(Line::from(details));
    for (id, amount) in &flow.reserve_amounts {
        if *amount != 0 {
            lines.push(Line::from(theme.value(
                "reserve amount",
                currency_amount_label(currency_display_value(context.store, id), *amount),
                Style::default(),
            )));
        }
    }
    lines.extend(input_context_lines(context, flow, width));
    let mut decorations = Vec::new();
    for (index, tag) in flow.tags.iter().enumerate() {
        if !decorations.is_empty() {
            decorations.push(Span::raw(" "));
        }
        let actionable = tag_target(flow, tag).is_some();
        let label = format!(
            "{}{}",
            display_output_tag(tag, context.store),
            if actionable {
                theme.glyph(" ↗", " >")
            } else {
                ""
            }
        );
        let mut badge = transaction_tag_spans(theme, &[label]);
        if let Some(span) = badge.first_mut() {
            if selected_link == Some(FlowLink::Tag(index)) {
                span.style = theme.selection_cursor();
            } else if actionable {
                span.style = span.style.add_modifier(Modifier::UNDERLINED);
            }
        }
        decorations.extend(badge);
    }
    let annotations = flow
        .annotations
        .iter()
        .filter(|annotation| !annotation.starts_with("Derived from "))
        .map(|label| theme.height_text(label, ""))
        .collect::<Vec<_>>();
    if !annotations.is_empty() && !decorations.is_empty() {
        decorations.push(Span::raw(" "));
    }
    decorations.extend(transaction_tag_spans(theme, &annotations));
    for badge in output_status_badges(context.transaction, flow, context.store) {
        if !decorations.is_empty() {
            decorations.push(Span::raw(" "));
        }
        decorations.push(badge_span(theme, badge));
    }
    if !decorations.is_empty() {
        lines.push(Line::from(decorations));
    }
    for (index, reference) in flow.references.links.iter().enumerate() {
        let value = match &reference.target {
            crate::protocol::Target::Currency(id) => {
                currency_display_value(context.store, id).to_string()
            }
            crate::protocol::Target::Destination { address, system } => {
                let address = identity_display_value(
                    context.store,
                    address,
                    context.unmask_identity_addresses,
                );
                if let Some(system) = system {
                    format!(
                        "{address} on {}",
                        currency_display_value(context.store, system.as_ref())
                    )
                } else {
                    address
                }
            }
            crate::protocol::Target::LegacyNotarizedBlock { hash, height } => {
                format!("#{height} {}", short_id(hash))
            }
            crate::protocol::Target::OpReturn(data) => {
                let subject = if data.script_body {
                    "script bytes"
                } else {
                    "payload"
                };
                format!("{} bytes ({subject})", data.bytes.len())
            }
            crate::protocol::Target::Outpoint(target) => format!(
                "{}:{} on {}",
                short_id(&target.txid),
                target.vout,
                currency_display_value(context.store, target.system_id.as_ref())
            ),
        };
        let copy_value = match &reference.target {
            crate::protocol::Target::LegacyNotarizedBlock { hash, .. } => hash.clone(),
            crate::protocol::Target::Outpoint(target) => format!("{}:{}", target.txid, target.vout),
            _ => value.clone(),
        };
        lines.push(Line::from(vec![
            Span::styled(format!("{}: ", reference.label), theme.muted()),
            theme.value_as(
                reference.label.as_str(),
                value,
                copy_value,
                link_style(FlowLink::Protocol(index)),
            ),
        ]));
    }
    lines.extend(
        flow.references
            .notes
            .iter()
            .cloned()
            .map(|note| Line::styled(note, theme.muted())),
    );
    SummaryTile {
        title: Some(Line::from(Span::styled(
            title,
            direction_style.bg(if selected {
                theme.panel
            } else {
                theme.background
            }),
        ))),
        selected,
        lines,
        alignment: if flow.kind == TransactionFlowKind::Output {
            Alignment::Right
        } else {
            Alignment::Left
        },
    }
}

fn private_flow_style(theme: &Theme) -> Style {
    Style::default()
        .fg(theme.background)
        .bg(theme.link)
        .add_modifier(Modifier::BOLD)
}

fn currency_value_label(value: &str, currency: &str) -> String {
    if value == "value unavailable" {
        value.to_string()
    } else {
        format!("{value} {currency}")
    }
}

fn render_address_view(
    frame: &mut Frame<'_>,
    config: &Config,
    theme: &Theme,
    state: &AppState,
    address: &str,
    selection: ActivitySelection,
    area: Rect,
) {
    let association_height = state
        .store
        .identity_associations(address)
        .map_or(0, |associations| {
            u16::try_from(associations.identities.len().max(1).saturating_add(1))
                .unwrap_or(7)
                .min(7)
        });
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Length(1),
            Constraint::Length(association_height),
            Constraint::Length(u16::from(association_height > 0)),
            Constraint::Min(4),
        ])
        .split(area);
    render_activity_summary_header(
        frame,
        config,
        theme,
        state,
        ActivityHeader {
            object_label: "Address",
            display_value: address,
            activity_address: address,
        },
        layout[0],
    );
    if association_height > 0 {
        render_address_identity_associations(
            frame,
            theme,
            state,
            address,
            selection.link,
            layout[2],
        );
    }
    render_address_activity(
        frame,
        config,
        theme,
        state,
        address,
        selection.link.is_none().then_some(selection.transaction),
        layout[4],
    );
}

fn render_address_identity_associations(
    frame: &mut Frame<'_>,
    theme: &Theme,
    state: &AppState,
    address: &str,
    selected: Option<usize>,
    area: Rect,
) {
    let Some(associations) = state.store.identity_associations(address) else {
        return;
    };
    let source = match associations.source {
        IdentityAssociationSource::LocalDaemon => "local RPC",
        IdentityAssociationSource::PublicApi => "public API",
    };
    let total = associations.identities.len();
    let rail = Rect::new(
        area.x,
        area.y.saturating_add(1),
        area.width,
        area.height.saturating_sub(1),
    );
    let visible = usize::from(rail.height).max(1);
    let selected_index = selected.unwrap_or(0).min(total.saturating_sub(1));
    let first = selected_index
        .saturating_sub(visible / 2)
        .min(total.saturating_sub(visible));
    let body = detail_header_content(scroll_view::content_area(area, total > visible));
    let mut lines = vec![Line::from(vec![
        Span::styled("Primary address for ", theme.focused()),
        Span::styled(format!("{total} identities"), theme.muted()),
        Span::styled(format!("  via {source}"), theme.muted()),
    ])];
    if associations.identities.is_empty() {
        lines.push(Line::from(Span::styled(
            "  No associated identities found.",
            theme.muted(),
        )));
    } else {
        for (index, identity) in associations
            .identities
            .iter()
            .enumerate()
            .skip(first)
            .take(visible)
        {
            let active = selected == Some(index);
            let line = justified_line(
                vec![theme.value("identity name", identity.display_name.clone(), theme.link())],
                vec![theme.value(
                    "identity i-address",
                    identity.identity_address.clone(),
                    theme.muted(),
                )],
                body.width,
            );
            lines.push(line.style(if active {
                theme.focused()
            } else {
                theme.base()
            }));
        }
    }
    frame.render_widget(Paragraph::new(lines).style(theme.base()), body);
    if let Some(offset) = selected.and_then(|index| index.checked_sub(first))
        && offset < total.saturating_sub(first).min(usize::from(rail.height))
    {
        render_detail_header_cursor(frame, theme, rail, offset);
    }
    scroll_view::render(frame, theme, rail, total, first, visible);
}

fn detail_header_content(area: Rect) -> Rect {
    let indent = u16::from(area.width > 1);
    Rect {
        x: area.x + indent,
        width: area.width - indent,
        ..area
    }
}

fn render_detail_header_cursor(frame: &mut Frame<'_>, theme: &Theme, area: Rect, offset: usize) {
    if area.width > 1 && offset < usize::from(area.height) {
        render_selection_gutter(
            frame,
            theme,
            Rect::new(area.x, area.y + offset as u16, 1, 1),
        );
    }
}

struct ActivityHeader<'a> {
    object_label: &'a str,
    display_value: &'a str,
    activity_address: &'a str,
}

#[derive(Debug, Clone, Copy)]
struct ActivitySelection {
    transaction: usize,
    link: Option<usize>,
}

fn render_activity_summary_header(
    frame: &mut Frame<'_>,
    config: &Config,
    theme: &Theme,
    state: &AppState,
    header: ActivityHeader<'_>,
    area: Rect,
) {
    let area = detail_header_content(area);
    let Some(record) = state.store.address(header.activity_address) else {
        let status = address_loading_status(state, header.activity_address, "chain activity");
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled(
                    header.object_label.to_string(),
                    theme.focused(),
                )),
                Line::from(theme.value("address", header.display_value, theme.link())),
                Line::from(""),
                Line::from(Span::styled(status, theme.warning())),
            ])
            .style(theme.base()),
            area,
        );
        return;
    };
    let currency = native_currency_name(config, &state.store);
    let balance_label = address_balance_label(state, record, &currency);
    let history_status = if state
        .store
        .stale_addresses
        .contains(header.activity_address)
    {
        "Stale after reorg; refreshing".to_string()
    } else {
        address_history_status(state, header.activity_address)
    };
    let first = if record.history_complete {
        address_transaction_time(record.first_txid.as_deref(), &state.store, theme)
    } else {
        history_status.clone()
    };
    let latest = address_transaction_time(record.latest_txid.as_deref(), &state.store, theme);
    let transaction_count = if record.history_complete {
        record.transaction_count.to_string()
    } else {
        format!("{} recent ({history_status})", record.transaction_count)
    };
    let header = Paragraph::new(vec![
        Line::from(Span::styled(
            header.object_label.to_string(),
            theme.focused(),
        )),
        Line::from(theme.value("address", header.display_value, theme.link())),
        Line::from(vec![
            Span::styled("Balances: ", theme.muted()),
            theme.value("balances", balance_label, theme.positive()),
        ]),
        Line::from(vec![
            Span::styled("Transactions: ", theme.muted()),
            theme.value("transaction count", transaction_count, Style::default()),
            Span::styled("   First: ", theme.muted()),
            theme.value("first transaction time", first, Style::default()),
            Span::styled("   Most recent: ", theme.muted()),
            theme.value("latest transaction time", latest, Style::default()),
        ]),
    ])
    .style(theme.base())
    .wrap(Wrap { trim: true });
    frame.render_widget(header, area);
}

fn address_transaction_time(txid: Option<&str>, store: &ObjectStore, theme: &Theme) -> String {
    txid.and_then(|txid| store.transaction(txid))
        .map(|transaction| theme.format_time(transaction.block_time))
        .unwrap_or_else(|| "unknown".to_string())
}

fn address_loading_status(state: &AppState, address: &str, subject: &str) -> String {
    if let Some(error) = state.address_errors.get(address) {
        if state.loading_addresses.contains(address) {
            return format!("Retrying {subject} after: {error}");
        }
        let remaining = state
            .address_retry_after
            .get(address)
            .map(|retry_at| retry_at.saturating_duration_since(Instant::now()).as_secs())
            .unwrap_or_default();
        return format!("Unable to load {subject}: {error}  retry in {remaining}s");
    }
    format!("Loading {subject}...")
}

fn address_history_status(state: &AppState, address: &str) -> String {
    if state.loading_address_histories.contains(address) {
        return "history loading".to_string();
    }
    if state.address_history_errors.contains_key(address) {
        let remaining = state
            .address_history_retry_after
            .get(address)
            .map(|retry_at| retry_at.saturating_duration_since(Instant::now()).as_secs())
            .unwrap_or_default();
        return format!("history retry in {remaining}s");
    }
    "history queued".to_string()
}

fn address_balance_status(state: &AppState, address: &str) -> String {
    if state.loading_address_balances.contains(address) {
        return "loading balance".to_string();
    }
    if state.address_balance_errors.contains_key(address) {
        let remaining = state
            .address_balance_retry_after
            .get(address)
            .map(|retry_at| retry_at.saturating_duration_since(Instant::now()).as_secs())
            .unwrap_or_default();
        return format!("balance retry in {remaining}s");
    }
    "balance queued".to_string()
}

#[allow(clippy::too_many_arguments)]
fn render_identity_view(
    frame: &mut Frame<'_>,
    config: &Config,
    theme: &Theme,
    state: &AppState,
    target: &IdentityTarget,
    selection: ActivitySelection,
    historical_height_input: Option<&str>,
    area: Rect,
) {
    let Some(version) = state.store.identity_for_target(target) else {
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled("Identity", theme.focused())),
                Line::from(Span::styled(target.query.clone(), theme.link())),
                Line::from(""),
                Line::from(Span::styled("Loading identity...", theme.warning())),
            ])
            .style(theme.base()),
            detail_header_content(area),
        );
        return;
    };
    let evidence_height = state
        .store
        .identity_evidence
        .get(&version.identity_address)
        .map_or(1, |(_, report)| {
            2u16.saturating_add(u16::from(report.registration.is_some()))
                .saturating_add(u16::try_from(report.links.len().clamp(1, 6)).unwrap_or(6))
        })
        .saturating_add(u16::from(historical_height_input.is_some()));
    let width = detail_header_content(area).width.max(1);
    let (lines, selected_line) =
        identity_header_lines(config, theme, state, version, selection.link, width);
    let cursor_row = selected_line.map(|index| {
        Paragraph::new(lines[..index].to_vec())
            .wrap(Wrap { trim: true })
            .line_count(width)
    });
    let header = Paragraph::new(lines)
        .style(theme.base())
        .wrap(Wrap { trim: true });
    let header_height = u16::try_from(header.line_count(width)).unwrap_or(u16::MAX);
    let layout = Layout::vertical([
        Constraint::Length(header_height),
        Constraint::Length(evidence_height),
        Constraint::Min(4),
    ])
    .spacing(1)
    .split(area);
    frame.render_widget(header, detail_header_content(layout[0]));
    if let Some(row) = cursor_row {
        render_detail_header_cursor(frame, theme, layout[0], row);
    }
    render_identity_evidence(
        frame,
        theme,
        state,
        version,
        selection.link,
        historical_height_input,
        layout[1],
    );
    render_address_activity(
        frame,
        config,
        theme,
        state,
        &version.identity_address,
        selection.link.is_none().then_some(selection.transaction),
        layout[2],
    );
}

fn render_identity_evidence(
    frame: &mut Frame<'_>,
    theme: &Theme,
    state: &AppState,
    version: &IdentityVersionRecord,
    selected_link: Option<usize>,
    historical_height_input: Option<&str>,
    area: Rect,
) {
    let Some((_, report)) = state.store.identity_evidence.get(&version.identity_address) else {
        let message = if state
            .store
            .loading_identity_evidence
            .contains(&version.identity_address)
        {
            "Registration and association evidence loading..."
        } else if state
            .store
            .identity_history_complete
            .contains(&version.identity_address)
        {
            "No additional associations to display"
        } else {
            "Registration evidence awaits complete identity history"
        };
        frame.render_widget(
            Paragraph::new(message).style(theme.muted()),
            detail_header_content(area),
        );
        return;
    };
    let mut lines = vec![Line::from(vec![
        Span::styled("Registration and associations", theme.focused()),
        Span::raw("  "),
        theme.value(
            "evidence link count",
            report.links.len().to_string(),
            theme.muted(),
        ),
        Span::styled(" evidence links", theme.muted()),
    ])];
    if let Some(input) = historical_height_input {
        lines.push(Line::from(format!("Historical reverse window ends at: {input}_  (Enter to query 1,000 blocks, Esc to cancel)")));
    }
    if let Some(registration) = &report.registration {
        let outpoint = report
            .links
            .iter()
            .find(|link| link.label == "Registration transaction");
        lines.push(
            if let Some((name, shortened)) = registration.rsplit_once("  tx ")
                && let Some(link) = outpoint
            {
                Line::from(vec![
                    Span::raw("  "),
                    theme.value("registered identity", name, Style::default()),
                    Span::raw("  tx "),
                    theme.value_as(
                        "registration outpoint",
                        shortened,
                        &link.source,
                        Style::default(),
                    ),
                ])
            } else {
                Line::from(vec![
                    Span::raw("  "),
                    theme.value(
                        "registration details",
                        theme.height_text(registration, ""),
                        Style::default(),
                    ),
                ])
            },
        );
    }
    let header_count = identity_header_item_count(version);
    let heading_height = lines.len() as u16;
    let links_area = Rect::new(
        area.x,
        area.y.saturating_add(heading_height),
        area.width,
        area.height.saturating_sub(heading_height).saturating_sub(1),
    );
    let visible = usize::from(links_area.height).max(1);
    let content = detail_header_content(scroll_view::content_area(
        links_area,
        report.links.len() > visible,
    ));
    let selected = selected_link.and_then(|index| index.checked_sub(header_count));
    let first = selected
        .unwrap_or_default()
        .saturating_sub(visible / 2)
        .min(report.links.len().saturating_sub(visible));
    for (offset, link) in report.links.iter().skip(first).take(visible).enumerate() {
        let index = first + offset;
        let target = match &link.target {
            identity_evidence::EvidenceTarget::Identity(value) => {
                let name =
                    identity_display_value(&state.store, value, state.unmask_identity_addresses);
                if name == *value && !state.unmask_identity_addresses {
                    short_id(value)
                } else {
                    truncate_middle(&name, usize::from(area.width / 3).max(12))
                }
            }
            identity_evidence::EvidenceTarget::Address(value)
            | identity_evidence::EvidenceTarget::Transaction(value) => short_id(value),
        };
        let active = selected == Some(index);
        let width = usize::from(content.width);
        let label = Span::styled(link.label.clone(), theme.link());
        let target_width = Span::raw(target.as_str()).width();
        let detail_width = width.saturating_sub(label.width() + target_width + 4);
        let mut spans = vec![label];
        if detail_width > 0 && !link.detail.is_empty() {
            let detail = evidence_detail_display(state, &theme.height_text(&link.detail, ""));
            let mut limit = detail_width;
            let mut fitted = detail;
            if Span::raw(fitted.as_str()).width() > limit {
                let detail = fitted;
                fitted = truncate_middle(&detail, limit);
                while Span::raw(fitted.as_str()).width() > detail_width {
                    limit -= 1;
                    fitted = truncate_middle(&detail, limit);
                }
            }
            let value = link
                .detail
                .strip_prefix("hash ")
                .filter(|hash| is_hash_like(hash))
                .unwrap_or(&link.detail);
            spans.push(Span::raw("  "));
            spans.push(theme.value_as("association detail", fitted, value, Style::default()));
        }
        let left_width: usize = spans.iter().map(Span::width).sum();
        spans.push(Span::raw(
            " ".repeat(width.saturating_sub(left_width + target_width).max(1)),
        ));
        spans.push(theme.value_as(
            "association target",
            target,
            match &link.target {
                identity_evidence::EvidenceTarget::Identity(value)
                | identity_evidence::EvidenceTarget::Address(value)
                | identity_evidence::EvidenceTarget::Transaction(value) => value.as_str(),
            },
            theme.muted(),
        ));
        lines.push(Line::from(spans).style(if active {
            theme.focused()
        } else {
            theme.base()
        }));
    }
    if report.links.is_empty() {
        lines.push(Line::from("  No additional associations to display"));
    }
    if let Some(link) = selected.and_then(|index| report.links.get(index)) {
        lines.push(Line::from(vec![
            Span::styled("  Source: ", theme.muted()),
            theme.value_as(
                "association source",
                truncate_middle(&link.source, usize::from(content.width.saturating_sub(10))),
                &link.source,
                theme.muted(),
            ),
        ]));
    } else if let Some(note) = report
        .notes
        .iter()
        .rev()
        .find(|note| evidence_note_visible(note))
    {
        lines.push(Line::from(Span::styled(format!("  {note}"), theme.muted())));
    } else {
        lines.push(Line::from(Span::styled(
            "  Association is evidence, not ownership; private funding may be invisible",
            theme.muted(),
        )));
    }
    let body = Rect {
        x: content.x,
        width: content.width,
        ..area
    };
    frame.render_widget(Paragraph::new(lines).style(theme.base()), body);
    if let Some(offset) = selected.and_then(|index| index.checked_sub(first))
        && offset
            < report
                .links
                .len()
                .saturating_sub(first)
                .min(usize::from(links_area.height))
    {
        render_detail_header_cursor(frame, theme, links_area, offset);
    }
    scroll_view::render(frame, theme, links_area, report.links.len(), first, visible);
}

fn identity_has_currency(version: &IdentityVersionRecord) -> bool {
    version.raw_json["identity"]
        .get("flags")
        .and_then(Value::as_u64)
        .is_some_and(|flags| flags & 1 != 0)
}

fn identity_header_item_count(version: &IdentityVersionRecord) -> usize {
    identity_primary_addresses(version).len() + usize::from(token_control::enabled(version))
}

fn identity_token_holder<'a>(
    store: &'a ObjectStore,
    version: &IdentityVersionRecord,
) -> Option<&'a str> {
    if !token_control::enabled(version) {
        return None;
    }
    match &store.token_control.get(&version.identity_address)?.status {
        token_control::Status::Unspent(_, Some(address)) => Some(address),
        _ => None,
    }
}

fn identity_token_control_line(
    theme: &Theme,
    state: &AppState,
    version: &IdentityVersionRecord,
    selected: bool,
) -> Line<'static> {
    use token_control::Status;
    let label = Span::styled("Current token holder: ", theme.muted());
    let status = state
        .store
        .token_control
        .get(&version.identity_address)
        .map(|lookup| &lookup.status);
    let detail = match status {
        Some(Status::Unspent(frontier, Some(address))) => {
            return Line::from(vec![
                label,
                theme.value_as(
                    "control token holder",
                    identity_display_value(&state.store, address, state.unmask_identity_addresses),
                    address,
                    theme.link(),
                ),
                Span::styled(
                    format!("  ·  {} hops checked", frontier.hops),
                    theme.muted(),
                ),
            ])
            .style(if selected {
                theme.focused()
            } else {
                theme.base()
            });
        }
        Some(Status::Unspent(frontier, None)) => {
            format!(
                "Unspent output has no single holder address  ·  {} hops checked",
                frontier.hops
            )
        }
        Some(Status::Paused(frontier)) => format!(
            "{} hops checked  ·  Enter follows up to {} more",
            frontier.hops,
            token_control::HOPS_PER_PASS
        ),
        Some(Status::Unresolved(Some(frontier), reason)) => {
            format!(
                "{} hops checked  ·  {reason}  ·  Enter retries",
                frontier.hops
            )
        }
        Some(Status::Unresolved(None, reason)) => format!("{reason}  ·  Enter retries"),
        Some(Status::Loading) => "Checking…".to_owned(),
        None if state
            .store
            .stale_identities
            .contains(&version.identity_address) =>
        {
            "Waiting for refreshed identity data…".to_owned()
        }
        None => "Enter checks the control token".to_owned(),
    };
    Line::from(vec![label, Span::raw(detail)]).style(if selected {
        theme.focused()
    } else {
        theme.base()
    })
}

fn identity_header_lines(
    config: &Config,
    theme: &Theme,
    state: &AppState,
    version: &IdentityVersionRecord,
    selected_primary_address: Option<usize>,
    width: u16,
) -> (Vec<Line<'static>>, Option<usize>) {
    let identity = version.raw_json.get("identity").unwrap_or(&Value::Null);
    let own_address = version.identity_address.as_str();
    let name = state
        .store
        .identity_local_name(own_address)
        .or_else(|| identity.get("name").and_then(Value::as_str))
        .unwrap_or(version.fully_qualified_name.trim_end_matches('@'));
    let name = friendly_identity_name(name.trim_end_matches('@'));
    let status = version
        .raw_json
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let status = if state.store.stale_identities.contains(own_address) {
        "Stale after reorg"
    } else {
        status
    };
    let status_style = if status.eq_ignore_ascii_case("active") {
        theme.positive()
    } else {
        theme.negative()
    };
    let primary_addresses = identity_primary_addresses(version);
    let version_number = identity
        .get("version")
        .map(Value::to_string)
        .unwrap_or_else(|| "?".to_string());
    let minimum_signatures = identity
        .get("minimumsignatures")
        .map(Value::to_string)
        .unwrap_or_else(|| "?".to_string());
    let mut timelock = identity_timelock_label(identity, state.tip.height);
    if let Some(h) = identity["timelock"].as_u64().filter(|h| *h > 0)
        && timelock.contains("unlocks at height")
    {
        timelock = format!("locked (unlocks {})", theme.format_height(h));
    }
    let authority = |label, key| {
        (
            label,
            identity_authority_label(state, identity, key, own_address),
            identity[key].as_str(),
        )
    };
    let versions = state
        .store
        .identity_heights
        .get(own_address)
        .map(|heights| {
            heights
                .iter()
                .filter_map(|height| {
                    state
                        .store
                        .identity_versions
                        .get(&(own_address.to_string(), *height))
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let registration = versions.first().copied().unwrap_or(version);
    let latest = versions.last().copied().unwrap_or(version);
    let history_complete = state.store.identity_history_complete.contains(own_address);
    let registration_label = if history_complete {
        format_identity_height_time(registration.block_height, registration.block_time, theme)
    } else {
        "loading history".to_string()
    };
    let updated = if !history_complete {
        "loading history".to_string()
    } else if latest.block_height != registration.block_height {
        format_identity_height_time(latest.block_height, latest.block_time, theme)
    } else {
        "never".to_string()
    };
    let (balances, activity) = if let Some(address) = state.store.address(own_address) {
        let currency = native_currency_name(config, &state.store);
        let balances = address_balance_label(state, address, &currency);
        let activity = format!(
            "{} tx   first {}   latest {}",
            address.transaction_count,
            address_transaction_time(address.first_txid.as_deref(), &state.store, theme),
            address_transaction_time(address.latest_txid.as_deref(), &state.store, theme)
        );
        (balances, activity)
    } else {
        ("loading".to_string(), "loading chain activity".to_string())
    };
    let masked_hint = if state.unmask_identity_addresses {
        "raw i-addresses"
    } else {
        "resolved names"
    };

    let mut lines = vec![
        Line::from(vec![
            Span::styled(format!("{} ", theme.glyph("👤", "ID")), theme.focused()),
            theme.value("identity name", format!("{name}@"), theme.focused()),
            Span::raw("  "),
            theme.value("identity status", status, status_style),
        ]),
        Line::from(theme.value("i-address", own_address, theme.link())),
        identity_property_pair(
            theme,
            "Version",
            version_number,
            "Fully qualified",
            state
                .store
                .identity_name(own_address)
                .unwrap_or(&version.fully_qualified_name)
                .to_string(),
            width,
        ),
    ];
    if identity_has_currency(version) {
        lines.push(Line::from(vec![
            Span::styled(" Currency defined ", theme.status_badge()),
            Span::raw("  "),
            Span::styled("c opens currency definition and activity", theme.link()),
        ]));
    }
    let tokenized = token_control::enabled(version);
    if tokenized {
        lines.push(Line::from(Span::styled(
            " Tokenized Control ",
            theme.status_badge(),
        )));
    } else if identity["flags"].as_u64() == Some(0) {
        lines.push(Line::from(Span::styled("No flags set", theme.muted())));
    }
    let offer_labels = identity_offer_labels(state, own_address);
    if !offer_labels.is_empty() {
        lines.insert(
            1,
            Line::from(
                offer_labels
                    .iter()
                    .map(|label| Span::styled(format!(" {label} "), theme.status_badge()))
                    .collect::<Vec<_>>(),
            ),
        );
    }
    lines.push(Line::from(Span::styled(
        "Primary addresses:",
        theme.muted(),
    )));
    let mut selected_line = selected_primary_address
        .filter(|&index| index < primary_addresses.len())
        .map(|index| lines.len() + index);
    if primary_addresses.is_empty() {
        lines.push(Line::from(Span::styled("none", theme.muted())));
    } else {
        lines.extend(
            primary_addresses
                .iter()
                .enumerate()
                .map(|(index, address)| {
                    let selected = selected_primary_address == Some(index);
                    Line::from(theme.value("primary address", address.clone(), theme.link())).style(
                        if selected {
                            theme.focused()
                        } else {
                            theme.base()
                        },
                    )
                }),
        );
    }
    if tokenized {
        let selected = selected_primary_address == Some(primary_addresses.len());
        if selected {
            selected_line = Some(lines.len());
        }
        lines.push(identity_token_control_line(theme, state, version, selected));
    }
    lines.extend(vec![
        identity_property_pair(
            theme,
            "Minimum signatures",
            minimum_signatures,
            "Timelock",
            timelock,
            width,
        ),
        identity_property_pair_as(
            theme,
            authority("Parent", "parent"),
            authority("System", "systemid"),
            width,
        ),
        identity_property_pair_as(
            theme,
            authority("Revocation", "revocationauthority"),
            authority("Recovery", "recoveryauthority"),
            width,
        ),
        identity_property_line(theme, "Balances", balances),
        identity_property_line(theme, "Activity", activity),
        identity_property_line(
            theme,
            if state
                .store
                .identity_evidence
                .get(own_address)
                .is_some_and(|(_, report)| {
                    report.links.iter().any(|link| {
                        link.label == "Identity import transaction"
                            && registration
                                .raw_json
                                .get("txid")
                                .and_then(Value::as_str)
                                .is_some_and(|txid| {
                                    link.target
                                        == identity_evidence::EvidenceTarget::Transaction(
                                            txid.into(),
                                        )
                                })
                    })
                })
            {
                "Imported"
            } else {
                "Registered"
            },
            registration_label,
        ),
        identity_property_line(theme, "Updated", updated),
        identity_property_line(
            theme,
            "Viewing update",
            format_identity_height_time(version.block_height, version.block_time, theme),
        ),
        identity_property_pair(
            theme,
            "Versions",
            versions.len().to_string(),
            "Display",
            masked_hint.to_string(),
            width,
        ),
    ]);
    (lines, selected_line)
}

fn identity_authority_label(
    state: &AppState,
    identity: &Value,
    key: &str,
    own_address: &str,
) -> String {
    let Some(address) = identity.get(key).and_then(Value::as_str) else {
        return "unknown".to_string();
    };
    if address == own_address {
        "self".to_string()
    } else {
        let display =
            identity_display_value(&state.store, address, state.unmask_identity_addresses);
        if display == address {
            short_id(address)
        } else {
            display
        }
    }
}

fn identity_primary_addresses(version: &IdentityVersionRecord) -> Vec<String> {
    version
        .raw_json
        .get("identity")
        .and_then(|identity| identity.get("primaryaddresses"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect()
}

fn identity_property_line(theme: &Theme, label: &str, value: String) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{label}: "), theme.muted()),
        theme.value(label, value, theme.base()),
    ])
}

fn identity_property_pair(
    theme: &Theme,
    left_label: &str,
    left_value: String,
    right_label: &str,
    right_value: String,
    width: u16,
) -> Line<'static> {
    identity_property_pair_as(
        theme,
        (left_label, left_value, None),
        (right_label, right_value, None),
        width,
    )
}

fn identity_property_pair_as(
    theme: &Theme,
    (left_label, left_value, left_raw): (&str, String, Option<&str>),
    (right_label, right_value, right_raw): (&str, String, Option<&str>),
    width: u16,
) -> Line<'static> {
    const GUTTER: &str = "        ";

    let fixed_width =
        left_label.chars().count() + right_label.chars().count() + 4 + GUTTER.chars().count();
    let available = usize::from(width).saturating_sub(fixed_width);
    let left_len = left_value.chars().count();
    let right_len = right_value.chars().count();
    let mut left_budget = left_len.min(available / 2);
    let mut right_budget = right_len.min(available.saturating_sub(left_budget));
    let remaining = available.saturating_sub(left_budget + right_budget);
    if left_budget < left_len {
        left_budget += remaining;
    } else {
        right_budget += remaining;
    }
    let left_display = truncate_middle(&left_value, left_budget);
    let right_display = truncate_middle(&right_value, right_budget);

    Line::from(vec![
        Span::styled(format!("{left_label}: "), theme.muted()),
        theme.value_as(
            left_label,
            left_display,
            left_raw.unwrap_or(&left_value),
            theme.base(),
        ),
        Span::raw(GUTTER),
        Span::styled(format!("{right_label}: "), theme.muted()),
        theme.value_as(
            right_label,
            right_display,
            right_raw.unwrap_or(&right_value),
            theme.base(),
        ),
    ])
}

fn identity_timelock_label(identity: &Value, chain_height: Option<u64>) -> String {
    const VERSION_VAULT: u64 = 2;
    const FLAG_LOCKED: u64 = 0x2;
    const FLAG_REVOKED: u64 = 0x8000;

    let version = identity
        .get("version")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let flags = identity
        .get("flags")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let unlock_after = identity
        .get("timelock")
        .and_then(Value::as_u64)
        .unwrap_or_default();

    if version < VERSION_VAULT || flags & FLAG_REVOKED != 0 {
        return "unlocked".to_string();
    }
    if flags & FLAG_LOCKED != 0 {
        let unit = if unlock_after == 1 { "block" } else { "blocks" };
        return format!("locked ({unlock_after} {unit} unlock delay)");
    }
    if unlock_after > 0 && chain_height.is_none_or(|height| unlock_after >= height) {
        return format!("locked (unlocks at height {unlock_after})");
    }
    "unlocked".to_string()
}

fn format_identity_height_time(height: u64, timestamp: u64, theme: &Theme) -> String {
    if theme.heights.borrow().dates {
        return theme.format_height_at(height, Some(timestamp));
    }
    if timestamp == 0 {
        format!("height {height}, time unavailable")
    } else {
        format!("height {height}, {}", theme.format_time(timestamp))
    }
}

fn render_address_activity(
    frame: &mut Frame<'_>,
    config: &Config,
    theme: &Theme,
    state: &AppState,
    address: &str,
    selected_tx: Option<usize>,
    area: Rect,
) {
    let Some(record) = state.store.address(address) else {
        let status = address_loading_status(state, address, "transaction activity");
        frame.render_widget(
            Paragraph::new(status)
                .style(theme.warning())
                .wrap(Wrap { trim: true }),
            area,
        );
        return;
    };
    let listed_count = record.transaction_ids.len();
    let content = scroll_view::content_area(area, listed_count > 1);
    let loaded_count = record
        .transaction_ids
        .iter()
        .filter(|txid| state.store.contains_transaction(txid))
        .count();
    let mut tile_heights = vec![None; listed_count];
    let available_height = area.height.saturating_sub(SECTION_HEADING_HEIGHT);
    let (first, last) = visible_range_by_height(
        listed_count,
        selected_tx.unwrap_or_default(),
        available_height,
        |index| {
            *tile_heights[index].get_or_insert_with(|| {
                state
                    .store
                    .transaction(&record.transaction_ids[index])
                    .map(|transaction| {
                        address_transaction_tile_height(transaction, &state.store, content.width)
                    })
                    .unwrap_or(ADDRESS_TRANSACTION_TILE_MIN_HEIGHT)
            })
        },
    );
    let heading_label = if !record.history_complete && listed_count == 0 {
        format!("0 recent ({})", address_history_status(state, address))
    } else if !record.history_complete {
        format!(
            "{}-{} recent ({} of {} loaded; {})",
            first.saturating_add(1),
            last,
            loaded_count,
            listed_count,
            address_history_status(state, address)
        )
    } else if listed_count == 0 {
        "0 of 0".to_string()
    } else if loaded_count < record.transaction_count {
        format!(
            "{}-{} of {} ({} recent loaded)",
            first.saturating_add(1),
            last,
            record.transaction_count,
            loaded_count
        )
    } else {
        format!("{}-{} of {}", first + 1, last, loaded_count)
    };
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("Transactions ", theme.focused()),
            Span::styled(heading_label, theme.muted()),
        ]))
        .style(theme.base()),
        Rect { height: 1, ..area },
    );
    let inner = Rect {
        y: area.y.saturating_add(SECTION_HEADING_HEIGHT),
        height: area.height.saturating_sub(SECTION_HEADING_HEIGHT),
        ..area
    };
    if listed_count == 0 {
        let message = if record.history_complete {
            "No indexed transactions.".to_string()
        } else {
            format!(
                "No transactions in the recent range; {}.",
                address_history_status(state, address)
            )
        };
        frame.render_widget(
            Paragraph::new(message)
                .style(theme.muted())
                .wrap(Wrap { trim: true }),
            inner,
        );
        return;
    }
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints(
            tile_heights[first..last]
                .iter()
                .copied()
                .flatten()
                .map(Constraint::Length)
                .collect::<Vec<_>>(),
        )
        .split(Rect {
            width: content.width,
            ..inner
        });
    let native_currency = native_currency_name(config, &state.store);
    let tile_context = AddressTileRenderContext {
        theme,
        store: &state.store,
        viewed_address: address,
        native_currency: &native_currency,
        unmask_identity_addresses: state.unmask_identity_addresses,
    };
    for (row, tx_index) in rows.iter().zip(first..last) {
        let Some(txid) = record.transaction_ids.get(tx_index) else {
            continue;
        };
        if let Some(transaction) = state.store.transaction(txid) {
            render_address_transaction_tile(
                frame,
                &tile_context,
                transaction,
                selected_tx == Some(tx_index),
                *row,
            );
        } else {
            render_address_transaction_placeholder(
                frame,
                theme,
                state,
                txid,
                selected_tx == Some(tx_index),
                *row,
            );
        }
    }
    scroll_view::render(frame, theme, inner, listed_count, first, last - first);
}

fn render_address_transaction_placeholder(
    frame: &mut Frame<'_>,
    theme: &Theme,
    state: &AppState,
    txid: &str,
    selected: bool,
    area: Rect,
) {
    let area = render_tile_gutter(frame, theme, selected, area);
    let status = if state.loading_transactions.contains(txid) {
        "loading details"
    } else if state
        .transaction_retry_after
        .get(txid)
        .is_some_and(|retry_at| *retry_at > Instant::now())
    {
        "details unavailable; retry queued"
    } else {
        "details queued"
    };
    let line = justified_line(
        vec![
            Span::raw("txid "),
            theme.value_as("transaction ID", short_id(txid), txid, theme.link()),
        ],
        vec![Span::styled(
            format!("{} {status}", theme.glyph("○", "o")),
            theme.muted(),
        )],
        area.width,
    );
    frame.render_widget(tile_paragraph(theme, selected, vec![line]), area);
}

fn address_transaction_tile_height(
    transaction: &TransactionRecord,
    store: &ObjectStore,
    width: u16,
) -> u16 {
    let input_count = transaction
        .raw_json
        .get("vin")
        .and_then(Value::as_array)
        .map_or(0, Vec::len)
        + private_input_count(&transaction.raw_json);
    let output_count = transaction
        .raw_json
        .get("vout")
        .and_then(Value::as_array)
        .map_or(0, Vec::len)
        + private_output_count(&transaction.raw_json);
    address_transaction_tile_height_for_counts(width, input_count, output_count).saturating_add(
        address_transaction_tags_height(transaction, store, tile_content_width(width)),
    )
}

fn address_transaction_tags_height(
    transaction: &TransactionRecord,
    store: &ObjectStore,
    width: u16,
) -> u16 {
    let tags = display_transaction_tags(&transaction.summary, store);
    if tags.is_empty() {
        return 0;
    }
    let spans = tags
        .iter()
        .enumerate()
        .flat_map(|(index, tag)| {
            let mut spans = Vec::new();
            if index > 0 {
                spans.push(Span::raw(" "));
            }
            spans.push(Span::raw(format!(" {tag} ")));
            spans
        })
        .collect::<Vec<_>>();
    wrapped_spans_height(&spans, width.max(1))
}

fn address_transaction_tile_height_for_counts(
    width: u16,
    input_count: usize,
    output_count: usize,
) -> u16 {
    let inputs = address_flow_column_height(input_count);
    let outputs = address_flow_column_height(output_count);
    let header_and_gutter = 2;
    if width >= 160 {
        header_and_gutter + inputs.max(outputs)
    } else {
        header_and_gutter + inputs + outputs
    }
}

fn address_flow_column_height(count: usize) -> u16 {
    1 + u16::try_from(count.min(ADDRESS_FLOW_LIMIT)).unwrap_or(ADDRESS_FLOW_LIMIT as u16)
        + u16::from(count > ADDRESS_FLOW_LIMIT)
}

fn variable_height_visible_range(
    heights: &[u16],
    selected: usize,
    available_height: u16,
) -> (usize, usize) {
    visible_range_by_height(heights.len(), selected, available_height, |i| heights[i])
}

fn visible_range_by_height(
    count: usize,
    selected: usize,
    available_height: u16,
    mut height_at: impl FnMut(usize) -> u16,
) -> (usize, usize) {
    if count == 0 {
        return (0, 0);
    }
    let selected = selected.min(count - 1);
    let mut first = selected;
    let mut last = selected + 1;
    let mut used = height_at(selected);
    let mut before_budget = available_height.saturating_sub(used) / 2;
    while first > 0 {
        let height = height_at(first - 1);
        if height > before_budget {
            break;
        }
        first -= 1;
        used = used.saturating_add(height);
        before_budget -= height;
    }
    while last < count {
        let height = height_at(last);
        if used.saturating_add(height) > available_height {
            break;
        }
        used = used.saturating_add(height);
        last += 1;
    }
    while first > 0 {
        let height = height_at(first - 1);
        if used.saturating_add(height) > available_height {
            break;
        }
        first -= 1;
        used = used.saturating_add(height);
    }
    (first, last)
}

#[derive(Debug)]
struct AddressTileFlow {
    index: u64,
    addresses: Vec<String>,
    amounts: BTreeMap<String, i128>,
    coinbase: bool,
    outpoint: Option<OutPoint>,
    private: bool,
}

struct AddressTileRenderContext<'a> {
    theme: &'a Theme,
    store: &'a ObjectStore,
    viewed_address: &'a str,
    native_currency: &'a str,
    unmask_identity_addresses: bool,
}

fn render_address_transaction_tile(
    frame: &mut Frame<'_>,
    context: &AddressTileRenderContext<'_>,
    transaction: &TransactionRecord,
    selected: bool,
    area: Rect,
) {
    let theme = context.theme;
    let wide = area.width >= 160;
    let area = render_tile_gutter(frame, theme, selected, area);
    let header_area = Rect { height: 1, ..area };
    let header = justified_line(
        vec![
            Span::raw("txid "),
            theme.value_as(
                "transaction ID",
                short_id(&transaction.summary.txid),
                transaction.summary.txid.as_str(),
                theme.link(),
            ),
        ],
        vec![
            Span::styled("time ", theme.muted()),
            theme.value(
                "timestamp",
                theme.format_time(transaction.block_time),
                Style::default(),
            ),
        ],
        header_area.width,
    );
    frame.render_widget(tile_paragraph(theme, selected, vec![header]), header_area);

    let tags = display_transaction_tags(&transaction.summary, context.store);
    let tag_height = address_transaction_tags_height(transaction, context.store, area.width);
    if tag_height > 0 {
        frame.render_widget(
            tile_paragraph(
                theme,
                selected,
                vec![Line::from(transaction_tag_spans(theme, &tags))],
            )
            .wrap(Wrap { trim: true }),
            Rect {
                y: area.y.saturating_add(1),
                height: tag_height,
                ..area
            },
        );
    }
    let heading_height = 1u16.saturating_add(tag_height);
    let body = Rect {
        y: area.y.saturating_add(heading_height),
        height: area.height.saturating_sub(heading_height),
        ..area
    };
    let (inputs, outputs) = address_tile_flows(
        transaction,
        context.store,
        context.native_currency,
        context.viewed_address,
    );
    if wide {
        let columns = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(body);
        render_address_flow_column(
            frame, context, "INPUTS", &inputs, false, selected, columns[0],
        );
        render_address_flow_column(
            frame, context, "OUTPUTS", &outputs, true, selected, columns[1],
        );
    } else {
        let sections = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(address_flow_column_height(inputs.len())),
                Constraint::Length(address_flow_column_height(outputs.len())),
            ])
            .split(body);
        render_address_flow_column(
            frame,
            context,
            "INPUTS",
            &inputs,
            false,
            selected,
            sections[0],
        );
        render_address_flow_column(
            frame,
            context,
            "OUTPUTS",
            &outputs,
            true,
            selected,
            sections[1],
        );
    }
}

fn render_address_flow_column(
    frame: &mut Frame<'_>,
    context: &AddressTileRenderContext<'_>,
    title: &str,
    flows: &[AddressTileFlow],
    outputs: bool,
    selected: bool,
    area: Rect,
) {
    let theme = context.theme;
    let title_style = if outputs {
        theme.positive()
    } else {
        theme.warning()
    };
    let mut lines = vec![Line::from(Span::styled(title.to_string(), title_style))];
    lines.extend(flows.iter().take(ADDRESS_FLOW_LIMIT).map(|flow| {
        address_flow_line(
            theme,
            context.store,
            context.viewed_address,
            context.unmask_identity_addresses,
            flow,
            outputs,
        )
    }));
    if flows.len() > ADDRESS_FLOW_LIMIT {
        lines.push(Line::from(Span::styled(
            format!(
                "{} {} more not shown",
                theme.glyph("…", "..."),
                flows.len() - ADDRESS_FLOW_LIMIT
            ),
            theme.warning(),
        )));
    }
    frame.render_widget(tile_paragraph(theme, selected, lines), area);
}

fn address_flow_line(
    theme: &Theme,
    store: &ObjectStore,
    viewed_address: &str,
    unmask_identity_addresses: bool,
    flow: &AddressTileFlow,
    output: bool,
) -> Line<'static> {
    let mut spans = Vec::new();
    if output {
        let (glyph, status_style) = if flow.private {
            (theme.glyph("◆", "#"), private_flow_style(theme))
        } else {
            match flow
                .outpoint
                .as_ref()
                .and_then(|outpoint| store.output_unspent.get(outpoint))
            {
                Some(true) => (theme.glyph("●", "+"), theme.positive()),
                Some(false) => (theme.glyph("×", "x"), theme.negative()),
                None => (theme.glyph("…", "?"), theme.muted()),
            }
        };
        spans.push(Span::styled(format!("{glyph} "), status_style));
    }
    spans.push(Span::styled(format!("#{} ", flow.index), theme.muted()));
    if flow.coinbase {
        spans.push(Span::styled(
            TransactionTag::Coinbase.label(),
            theme.warning(),
        ));
    } else if flow.private {
        spans.push(Span::styled(
            if output {
                "Private Output"
            } else {
                "Private Input"
            },
            private_flow_style(theme),
        ));
    } else if flow.addresses.is_empty() {
        spans.push(Span::styled("address unavailable", theme.muted()));
    } else {
        let mut addresses = flow.addresses.clone();
        addresses.sort_by_key(|address| address != viewed_address);
        for (index, address) in addresses.iter().enumerate() {
            if index > 0 {
                spans.push(Span::raw(","));
            }
            spans.push(theme.value(
                "address",
                identity_display_value(store, address, unmask_identity_addresses),
                if address == viewed_address {
                    theme.focused()
                } else {
                    theme.link()
                },
            ));
        }
    }
    spans.push(Span::raw("  "));
    if flow.private {
        spans.push(Span::styled("amount hidden", theme.muted()));
    } else if flow.amounts.is_empty() {
        spans.push(Span::styled("value unavailable", theme.muted()));
    } else {
        for (index, (currency, amount)) in flow.amounts.iter().enumerate() {
            if index > 0 {
                spans.push(Span::raw(", "));
            }
            spans.push(theme.value(
                "amount",
                currency_amount_label(currency_display_value(store, currency), *amount),
                Style::default(),
            ));
        }
    }
    Line::from(spans)
}

fn address_tile_flows(
    transaction: &TransactionRecord,
    store: &ObjectStore,
    native_currency: &str,
    viewed_address: &str,
) -> (Vec<AddressTileFlow>, Vec<AddressTileFlow>) {
    let mut inputs = transaction
        .raw_json
        .get("vin")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
        .map(|(index, input)| {
            let source = resolved_prevout(input, store).unwrap_or(input);
            AddressTileFlow {
                index: index as u64,
                addresses: addresses_in(source),
                amounts: output_currency_amounts(source, native_currency),
                coinbase: input.get("coinbase").is_some(),
                outpoint: None,
                private: false,
            }
        })
        .collect::<Vec<_>>();
    let transparent_input_count = inputs.len();
    inputs.extend(
        (0..private_input_count(&transaction.raw_json)).map(|offset| AddressTileFlow {
            index: u64::try_from(transparent_input_count + offset).unwrap_or(u64::MAX),
            addresses: Vec::new(),
            amounts: BTreeMap::new(),
            coinbase: false,
            outpoint: None,
            private: true,
        }),
    );
    let mut outputs = transaction
        .raw_json
        .get("vout")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
        .map(|(fallback_index, output)| {
            let index = output
                .get("n")
                .and_then(Value::as_u64)
                .unwrap_or(fallback_index as u64);
            AddressTileFlow {
                index,
                addresses: addresses_in(output),
                amounts: output_currency_amounts(output, native_currency),
                coinbase: false,
                outpoint: Some(OutPoint {
                    txid: transaction.summary.txid.clone(),
                    index,
                }),
                private: false,
            }
        })
        .collect::<Vec<_>>();
    let transparent_output_count = outputs.len();
    outputs.extend(
        (0..private_output_count(&transaction.raw_json)).map(|offset| AddressTileFlow {
            index: u64::try_from(transparent_output_count + offset).unwrap_or(u64::MAX),
            addresses: Vec::new(),
            amounts: BTreeMap::new(),
            coinbase: false,
            outpoint: None,
            private: true,
        }),
    );
    inputs.sort_by_key(|flow| {
        if flow
            .addresses
            .iter()
            .any(|address| address == viewed_address)
        {
            0
        } else if flow.private {
            1
        } else {
            2
        }
    });
    outputs.sort_by_key(|flow| {
        if flow
            .addresses
            .iter()
            .any(|address| address == viewed_address)
        {
            0
        } else if flow.private {
            1
        } else {
            2
        }
    });
    (inputs, outputs)
}

fn address_balance_label(
    state: &AppState,
    record: &AddressRecord,
    native_currency: &str,
) -> String {
    let Some(response) = record.balance_response.as_ref() else {
        return address_balance_status(state, &record.address);
    };
    let balances = address_balance_amounts(response, &state.store, native_currency);
    if balances.is_empty() {
        return "none".to_string();
    }
    balances
        .into_iter()
        .map(|(currency, amount)| {
            currency_amount_label(currency_display_value(&state.store, &currency), amount)
        })
        .collect::<Vec<_>>()
        .join("  ")
}

fn address_balance_amounts(
    response: &Value,
    store: &ObjectStore,
    native_currency: &str,
) -> BTreeMap<String, i128> {
    let mut balances = BTreeMap::new();
    let names = response.get("currencynames").and_then(Value::as_object);
    let received: BTreeMap<_, _> = response
        .get("currencyreceived")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
        .filter_map(|(id, value)| decimal_value_to_zat(value).map(|amount| (id.clone(), amount)))
        .collect();
    let mut amounts: BTreeMap<_, _> = response
        .get("currencybalance")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
        .filter_map(|(id, value)| decimal_value_to_zat(value).map(|amount| (id.clone(), amount)))
        .collect();
    // The daemon can omit spent-down currencies from its sparse balance map.
    for (id, amount) in &received {
        if *amount > 0 {
            amounts.entry(id.clone()).or_insert(0);
        }
    }
    let native_id = store.chain.chain_id.as_deref();
    let native_balance = response.get("balance").and_then(json_i128);
    let native_received = response.get("received").and_then(json_i128).unwrap_or(0);
    let currency_name = |id: &str| {
        if native_id == Some(id) {
            return native_currency.to_string();
        }
        names
            .and_then(|names| names.get(id))
            .and_then(Value::as_str)
            .or_else(|| store.currency_name(id))
            .unwrap_or(id)
            .to_string()
    };
    for (id, amount) in &amounts {
        if *amount != 0 || received.get(id).is_some_and(|value| *value > 0) {
            balances.insert(currency_name(id), *amount);
        }
    }
    let has_native = balances
        .keys()
        .any(|currency| currency.eq_ignore_ascii_case(native_currency));
    if let Some(native_balance) = native_balance
        && !has_native
        && (native_balance != 0 || native_received > 0)
    {
        // Older responses can omit names. Infer only one unambiguous native
        // entry, and never relabel an unrelated zero reserve balance.
        if native_id.is_none() {
            let candidates: Vec<_> = amounts
                .iter()
                .filter(|(id, amount)| {
                    **amount == native_balance
                        && currency_name(id) == **id
                        && (native_balance != 0 || received.get(*id) == Some(&native_received))
                })
                .map(|(id, _)| id)
                .collect();
            if candidates.len() == 1 {
                balances.remove(candidates[0]);
            }
        }
        balances.insert(native_currency.to_string(), native_balance);
    }
    balances
}

fn output_currency_amounts(output: &Value, native_currency: &str) -> BTreeMap<String, i128> {
    let mut amounts = BTreeMap::new();
    let native =
        native_value_zat(output).or_else(|| output.get("value").and_then(decimal_value_to_zat));
    if let Some(native) = native {
        amounts.insert(native_currency.to_string(), native);
    }
    collect_reserve_currency_amounts(output, &mut amounts);
    amounts
}

fn collect_reserve_currency_amounts(value: &Value, amounts: &mut BTreeMap<String, i128>) {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                let normalized = key.to_ascii_lowercase().replace('_', "");
                if matches!(
                    normalized.as_str(),
                    "reservevalues" | "currencyvalues" | "reserveamounts"
                ) && let Some(values) = child.as_object()
                {
                    for (currency, amount) in values {
                        if let Some(amount) = decimal_value_to_zat(amount) {
                            *amounts.entry(currency.clone()).or_default() += amount;
                        }
                    }
                } else {
                    collect_reserve_currency_amounts(child, amounts);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_reserve_currency_amounts(item, amounts);
            }
        }
        _ => {}
    }
}

fn decimal_value_to_zat(value: &Value) -> Option<i128> {
    if let Some(value) = value.as_str() {
        return decimal_string_to_zat(value);
    }
    if value.is_number() {
        return decimal_string_to_zat(&value.to_string());
    }
    value
        .get("valueZat")
        .and_then(json_i128)
        .or_else(|| value.get("value").and_then(decimal_value_to_zat))
        .or_else(|| value.get("amount").and_then(decimal_value_to_zat))
}

fn decimal_string_to_zat(value: &str) -> Option<i128> {
    let value = value.trim();
    let (mantissa, exponent) = value
        .split_once(['e', 'E'])
        .map_or(Some((value, 0)), |(m, e)| Some((m, e.parse::<i32>().ok()?)))?;
    let (negative, unsigned) = mantissa
        .strip_prefix('-')
        .map_or((false, mantissa), |v| (true, v));
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if whole.is_empty()
        || !whole.chars().all(|ch| ch.is_ascii_digit())
        || !fraction.chars().all(|ch| ch.is_ascii_digit())
    {
        return None;
    }
    let digits = format!("{whole}{fraction}").parse::<i128>().ok()?;
    let scale = 8_i32
        .checked_add(exponent)?
        .checked_sub(i32::try_from(fraction.len()).ok()?)?;
    let amount = if scale >= 0 {
        digits.checked_mul(10_i128.checked_pow(u32::try_from(scale).ok()?)?)?
    } else {
        let divisor = 10_i128.checked_pow(u32::try_from(scale.checked_neg()?).ok()?)?;
        if digits % divisor != 0 {
            return None;
        }
        digits / divisor
    };
    Some(if negative { -amount } else { amount })
}

fn collect_currency_name_hints(value: &Value, hints: &mut HashMap<String, String>) {
    match value {
        Value::Object(fields) => {
            if let Some(names) = fields.get("currencynames").and_then(Value::as_object) {
                for (id, name) in names {
                    if let Some(name) = name.as_str().filter(|name| !name.is_empty() && *name != id)
                    {
                        hints.insert(id.clone(), name.to_string());
                    }
                }
            }
            if let Some(names) = fields.get("reserve_balance").and_then(Value::as_object)
                && names.len() == 1
                && let Some(values) = fields
                    .get("reserveoutput")
                    .or_else(|| fields.get("reservedeposit"))
                    .and_then(|output| output.get("currencyvalues"))
                    .and_then(Value::as_object)
                && values.len() == 1
            {
                let (id, amount) = values.iter().next().unwrap();
                let (name, named_amount) = names.iter().next().unwrap();
                if let Some(amount) = decimal_value_to_zat(amount)
                    && Some(amount) == decimal_value_to_zat(named_amount)
                    && !name.is_empty()
                    && name != id
                {
                    hints.insert(id.clone(), name.clone());
                }
            }
            for child in fields.values() {
                collect_currency_name_hints(child, hints);
            }
        }
        Value::Array(items) => {
            for child in items {
                collect_currency_name_hints(child, hints);
            }
        }
        _ => {}
    }
}

fn currency_display_value<'a>(store: &'a ObjectStore, id: &'a str) -> &'a str {
    store
        .currency_name(id)
        .or_else(|| store.identity_name(id))
        .unwrap_or(id)
}

fn currency_text_display(store: &ObjectStore, text: &str) -> String {
    let mut display = text.to_string();
    for id in text
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|id| looks_like_identity_address(id))
        .collect::<BTreeSet<_>>()
    {
        if let Some(name) = store.currency_name(id) {
            display = display.replace(id, name);
        }
    }
    display
}

fn currency_amount_label(currency: &str, amount: i128) -> String {
    format!("{} {currency}", format_compact_zat(amount))
}

fn format_compact_zat(zat: i128) -> String {
    let mut value = format_native_zat(zat);
    while value.contains('.') && value.ends_with('0') {
        value.pop();
    }
    if value.ends_with('.') {
        value.pop();
    }
    value
}

fn render_missing_object(frame: &mut Frame<'_>, theme: &Theme, title: &str, area: Rect) {
    let missing = Paragraph::new(vec![
        Line::from(vec![Span::styled(title.to_string(), theme.focused())]),
        Line::from("Object is not loaded in the session cache yet."),
    ])
    .style(theme.negative());
    frame.render_widget(missing, area);
}

#[derive(Debug, Clone)]
struct JsonEntry {
    path: String,
    pointer: String,
    depth: usize,
    label: String,
    value: String,
    kind: JsonEntryKind,
    collapsible: bool,
    expanded: bool,
    scalar_value: Option<String>,
    link: Option<JsonLink>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum JsonLink {
    BlockHash(String),
    Txid(String),
    Identity(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonEntryKind {
    Object,
    Array,
    String,
    Number,
    Bool,
    Null,
}

impl JsonState {
    fn viewport_start(&self, height: usize, rows: usize) -> usize {
        let height = height.max(1);
        let centered = self.cursor.saturating_sub(height / 2);
        let previous_cursor = self.last_drawn_cursor.replace(self.cursor);
        let mut start = self.scroll.get();
        if self.cursor > previous_cursor {
            start = start.max(centered);
        } else if self.cursor < previous_cursor {
            start = start.min(centered);
        }
        // Keep selection visible after resize/collapse, and never scroll past the end.
        start = start
            .min(self.cursor)
            .max(self.cursor.saturating_sub(height - 1));
        start = start.min(rows.saturating_sub(height));
        self.scroll.set(start);
        start
    }

    fn new() -> Self {
        let mut expanded = BTreeSet::new();
        expanded.insert("$".to_string());
        Self {
            initial_focus: None,
            expanded,
            cursor: 0,
            scroll: Cell::new(0),
            last_drawn_cursor: Cell::new(0),
            scalar_scroll: 0,
        }
    }

    fn visible_entries(&self, root: &Value) -> Vec<JsonEntry> {
        let mut entries = Vec::new();
        collect_json_entries(
            root,
            "$".to_string(),
            String::new(),
            "$".to_string(),
            0,
            &self.expanded,
            &mut entries,
        );
        entries
    }

    fn current_path(&self, root: &Value) -> String {
        self.visible_entries(root)
            .get(self.cursor)
            .map(|entry| entry.path.clone())
            .unwrap_or_else(|| "$".to_string())
    }
}

impl SavePrompt {
    fn filename(&self) -> String {
        if self.basename.ends_with(&self.extension) {
            self.basename.clone()
        } else {
            format!("{}{}", self.basename, self.extension)
        }
    }
}

fn render_save_prompt(frame: &mut Frame<'_>, theme: &Theme, prompt: &SavePrompt, area: Rect) {
    let width = area.width.clamp(24, 72);
    let height = 7;
    let x = area.x + area.width.saturating_sub(width) / 2;
    let y = area.y + area.height.saturating_sub(height) / 2;
    let popup = Rect {
        x,
        y,
        width,
        height,
    };
    frame.render_widget(Clear, popup);

    let filename_line = if prompt.selection_active {
        Line::from(vec![
            Span::styled("filename ", theme.muted()),
            Span::styled(prompt.basename.clone(), theme.focused()),
            Span::raw(prompt.extension.clone()),
        ])
    } else {
        Line::from(vec![
            Span::styled("filename ", theme.muted()),
            Span::styled(prompt.filename(), theme.focused()),
        ])
    };

    let mut lines = vec![
        Line::from("Save JSON"),
        filename_line,
        Line::from("Enter saves, Esc cancels. Typing replaces the selected basename."),
    ];
    if let Some(status) = &prompt.status {
        lines.push(Line::from(vec![
            Span::styled("status ", theme.muted()),
            Span::styled(status.clone(), theme.negative()),
        ]));
    }

    let body = Paragraph::new(lines)
        .style(theme.base())
        .block(
            Block::default()
                .title(" Save ")
                .borders(Borders::ALL)
                .border_style(theme.focused()),
        )
        .wrap(Wrap { trim: true });
    frame.render_widget(body, popup);
}

fn render_cache_clear_prompt(
    frame: &mut Frame<'_>,
    theme: &Theme,
    prompt: &CacheClearPrompt,
    area: Rect,
) {
    let width = area.width.min(72);
    let height = area.height.min(if width < 50 { 10 } else { 8 });
    let popup = Rect {
        x: area.x + area.width.saturating_sub(width) / 2,
        y: area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    };
    frame.render_widget(Clear, popup);

    let lines = if prompt.clearing {
        vec![
            Line::from(Span::styled("Clearing cache...", theme.warning())),
            Line::from("The database is being compacted. This may take a moment."),
        ]
    } else {
        vec![
            Line::from(vec![
                Span::raw("Clear the "),
                Span::styled(format_data_size(prompt.size_bytes), theme.warning()),
                Span::raw(" local cache?"),
            ]),
            Line::from("Persistent RPC responses and currency data will be removed."),
            Line::from("Objects already loaded in this session remain available."),
            Line::from("Press y to clear. Press n or Esc to cancel."),
        ]
    };
    let body = Paragraph::new(lines)
        .style(theme.base())
        .block(
            Block::default()
                .title(" Clear Cache ")
                .borders(Borders::ALL)
                .border_style(theme.warning()),
        )
        .wrap(Wrap { trim: true });
    frame.render_widget(body, popup);
}

fn collect_json_entries(
    value: &Value,
    path: String,
    pointer: String,
    label: String,
    depth: usize,
    expanded: &BTreeSet<String>,
    entries: &mut Vec<JsonEntry>,
) {
    let structural = matches!(value, Value::Array(_) | Value::Object(_));
    let collapsible_string = is_collapsible_json_string(value);
    let collapsible = structural || collapsible_string;
    let is_expanded = expanded.contains(&path);
    entries.push(JsonEntry {
        path: path.clone(),
        pointer: pointer.clone(),
        depth,
        link: json_link(&label, value),
        label,
        value: json_value_preview(value, collapsible_string, is_expanded),
        kind: json_entry_kind(value),
        collapsible,
        expanded: is_expanded,
        scalar_value: if collapsible_string && is_expanded {
            value.as_str().map(str::to_string)
        } else {
            None
        },
    });

    if collapsible_string {
        return;
    }

    if !structural || !is_expanded {
        return;
    }

    match value {
        Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                collect_json_entries(
                    item,
                    format!("{path}[{index}]"),
                    format!("{pointer}/{index}"),
                    format!("[{index}]"),
                    depth + 1,
                    expanded,
                    entries,
                );
            }
        }
        Value::Object(map) => {
            for (key, item) in map {
                let child_path = format!("{path}.{}", escape_json_path_key(key));
                let pointer_key = key.replace('~', "~0").replace('/', "~1");
                collect_json_entries(
                    item,
                    child_path,
                    format!("{pointer}/{pointer_key}"),
                    key.clone(),
                    depth + 1,
                    expanded,
                    entries,
                );
            }
        }
        _ => {}
    }
}

fn render_json_view(
    frame: &mut Frame<'_>,
    theme: &Theme,
    document: &JsonDocument<'_>,
    json: &JsonState,
    store: &ObjectStore,
    unmask_identity_addresses: bool,
    area: Rect,
) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Min(3),
            Constraint::Length(1),
        ])
        .split(area);
    frame.render_widget(
        Paragraph::new(Line::from(vec![Span::styled(
            document.title.clone(),
            theme.focused(),
        )]))
        .style(theme.base()),
        layout[0],
    );

    let rows = json.visible_entries(document.root);
    if let Some(entry) = rows.get(json.cursor)
        && entry.expanded
        && let Some(scalar) = entry.scalar_value.as_deref()
    {
        let display_scalar = if entry.label.eq_ignore_ascii_case("friendlyname") {
            friendly_identity_name(scalar)
        } else {
            scalar.to_string()
        };
        let serialized = serde_json::to_string(&display_scalar)
            .unwrap_or_else(|_| format!("{display_scalar:?}"));
        scroll_view::paragraph(
            frame,
            theme,
            Paragraph::new(Line::from(theme.copy_values.borrow_mut().span(
                Span::styled(serialized, theme.positive()),
                crate::copy_values::Target::Json {
                    row: json.cursor,
                    pointer: entry.pointer.clone(),
                },
            )))
            .style(theme.base())
            .wrap(Wrap { trim: false }),
            layout[1],
            json.scalar_scroll,
        );
        let footer = Paragraph::new(vec![Line::from(vec![
            Span::styled("path ", theme.muted()),
            Span::styled(
                truncate_middle(&entry.path, usize::from(area.width.saturating_sub(5))),
                theme.focused(),
            ),
        ])])
        .style(theme.base())
        .wrap(Wrap { trim: true });
        frame.render_widget(footer, layout[2]);
        return;
    }

    let visible_height = usize::from(layout[1].height).max(1);
    let scroll = json.viewport_start(visible_height, rows.len());
    let end = (scroll + visible_height).min(rows.len());
    let mut lines = Vec::new();

    for (index, entry) in rows.iter().enumerate().take(end).skip(scroll) {
        let indicator = if entry.collapsible {
            if entry.expanded { "▾" } else { "▸" }
        } else {
            " "
        };
        let indent = "  ".repeat(entry.depth);
        let style = json_style(theme, entry.kind, index == json.cursor);
        lines.push(Line::from(vec![
            theme.selection_marker(index == json.cursor),
            Span::raw(indent),
            Span::styled(indicator, theme.muted()),
            Span::raw(" "),
            Span::styled(
                identity_display_value(store, &entry.label, unmask_identity_addresses),
                theme.link(),
            ),
            Span::styled(": ", theme.muted()),
            theme.copy_values.borrow_mut().span(
                Span::styled(
                    json_dates::display(theme, document.root, &entry.pointer, store)
                        .unwrap_or_else(|| {
                            json_entry_display_value(entry, store, unmask_identity_addresses)
                        }),
                    style,
                ),
                crate::copy_values::Target::Json {
                    row: index,
                    pointer: entry.pointer.clone(),
                },
            ),
        ]));
    }

    if lines.is_empty() {
        lines.push(Line::from("empty JSON"));
    }

    let body = Paragraph::new(lines).style(theme.base());
    frame.render_widget(
        body,
        scroll_view::content_area(layout[1], rows.len() > visible_height),
    );
    scroll_view::render(frame, theme, layout[1], rows.len(), scroll, visible_height);

    let footer = Paragraph::new(vec![Line::from(vec![
        Span::styled("path ", theme.muted()),
        Span::styled(
            truncate_middle(
                &json.current_path(document.root),
                usize::from(area.width.saturating_sub(5)),
            ),
            theme.focused(),
        ),
    ])])
    .style(theme.base())
    .wrap(Wrap { trim: true });
    frame.render_widget(footer, layout[2]);
}

fn json_style(theme: &Theme, kind: JsonEntryKind, selected: bool) -> Style {
    if selected {
        return theme.focused();
    }
    match kind {
        JsonEntryKind::Object | JsonEntryKind::Array => theme.warning(),
        JsonEntryKind::String => theme.positive(),
        JsonEntryKind::Number => theme.base().fg(theme.json_number),
        JsonEntryKind::Bool => theme.focused(),
        JsonEntryKind::Null => theme.muted(),
    }
}

fn json_entry_display_value(
    entry: &JsonEntry,
    store: &ObjectStore,
    unmask_identity_addresses: bool,
) -> String {
    // These are explicit identity data, not references to substitute through
    // the alias cache. In particular, the primary address must remain usable.
    if entry.label.eq_ignore_ascii_case("identityaddress")
        || entry.label.eq_ignore_ascii_case("fullyqualifiedname")
    {
        return entry.value.clone();
    }
    if entry.label.eq_ignore_ascii_case("friendlyname") {
        let name = match &entry.link {
            Some(JsonLink::Identity(name)) => Some(name.clone()),
            _ => serde_json::from_str::<String>(&entry.value).ok(),
        };
        return name
            .map(|name| format!("{:?}", friendly_identity_name(&name)))
            .unwrap_or_else(|| entry.value.clone());
    }
    match &entry.link {
        Some(JsonLink::Identity(identity)) if !unmask_identity_addresses => store
            .currency_name(identity)
            .or_else(|| store.identity_name(identity))
            .or_else(|| {
                store
                    .identity_address_for_query(identity)
                    .and_then(|address| store.identity_name(address))
            })
            .map(|name| format!("{:?}", friendly_identity_name(name)))
            .unwrap_or_else(|| entry.value.clone()),
        _ => entry.value.clone(),
    }
}

/// VRSC is the assumed root in friendly names on every chain. Preserve all
/// intermediate namespaces, other roots, casing and the identity's @ marker.
fn friendly_identity_name(name: &str) -> String {
    let (body, marker) = name
        .strip_suffix('@')
        .map_or((name, ""), |body| (body, "@"));
    if body.len() > 5
        && body
            .get(body.len() - 5..)
            .is_some_and(|root| root.eq_ignore_ascii_case(".VRSC"))
    {
        format!("{}{marker}", &body[..body.len() - 5])
    } else {
        name.to_string()
    }
}

fn identity_display_value(
    store: &ObjectStore,
    value: &str,
    unmask_identity_addresses: bool,
) -> String {
    if unmask_identity_addresses {
        return value.to_string();
    }
    store
        .identity_name(value)
        .or_else(|| store.currency_name(value))
        .or_else(|| {
            store
                .identity_address_for_query(value)
                .and_then(|address| store.identity_name(address))
        })
        .map(friendly_identity_name)
        .unwrap_or_else(|| value.to_string())
}

fn json_entry_kind(value: &Value) -> JsonEntryKind {
    match value {
        Value::Object(_) => JsonEntryKind::Object,
        Value::Array(_) => JsonEntryKind::Array,
        Value::String(_) => JsonEntryKind::String,
        Value::Number(_) => JsonEntryKind::Number,
        Value::Bool(_) => JsonEntryKind::Bool,
        Value::Null => JsonEntryKind::Null,
    }
}

fn json_value_preview(value: &Value, collapsible_string: bool, expanded: bool) -> String {
    match value {
        Value::Object(map) => format!("{{{}}}", map.len()),
        Value::Array(items) => format!("[{}]", items.len()),
        Value::String(value) if collapsible_string && expanded => {
            format!(
                "expanded | {}",
                quoted_excerpt(value, JSON_STRING_PREVIEW_CHARS)
            )
        }
        Value::String(value) if collapsible_string => {
            format!(
                "collapsed, expand for full value | {}",
                quoted_excerpt(value, JSON_STRING_PREVIEW_CHARS)
            )
        }
        Value::String(value) => quoted_excerpt(value, JSON_STRING_PREVIEW_CHARS),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Null => "null".to_string(),
    }
}

fn is_collapsible_json_string(value: &Value) -> bool {
    let Some(value) = value.as_str() else {
        return false;
    };
    value.chars().count() > JSON_STRING_PREVIEW_CHARS
}

fn quoted_excerpt(value: &str, max_chars: usize) -> String {
    let char_count = value.chars().count();
    if char_count <= max_chars {
        return format!("{value:?}");
    }

    let excerpt = value.chars().take(max_chars).collect::<String>();
    format!("{excerpt:?}...({char_count} chars)")
}

fn json_link(label: &str, value: &Value) -> Option<JsonLink> {
    let value = value.as_str()?;
    if looks_like_identity_address(value) || value.ends_with('@') {
        return Some(JsonLink::Identity(value.to_string()));
    }
    if !is_hash_like(value) {
        return None;
    }

    let normalized_label = label.to_ascii_lowercase();
    if normalized_label == "txid" {
        Some(JsonLink::Txid(value.to_string()))
    } else if matches!(
        normalized_label.as_str(),
        "blockhash" | "previousblockhash" | "nextblockhash" | "bestblockhash"
    ) {
        Some(JsonLink::BlockHash(value.to_string()))
    } else {
        None
    }
}

fn looks_like_identity_address(value: &str) -> bool {
    value.starts_with('i')
        && value.len() == 34
        && value.chars().all(|ch| ch.is_ascii_alphanumeric())
}

fn is_hash_like(value: &str) -> bool {
    value.len() == 64 && value.chars().all(|ch| ch.is_ascii_hexdigit())
}

fn escape_json_path_key(key: &str) -> String {
    if key
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        key.to_string()
    } else {
        format!("{key:?}")
    }
}

fn chain_display_name(config: &Config, store: &ObjectStore) -> String {
    let id = config.chain.system_id.get().map(|id| id.as_ref());
    id.and_then(|id| store.identity_name(id))
        .or_else(|| id.and_then(|id| chain_qualified_currency_name(store, id)))
        .or_else(|| id.and_then(|id| store.currency_name(id)))
        .or(store.chain.chain_name.as_deref())
        .unwrap_or(&config.chain.name)
        .to_string()
}

fn chain_qualified_currency_name<'a>(store: &'a ObjectStore, id: &str) -> Option<&'a str> {
    store
        .currencies_by_id
        .get(id)
        .and_then(|record| record.definition_json.get("fullyqualifiedname"))
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
}

fn source_chain_display_names(configs: &[Config], sessions: &[ChainSession]) -> Vec<String> {
    configs
        .iter()
        .enumerate()
        .map(|(index, config)| {
            let id = config.chain.system_id.get().map(|id| id.as_ref());
            id.and_then(|id| {
                sessions
                    .iter()
                    .find_map(|session| session.state.store.identity_name(id))
            })
            .or_else(|| {
                id.and_then(|id| {
                    sessions
                        .iter()
                        .find_map(|session| chain_qualified_currency_name(&session.state.store, id))
                })
            })
            .or_else(|| {
                id.and_then(|id| {
                    sessions
                        .iter()
                        .find_map(|session| session.state.store.currency_name(id))
                })
            })
            .map(str::to_string)
            .unwrap_or_else(|| {
                sessions
                    .get(index)
                    .map(|session| chain_display_name(config, &session.state.store))
                    .unwrap_or_else(|| config.chain.name.clone())
            })
        })
        .collect()
}

fn chain_directory_label(config: &Config) -> String {
    config
        .daemon
        .config_file
        .as_ref()
        .and_then(|path| path.parent())
        .and_then(|parent| parent.file_name())
        .and_then(|name| name.to_str())
        .filter(|name| name.len() == 40 && name.bytes().all(|ch| ch.is_ascii_hexdigit()))
        .map(str::to_string)
        .or_else(|| {
            crate::world::chain_directory(
                &config.chain.name,
                crate::world::Network::from_testnet(config.chain.testnet),
            )
            .ok()
        })
        .unwrap_or_else(|| "unknown".into())
}

fn header_line(
    config: &Config,
    chain_name: &str,
    theme: &Theme,
    tip: &ChainTipStatus,
    now: i64,
    width: u16,
) -> Line<'static> {
    let chain = if config.chain.testnet {
        format!("{chain_name} testnet")
    } else {
        chain_name.to_string()
    };
    let icon = theme.glyph("🔦", "*");
    let api = if config
        .daemon
        .api_pool
        .endpoints
        .iter()
        .any(|endpoint| endpoint.control.state() != crate::sources::ConnectionState::Disabled)
    {
        "api on"
    } else {
        "api off"
    };
    let rpc = if tip.healthy { "rpc on" } else { "rpc off" };
    let peers = tip
        .connections
        .map(|connections| connections.to_string())
        .unwrap_or_else(|| "?".to_string());

    let full = tip_status_text(tip, now);
    let height = tip
        .height
        .map(|height| height.to_string())
        .unwrap_or_else(|| "?".into());
    let delta = match (tip.height, tip.longest_chain) {
        (Some(height), Some(longest)) => format!("Δ{}", i128::from(longest) - i128::from(height)),
        _ => "Δ?".into(),
    };
    let age = last_block_age_text(tip.last_block_time, now);
    let compact = format!("tip {height} ({delta}) | last {age}");
    let minimal = format!("tip {height} | last {age}");
    for name in [APP_TITLE, "Carbide"] {
        for status in [&full, &compact, &minimal] {
            let line = Line::from(format!(
                " {icon} {name} | {chain} | {status} | {rpc} | {api} | peers {peers} "
            ));
            if line.width() <= usize::from(width) {
                return line;
            }
        }
    }
    // At very small widths remove other whole fields before clipping the height.
    for text in [
        format!("{icon} Carbide | {chain} | {compact} | {rpc}"),
        format!("{icon} Carbide | {chain} | {minimal}"),
        format!("{chain} | {minimal} | {rpc}"),
        minimal,
        format!("tip {height}"),
        height,
    ] {
        let line = Line::from(text);
        if line.width() <= usize::from(width) {
            return line;
        }
    }
    Line::default()
}

fn tip_status_text(tip: &ChainTipStatus, now: i64) -> String {
    if !tip.healthy {
        return tip.label.clone();
    }

    let height = tip
        .height
        .map(|height| height.to_string())
        .unwrap_or_else(|| "?".to_string());
    let longest_chain = tip
        .longest_chain
        .map(|height| height.to_string())
        .unwrap_or_else(|| "?".to_string());
    let delta = match (tip.height, tip.longest_chain) {
        (Some(height), Some(longest_chain)) => {
            let delta = i128::from(longest_chain) - i128::from(height);
            format!("Δ{delta}")
        }
        _ => "Δ?".to_string(),
    };

    format!(
        "tip {height}/{longest_chain} ({delta}) | last {}",
        last_block_age_text(tip.last_block_time, now)
    )
}

fn last_block_age_text(last_block_time: Option<u64>, now: i64) -> String {
    let Some(last_block_time) = last_block_time else {
        return "unknown".to_string();
    };
    let Ok(last_block_time) = i64::try_from(last_block_time) else {
        return "unknown".to_string();
    };

    let elapsed = now.saturating_sub(last_block_time).max(0);
    if elapsed < 600 {
        return format!("{elapsed}s");
    }

    format!("{}:{:02}", elapsed / 60, elapsed % 60)
}

fn current_unix_timestamp() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
}

fn config_file_label(config: &Config) -> String {
    config
        .daemon
        .config_file
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "none".to_string())
}

fn setup_terminal() -> Result<Tui> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(
        stdout,
        EnterAlternateScreen,
        EnableMouseCapture,
        EnableBracketedPaste
    )?;
    set_terminal_title(&mut stdout)?;
    let backend = CrosstermBackend::new(stdout);
    Ok(Terminal::new(backend)?)
}

fn restore_terminal(terminal: &mut Tui) -> Result<()> {
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        DisableMouseCapture,
        DisableBracketedPaste,
        LeaveAlternateScreen
    )?;
    #[cfg(unix)]
    execute!(
        terminal.backend_mut(),
        crossterm::style::Print("\x1b[23;0t")
    )?;
    terminal.show_cursor()?;
    Ok(())
}

fn set_terminal_title(output: &mut impl io::Write) -> io::Result<()> {
    // Save the previous title on terminals that support the xterm title stack.
    #[cfg(unix)]
    execute!(
        output,
        crossterm::style::Print("\x1b[22;0t"),
        SetTitle(APP_TITLE)
    )?;
    #[cfg(not(unix))]
    execute!(output, SetTitle(APP_TITLE))?;
    Ok(())
}

fn resume_terminal(terminal: &mut Tui, mouse_capture: bool) -> Result<()> {
    enable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        EnterAlternateScreen,
        EnableBracketedPaste
    )?;
    set_terminal_title(terminal.backend_mut())?;
    input::set_mouse_capture(terminal.backend_mut(), mouse_capture)?;
    let size = terminal.size()?;
    terminal.resize(Rect::new(0, 0, size.width, size.height))?;
    terminal.hide_cursor()?;
    Ok(())
}

#[cfg(unix)]
fn suspend_terminal(terminal: &mut Tui, mouse_capture: bool) -> Result<()> {
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    restore_terminal(terminal)?;
    kill(Pid::from_raw(0), Signal::SIGTSTP)?;
    resume_terminal(terminal, mouse_capture)
}

#[cfg(not(unix))]
fn suspend_terminal(_terminal: &mut Tui, _mouse_capture: bool) -> Result<()> {
    Ok(())
}

fn session_relationships(
    sessions: &[ChainSession],
    active: usize,
) -> Vec<crate::relationships::Link> {
    let source = &sessions[active];
    let mut links = visible_relationships(&source.config, &source.state);
    let Screen::Transaction { txid, .. } = &source.state.screen else {
        return links;
    };
    let Some(system) = source.config.chain.system_id.get() else {
        return links;
    };
    if let Some(stored) = source.state.store.reverse_relationships.get(txid) {
        links.extend(stored.iter().cloned());
    }
    for session in sessions {
        let Some(carrier_system) = session.config.chain.system_id.get() else {
            continue;
        };
        for carrier in session.state.store.transactions_by_txid.values() {
            if carrier_system == system && carrier.summary.txid == *txid {
                continue;
            }
            links.extend(crate::relationships::reverse_links(
                system,
                txid,
                carrier_system,
                &carrier.raw_json,
            ));
        }
    }
    links.sort_by(|a, b| {
        (
            a.field.starts_with("reverse:"),
            a.carrier_vout,
            &a.field,
            &a.target.txid,
            a.target.vout,
        )
            .cmp(&(
                b.field.starts_with("reverse:"),
                b.carrier_vout,
                &b.field,
                &b.target.txid,
                b.target.vout,
            ))
    });
    let mut seen = HashSet::new();
    links.retain(|link| seen.insert((link.field.clone(), link.carrier_vout, link.target.clone())));
    links
}

fn visible_relationships(config: &Config, state: &AppState) -> Vec<crate::relationships::Link> {
    let Some(system) = config.chain.system_id.get() else {
        return vec![];
    };
    let Screen::Transaction { txid, .. } = &state.screen else {
        return vec![];
    };
    state
        .store
        .transaction(txid)
        .map(|tx| crate::relationships::transaction_links(system, &tx.raw_json))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observe_test_block(store: &mut ObjectStore, height: u64, depth: u64, order: u64) {
        store.chain.observe(RpcObservation {
            authorization: None,
            order,
            method: "getinfo".into(),
            params: vec![],
            result: json!({"blocks":height+depth-1,"longestchain":height+depth-1,"name":"VRSC"}),
        });
        store.chain.observe(RpcObservation {
            authorization: None, order: order+1, method: "getblockheader".into(), params: vec![], result: json!({"hash":"reward-block","height":height,"time":1000,"confirmations":depth}) });
    }

    #[test]
    fn outpoint_navigation_selects_output_after_inputs_with_multiple_links() {
        let raw = json!({"vin":[{"txid":"input","vout":0}],"vout":[{"n":0,"value":1,"scriptPubKey":{"addresses":["RFirst","RSecond"]}},{"n":1,"value":2,"scriptPubKey":{}}]});
        let flows = transaction_flows(&raw, &ObjectStore::default());
        let selection = output_selection(&flows, 1).unwrap();
        let (flow, _) = transaction_flow_selections(&flows)[selection];
        assert_eq!(flows[flow].kind, TransactionFlowKind::Output);
        assert_eq!(flows[flow].index, 1);
        assert!(output_selection(&flows, 9).is_none());
    }

    #[test]
    fn output_tags_open_the_exact_trail_or_observing_currency() {
        let currency = "i3f7tSctFkiPpiedY8QR5Tep9p4qDVebDx";
        let transfer = json!({"valueSat":100,"scriptPubKey":{"reservetransfer":{
            "version":1,"flags":3,"convert":true,"destinationcurrencyid":currency}}});
        let raw = json!({"vout":[transfer.clone(), {"valueSat":0}, transfer]});
        let flows = transaction_flows(&raw, &ObjectStore::default());
        let flow = &flows[2];
        let reserve = flow
            .tags
            .iter()
            .find(|tag| tag.kind == OutputTagKind::ReserveTransfer)
            .unwrap();
        let conversion = flow
            .tags
            .iter()
            .find(|tag| tag.kind == OutputTagKind::Conversion)
            .unwrap();
        assert!(matches!(
            tag_target(flow, reserve),
            Some(FlowNavigation::Trail(2))
        ));
        assert!(
            matches!(tag_target(flow,conversion),Some(FlowNavigation::Currency(id)) if id==currency)
        );
        let panel = DefiPanel::for_output(&raw, 2).unwrap();
        assert_eq!(panel.roots[panel.selected_root].0, 2);
        assert!(DefiPanel::for_output(&raw, 1).is_none());
        assert!(DefiPanel::for_output(&raw, 7).is_none());
        let selections = transaction_flow_selections(&flows);
        assert!(
            selections
                .iter()
                .any(|(index, link)| *index == 2 && matches!(link, FlowLink::Tag(_)))
        );
    }

    #[test]
    fn protocol_output_links_preserve_source_system_vout_and_money() {
        let local = "iHog9UCTrn95qpUBFCZ7kKz7qWdMA8MQ6N";
        let source = "i5w5MuNik5NtLcYmNzcvaoixooEebB6MGV";
        let store = ObjectStore {
            workspace_system: Some(crate::world::SystemId::try_from(local.to_owned()).unwrap()),
            ..ObjectStore::default()
        };
        let raw = json!({"txid":"a".repeat(64),"vout":[{"valueSat":1234,"scriptPubKey":{
            "addresses":["RKLN7wFhbrJFkPG8XkKteErAe5CjqoddTm"],
            "crosschainimport":{"sourcesystemid":source,"exporttxid":"b".repeat(64),"exporttxout":7},
            "reserveoutput":{"currencyvalues":{source:2.5}},
            "contentmap":{"address":"RNotARecipient"}}}]});
        let flow = transaction_flows(&raw, &store).remove(0);
        assert_eq!(flow.value_zat, Some(1234));
        assert_eq!(flow.reserve_amounts.get(source), Some(&250000000));
        assert_eq!(flow.addresses, vec!["RKLN7wFhbrJFkPG8XkKteErAe5CjqoddTm"]);
        assert!(
            flow.routing
                .values()
                .any(|label| label.contains("protocol routing"))
        );
        assert!(flow.references.links.iter().any(|link|link.label=="Source export" &&
            matches!(&link.target,crate::protocol::Target::Outpoint(target) if target.system_id.as_ref()==source && target.vout==7 && target.txid=="b".repeat(64))));
    }

    #[test]
    fn disabled_source_cannot_apply_queued_live_results() {
        let mut state = regression_state();
        let control = Arc::new(crate::sources::Control::default());
        let generation = control.generation();
        state.mempool.loading = true;
        let event = AppEvent::Guarded {
            control: control.clone(),
            generation,
            event: Box::new(AppEvent::MempoolRefresh(Ok(parse_mempool_snapshot(
                std::collections::HashMap::new(),
            )))),
        };
        control.set(crate::sources::ConnectionState::Disabled);
        handle_app_event(&mut state, event);
        assert!(!state.mempool.loading);
        assert!(state.mempool.last_refresh.is_none());
        state.store.chain.observe(RpcObservation {
            authorization: Some((control, generation, 0)),
            order: 999,
            method: "getinfo".into(),
            params: vec![],
            result: json!({"blocks":999}),
        });
        assert!(state.store.chain.height.is_none());
    }

    #[test]
    fn output_age_badges_disappear_at_the_exact_confirmation_boundaries() {
        let mut store = ObjectStore::default();
        let mut transaction = transaction_record(
            "reward",
            json!({"txid":"reward","blockhash":"reward-block","vin":[{"coinbase":"00"}],"vout":[{"n":0,"valueZat":100,"scriptPubKey":{"type":"pubkeyhash","addresses":["RReward"],"spendableoutput":true}}]}),
        );
        transaction.block_height = Some(500);
        let flow = transaction_flows(&transaction.raw_json, &store)
            .into_iter()
            .find(|flow| flow.kind == TransactionFlowKind::Output)
            .unwrap();
        observe_test_block(&mut store, 500, 99, 1);
        assert_eq!(
            output_status_badges(&transaction, &flow, &store),
            vec![StatusBadge::Immature, StatusBadge::StakeAgePending]
        );
        // Tip updates alone change the badges; the transaction isn't reloaded.
        store.chain.observe(RpcObservation {
            authorization: None,
            order: 3,
            method: "getblockcount".into(),
            params: vec![],
            result: json!(599),
        });
        assert_eq!(
            output_status_badges(&transaction, &flow, &store),
            vec![StatusBadge::StakeAgePending]
        );
        store.chain.observe(RpcObservation {
            authorization: None,
            order: 4,
            method: "getblockcount".into(),
            params: vec![],
            result: json!(648),
        });
        assert_eq!(
            output_status_badges(&transaction, &flow, &store),
            vec![StatusBadge::StakeAgePending]
        );
        store.chain.observe(RpcObservation {
            authorization: None,
            order: 5,
            method: "getblockcount".into(),
            params: vec![],
            result: json!(649),
        });
        assert!(output_status_badges(&transaction, &flow, &store).is_empty());
        store.chain.online = false;
        assert!(output_status_badges(&transaction, &flow, &store).is_empty());
    }

    #[test]
    fn age_badges_exclude_protocol_outputs_and_pbaas_block_one_maturity() {
        let mut store = ObjectStore::default();
        let transaction = transaction_record(
            "reward",
            json!({
                "txid":"reward","height":1,"blockhash":"reward-block","vin":[{"coinbase":"00"}],
                "vout":[
                    {"n":0,"valueSat":100,"scriptPubKey":{"type":"pubkeyhash","spendableoutput":true}},
                    {"n":1,"valueSat":0,"scriptPubKey":{"type":"cryptocondition","spendableoutput":false}},
                    {"n":2,"valueSat":100,"scriptPubKey":{"type":"nulldata"}}
                ]
            }),
        );
        observe_test_block(&mut store, 1, 2, 1);
        let flows = transaction_flows(&transaction.raw_json, &store);
        let outputs: Vec<_> = flows
            .iter()
            .filter(|flow| flow.kind == TransactionFlowKind::Output)
            .collect();
        assert_eq!(
            output_status_badges(&transaction, outputs[0], &store),
            vec![StatusBadge::Immature, StatusBadge::StakeAgePending]
        );
        assert!(output_status_badges(&transaction, outputs[1], &store).is_empty());
        assert!(output_status_badges(&transaction, outputs[2], &store).is_empty());
        store.chain.chain_name = Some("PBaaS".into());
        assert_eq!(
            output_status_badges(&transaction, outputs[0], &store),
            vec![StatusBadge::StakeAgePending]
        );
        assert!(output_status_badges(&transaction, &flows[0], &store).is_empty());
    }

    #[test]
    fn input_ids_and_addresses_are_separate_links_and_source_context_is_historical() {
        let mut state = regression_state();
        let source_id = "a".repeat(64);
        let mut source = transaction_record(
            &source_id,
            json!({"txid":source_id,"blockhash":"source-block","blocktime":1000,"vout":[{"n":0,"valueZat":100,"scriptPubKey":{"type":"pubkeyhash","addresses":["RSource"]}}]}),
        );
        source.block_height = Some(40);
        state.store.insert_transaction(source);
        let mut spending = transaction_record(
            "spend",
            json!({"txid":"spend","blockhash":"spending-block","vin":[{"txid":source_id,"vout":0,"sequence":1}],"vout":[]}),
        );
        spending.block_height = Some(50);
        let flows = transaction_flows(&spending.raw_json, &state.store);
        assert_eq!(
            transaction_flow_selections(&flows),
            vec![(0, FlowLink::SourceTransaction), (0, FlowLink::Address(0))]
        );
        assert!(source_outpoint_label(&flows[0], 80).contains(&source_id));
        assert!(!source_outpoint_label(&flows[0], 60).contains(&source_id));
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        let context = TransactionFlowRenderContext {
            transaction: &spending,
            theme: &theme,
            store: &state.store,
            currency: "VRSC",
            unmask_identity_addresses: false,
        };
        let text = input_context_lines(&context, &flows[0], 150)[0].to_string();
        assert!(text.contains("Block time 1970-01-01T00:16:40Z"));
        assert!(text.contains("Confirmations when spent 10"));
        assert!(text.contains("Height 40"));
        assert!(
            !input_context_lines(&context, &flows[0], 60)[0]
                .to_string()
                .contains("Height")
        );
        state.store.insert_transaction(spending);
        state.screen = transaction_screen("spend".into());
        let config = regression_config();
        let (tx, _rx) = unbounded_channel();
        open_selected_transaction_flow(&mut state, &config, &tx);
        assert!(matches!(&state.screen,Screen::Transaction{txid,..} if txid==&source_id));
        state.screen = transaction_screen("spend".into());
        select_next_transaction_flow(&mut state);
        assert!(matches!(
            &state.screen,
            Screen::Transaction {
                selected_flow: 1,
                ..
            }
        ));
    }

    #[test]
    fn input_link_remains_selected_when_an_earlier_prevout_loads() {
        let mut state = regression_state();
        state.store.insert_transaction(transaction_record(
            "pending",
            json!({
                "txid":"pending", "vin":[{"txid":"first","vout":0},{"txid":"second","vout":0}]
            }),
        ));
        if let Screen::Transaction { selected_flow, .. } = &mut state.screen {
            *selected_flow = 1;
        }
        handle_app_event(
            &mut state,
            AppEvent::Transactions {
                requested: vec!["first".into()],
                failed: vec![],
                records: vec![transaction_record(
                    "first",
                    json!({"txid":"first", "vout":[{"n":0,"valueSat":100,"scriptPubKey":{"type":"pubkeyhash","addresses":["RFirst"]}}]}),
                )],
            },
        );
        assert!(matches!(
            state.screen,
            Screen::Transaction {
                selected_flow: 2,
                ..
            }
        ));
    }

    #[test]
    fn overview_input_links_render_at_wide_and_narrow_widths() {
        use ratatui::backend::TestBackend;
        let mut state = regression_state();
        let source = "a".repeat(64);
        state.store.insert_transaction(transaction_record(&source, json!({"txid":source,"height":40,"blockhash":"source-block","blocktime":1000,"vout":[{"n":0,"valueSat":100,"scriptPubKey":{"type":"pubkeyhash","addresses":["RSource"]}}]})));
        state.store.insert_transaction(transaction_record("pending", json!({"txid":"pending","height":50,"blockhash":"spent-block","blocktime":1600,"vin":[{"txid":source,"vout":0,"sequence":1}]})));
        let config = regression_config();
        let theme = Theme::desert(config.emoji);
        for width in [45, 80, 120] {
            let mut terminal = Terminal::new(TestBackend::new(width, 35)).unwrap();
            terminal
                .draw(|frame| {
                    render(frame, &config, &theme, &state, true);
                })
                .unwrap();
            let display: String = terminal
                .backend()
                .buffer()
                .content
                .iter()
                .map(|cell| cell.symbol())
                .collect();
            assert!(display.contains("INPUT 0"), "{width}: {display}");
            assert!(
                display.contains("Confirmations when spent 10"),
                "{width}: {display}"
            );
            assert_eq!(display.contains(&source), width >= 80);
        }
    }

    #[test]
    fn help_time_toggle_preserves_raw_json() {
        let mut state = regression_state();
        state.auxiliary = Some(AuxiliaryScreen::Help { scroll: 0 });
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        let config = regression_config();
        let (tx, _rx) = unbounded_channel();
        let raw = json!({"time":1000});
        state
            .store
            .insert_transaction(transaction_record("time", raw.clone()));
        assert_eq!(
            theme.time_display.get(),
            crate::display_time::TimeDisplay::Utc
        );
        handle_auxiliary_key(&mut state, &theme, &config, &tx, KeyCode::Char('t'), 30);
        assert_eq!(
            theme.time_display.get(),
            crate::display_time::TimeDisplay::Local
        );
        assert_eq!(state.store.transaction("time").unwrap().raw_json, raw);
        handle_auxiliary_key(&mut state, &theme, &config, &tx, KeyCode::Char('t'), 30);
        assert_eq!(
            theme.time_display.get(),
            crate::display_time::TimeDisplay::Utc
        );
    }

    #[test]
    fn obsolete_branch_results_do_not_restore_mutable_views() {
        let mut state = regression_state();
        let control = Arc::new(crate::sources::Control::default());
        control
            .branch_epoch
            .store(1, std::sync::atomic::Ordering::Release);
        let event = AppEvent::RevisionGuarded {
            control: control.clone(),
            generation: control.generation(),
            epoch: 0,
            event: Box::new(AppEvent::IdentityOffers {
                identity: "old".into(),
                result: Ok(vec![]),
            }),
        };
        handle_app_event(&mut state, event);
        assert!(!state.store.identity_offers.contains_key("old"));
        let screen = format!("{:?}", state.screen);
        state
            .store
            .insert_transaction(transaction_record("pending", json!({"txid":"pending"})));
        invalidate_reorg_views(&mut state);
        assert_eq!(format!("{:?}", state.screen), screen);
        assert!(state.store.transaction("pending").is_some());
    }

    #[tokio::test]
    async fn evidence_name_hydration_does_not_wait_for_address_activity() {
        let mut state = regression_state();
        let own = "i4NpJp1vqrXgDvSNBXkNYvTR1VF2HMkeDA";
        let other = "i5v3h9FWVdRFbNHU7DfcpGykQjRaHtMqu7";
        state
            .store
            .insert_identity_version("Test@", test_identity_version(own, "Test@", 100));
        state.screen = identity_screen("Test@".into(), None);
        let report = identity_evidence::IdentityEvidence {
            links: vec![identity_evidence::EvidenceLink {
                label: "Referral".into(),
                detail: format!("via {own}"),
                target: identity_evidence::EvidenceTarget::Identity(other.into()),
                source: "tx:0".into(),
            }],
            ..Default::default()
        };
        state
            .store
            .identity_evidence
            .insert(own.into(), (100, report));
        let (tx, _rx) = unbounded_channel();
        request_current_evidence_identity_names(&mut state, &regression_config(), &tx);
        assert!(state.loading_identity_names.contains(other));
    }

    #[test]
    fn evidence_list_resolves_names_and_hides_cached_rpc_errors() {
        let mut state = regression_state();
        let own = "i4NpJp1vqrXgDvSNBXkNYvTR1VF2HMkeDA";
        let other = "i5v3h9FWVdRFbNHU7DfcpGykQjRaHtMqu7";
        let version = test_identity_version(own, "Test@", 100);
        state
            .store
            .insert_resolved_identity_name(other.into(), "Foundation@".into());
        let report = identity_evidence::IdentityEvidence {
            links: vec![identity_evidence::EvidenceLink {
                label: "Referral".into(),
                detail: format!("via {other}"),
                target: identity_evidence::EvidenceTarget::Identity(other.into()),
                source: "tx:0".into(),
            }],
            notes: vec![
                "revocationauthority reverse lookup unavailable: RPC error requires -idindex=1"
                    .into(),
            ],
            ..Default::default()
        };
        assert!(evidence_identity_references(&report).contains(other));
        state
            .store
            .identity_evidence
            .insert(own.into(), (100, report));
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        for raw in [false, true] {
            state.unmask_identity_addresses = raw;
            let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(160, 12)).unwrap();
            terminal
                .draw(|frame| {
                    render_identity_evidence(
                        frame,
                        &theme,
                        &state,
                        &version,
                        None,
                        None,
                        frame.area(),
                    )
                })
                .unwrap();
            let text: String = terminal
                .backend()
                .buffer()
                .content
                .iter()
                .map(|cell| cell.symbol())
                .collect();
            assert!(
                !text.contains("RPC error") && !text.contains("-idindex"),
                "{text}"
            );
            if raw {
                assert!(text.contains(other), "{text}");
            } else {
                assert!(text.contains("Foundation@"), "{text}");
                assert!(!text.contains(other), "{text}");
            }
        }
    }

    #[test]
    fn identity_detail_leaves_one_blank_line_between_sections() {
        let mut state = regression_state();
        let own = "i5w5MuNik5NtLcYmNzcvaoixooEebB6MGV";
        let mut version = test_identity_version(own, "VRSC@", 1000957);
        let primary = "RFullPrimaryAddressOne";
        version.raw_json["identity"]["primaryaddresses"] = json!([primary]);
        state.store.insert_identity_version(own, version);
        state.store.identity_history_complete.insert(own.into());
        let report = identity_evidence::IdentityEvidence {
            links: vec![identity_evidence::EvidenceLink {
                label: "Identity import transaction".into(),
                detail: "imported identity output 5".into(),
                target: identity_evidence::EvidenceTarget::Transaction("import".into()),
                source: "import:5".into(),
            }],
            ..Default::default()
        };
        state
            .store
            .identity_evidence
            .insert(own.into(), (1000957, report));
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(160, 50)).unwrap();
        terminal
            .draw(|frame| {
                render_identity_view(
                    frame,
                    &regression_config(),
                    &theme,
                    &state,
                    &IdentityTarget {
                        query: own.into(),
                        height: None,
                    },
                    ActivitySelection {
                        transaction: 0,
                        link: Some(0),
                    },
                    None,
                    frame.area(),
                )
            })
            .unwrap();
        let rows: Vec<String> = terminal
            .backend()
            .buffer()
            .content
            .chunks(160)
            .map(|row| row.iter().map(|c| c.symbol()).collect())
            .collect();
        let note = rows
            .iter()
            .position(|row| row.contains("Association is evidence"))
            .unwrap();
        let associations = rows
            .iter()
            .position(|row| row.contains("Registration and associations"))
            .unwrap();
        assert!(!rows[associations - 2].trim().is_empty(), "{rows:#?}");
        assert!(rows[associations - 1].trim().is_empty(), "{rows:#?}");
        assert!(rows[note + 1].trim().is_empty(), "{rows:#?}");
        assert!(rows[note + 2].contains("transaction activity"), "{rows:#?}");
        let primary_row = rows.iter().position(|row| row.contains(primary)).unwrap() as u16;
        let cursor = &terminal.backend().buffer()[(0, primary_row)];
        assert_eq!(cursor.symbol(), SELECTION_MARKER);
        assert_eq!(cursor.bg, theme.accent);
    }

    #[test]
    fn identity_overview_copies_evidence_and_raw_authority_values() {
        let mut state = regression_state();
        let address = "i5w5MuNik5NtLcYmNzcvaoixooEebB6MGV";
        let mut version = test_identity_version(address, "VRSC@", 100);
        version.raw_json["identity"]["revocationauthority"] = json!(address);
        state.store.insert_identity_version(address, version);
        let hash = "b".repeat(64);
        let txid = "a".repeat(64);
        let source = format!("{txid}:0");
        state.store.identity_evidence.insert(
            address.into(),
            (
                100,
                identity_evidence::IdentityEvidence {
                    registration: Some(format!("VRSC@  tx {}:0", short_id(&txid))),
                    links: vec![
                        identity_evidence::EvidenceLink {
                            label: "Name commitment".into(),
                            detail: format!("hash {hash}"),
                            target: identity_evidence::EvidenceTarget::Transaction(txid.clone()),
                            source: source.clone(),
                        },
                        identity_evidence::EvidenceLink {
                            label: "Registration transaction".into(),
                            detail: "reservation output 0".into(),
                            target: identity_evidence::EvidenceTarget::Transaction(txid.clone()),
                            source: source.clone(),
                        },
                    ],
                    ..Default::default()
                },
            ),
        );
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(160, 40)).unwrap();
        terminal
            .draw(|frame| {
                theme.copy_values.borrow_mut().begin(true);
                render_identity_view(
                    frame,
                    &regression_config(),
                    &theme,
                    &state,
                    &IdentityTarget {
                        query: address.into(),
                        height: None,
                    },
                    ActivitySelection {
                        transaction: 0,
                        link: Some(0),
                    },
                    None,
                    frame.area(),
                );
                theme
                    .copy_values
                    .borrow_mut()
                    .finish(frame.buffer_mut(), theme.selection_cursor());
            })
            .unwrap();
        let rows = terminal
            .backend()
            .buffer()
            .content
            .chunks(160)
            .map(|row| row.iter().map(|cell| cell.symbol()).collect::<String>())
            .collect::<Vec<_>>();
        for (line, clicked, expected) in [
            ("Name commitment", hash.as_str(), hash.as_str()),
            ("Name commitment", short_id(&txid).as_str(), txid.as_str()),
            ("Source:", source.as_str(), source.as_str()),
            ("VRSC@  tx", short_id(&txid).as_str(), source.as_str()),
            ("Revocation:", "self", address),
        ] {
            let (y, row) = rows
                .iter()
                .enumerate()
                .find(|(_, row)| row.contains(line))
                .unwrap();
            let x = row.find(clicked).unwrap();
            let hit = theme
                .copy_values
                .borrow_mut()
                .select(ratatui::layout::Position::new(x as u16, y as u16));
            assert!(
                matches!(hit, Some(crate::copy_values::Target::Text { value, .. }) if value == expected),
                "unexpected copy target for {line}"
            );
        }
    }

    #[test]
    fn identity_association_details_use_available_columns() {
        let mut state = regression_state();
        let address = "i5w5MuNik5NtLcYmNzcvaoixooEebB6MGV";
        let version = test_identity_version(address, "VRSC@", 100);
        let label = "Commitment control destination";
        let detail = "decoded commitment output destination; control role may vary";
        let target = "a".repeat(64);
        state.store.identity_evidence.insert(
            address.into(),
            (
                100,
                identity_evidence::IdentityEvidence {
                    links: vec![identity_evidence::EvidenceLink {
                        label: label.into(),
                        detail: detail.into(),
                        target: identity_evidence::EvidenceTarget::Transaction(target.clone()),
                        source: "synthetic".into(),
                    }],
                    ..Default::default()
                },
            ),
        );
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        for (width, fits) in [(120, true), (70, false)] {
            let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(width, 5)).unwrap();
            terminal
                .draw(|frame| {
                    render_identity_evidence(
                        frame,
                        &theme,
                        &state,
                        &version,
                        None,
                        None,
                        frame.area(),
                    );
                })
                .unwrap();
            let row: String = terminal.backend().buffer().content
                [usize::from(width)..2 * usize::from(width)]
                .iter()
                .map(|cell| cell.symbol())
                .collect();
            assert!(row.contains(label), "{row}");
            assert_eq!(row.contains(detail), fits, "{row}");
            assert!(row.ends_with(&short_id(&target)), "{row}");
        }
    }

    #[test]
    fn identity_association_scrollbar_tracks_the_visible_links() {
        let mut state = regression_state();
        let address = "i5w5MuNik5NtLcYmNzcvaoixooEebB6MGV";
        let version = test_identity_version(address, "VRSC@", 100);
        let links = (0..14)
            .map(|index| identity_evidence::EvidenceLink {
                label: format!("Association {index}"),
                detail: String::new(),
                target: identity_evidence::EvidenceTarget::Transaction(format!("{index:064x}")),
                source: format!("source {index}"),
            })
            .collect();
        state.store.identity_evidence.insert(
            address.into(),
            (
                100,
                identity_evidence::IdentityEvidence {
                    links,
                    ..Default::default()
                },
            ),
        );
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(90, 8)).unwrap();
        for (selected, thumb, track) in [(0, 1, 6), (13, 6, 1)] {
            terminal
                .draw(|frame| {
                    render_identity_evidence(
                        frame,
                        &theme,
                        &state,
                        &version,
                        Some(selected),
                        None,
                        frame.area(),
                    );
                })
                .unwrap();
            let buffer = terminal.backend().buffer();
            assert_eq!(buffer[(89, thumb)].symbol(), "█");
            assert_eq!(buffer[(89, thumb)].fg, theme.accent);
            assert_eq!(buffer[(89, track)].symbol(), "│");
            assert_eq!(buffer[(89, 0)].symbol(), " ");
            assert_eq!(buffer[(89, 7)].symbol(), " ");
        }
    }

    #[test]
    fn queued_updates_leave_time_for_keyboard_input() {
        let mut state = regression_state();
        let (tx, mut rx) = unbounded_channel();
        for _ in 0..1000 {
            tx.send(AppEvent::ReconciliationStarted).unwrap();
        }
        let processed = drain_app_events(&mut state, &mut rx);
        assert!(processed > 0 && processed <= APP_EVENT_BATCH_LIMIT);
        assert_eq!(rx.len(), 1000 - processed);
        assert!(state.store.chain.reconciling);
    }

    #[test]
    fn background_drain_leaves_ordered_events_queued_when_input_arrives() {
        let mut state = regression_state();
        let (tx, mut rx) = unbounded_channel();
        for _ in 0..1000 {
            tx.send(AppEvent::ReconciliationStarted).unwrap();
        }
        let mut checks = 0;
        let processed = drain_app_events_until(
            &mut state,
            &mut rx,
            Instant::now(),
            &ui_timing::Timing::default(),
            || {
                checks += 1;
                checks > 3
            },
        );
        assert_eq!(processed, 3);
        assert_eq!(rx.len(), 997);
    }

    #[test]
    fn reconciliation_of_large_history_keeps_ui_work_bounded() {
        let mut state = regression_state();
        let canonical = (0..40_000_u64)
            .map(|height| {
                (
                    height,
                    crate::reconciliation::Header {
                        hash: format!("{height:064x}"),
                        height,
                        parent: height.checked_sub(1).map(|p| format!("{p:064x}")),
                        time: height,
                    },
                )
            })
            .collect();
        let block_hash = format!("{:064x}", 39_999);
        for index in 0..512 {
            let txid = format!("tx-{index}");
            state.store.insert_transaction(transaction_record(
                &txid,
                json!({"txid":txid,"blockhash":block_hash,"vin":[],"vout":[]}),
            ));
        }
        let change = crate::reconciliation::Change {
            state: crate::reconciliation::State {
                revision: 1,
                canonical,
                ..Default::default()
            },
            detached: vec![],
            attached: vec![],
            ancestor: None,
        };
        let started = Instant::now();
        apply_app_event(
            &mut state,
            AppEvent::Reconciled(Ok(Arc::new(change.into()))),
        );
        let elapsed = started.elapsed();
        eprintln!("40,000 headers / 512 transactions applied in {elapsed:?}");
        assert!(
            elapsed < Duration::from_millis(250),
            "UI reconciliation stalled for {elapsed:?}"
        );
        assert!(
            state
                .store
                .transactions_by_txid
                .values()
                .all(|tx| tx.block_height == Some(39_999))
        );
    }

    #[test]
    fn paste_only_inserts_into_active_inputs_and_never_submits_or_runs_shortcuts() {
        let mut state = regression_state();
        let before = state.screen.clone();
        paste_into_active_input(&mut state, "qDM/");
        assert_eq!(
            std::mem::discriminant(&state.screen),
            std::mem::discriminant(&before)
        );
        assert!(!state.search.active);
        begin_search(&mut state);
        paste_into_active_input(&mut state, &input::paste_text("VRSC@\rqDM\n"));
        assert_eq!(state.search.query, "VRSC@qDM");
        assert!(state.search.active);
        assert!(state.search.pending_request.is_none());
        assert!(!state.unmask_identity_addresses);
    }

    #[test]
    fn escape_leaves_navigation_roots_open_and_returns_non_dashboard_roots_to_dashboard() {
        let mut state = regression_state();
        state.screen = transaction_screen("a".repeat(64));
        state.screen_stack.clear();
        navigate_back_or_dashboard(&mut state);
        assert!(matches!(state.screen, Screen::Dashboard));
        navigate_back_or_dashboard(&mut state);
        assert!(matches!(state.screen, Screen::Dashboard));
        push_screen(&mut state, transaction_screen("a".repeat(64)));
        push_screen(&mut state, block_screen(BlockTarget::Height(1)));
        navigate_back_or_dashboard(&mut state);
        assert!(matches!(state.screen, Screen::Transaction { .. }));
        assert_eq!(state.screen_stack.len(), 1);
    }

    #[test]
    fn visible_auxiliary_and_filter_panels_do_not_inherit_hidden_text_entry_focus() {
        let mut state = regression_state();
        state.search.active = true;
        assert!(quit_input_active(&state));
        state.auxiliary = Some(AuxiliaryScreen::Help { scroll: 0 });
        assert!(!quit_input_active(&state));
        let config = regression_config();
        let (tx, _rx) = unbounded_channel();
        assert!(close_base_interface(&mut state, &config, &tx));
        assert!(state.auxiliary.is_none());
        assert!(state.search.active); // Escape closed Help, not the hidden search.
        close_base_interface(&mut state, &config, &tx);
        assert!(!state.search.active);
        state.screen = currency_browser::screen(&state.store);
        if let Screen::Currencies(view) = &mut state.screen {
            view.editing = true;
        }
        assert!(quit_input_active(&state));
        if let Screen::Currencies(view) = &mut state.screen {
            view.options_open = true;
        }
        assert!(!quit_input_active(&state));
    }

    pub(super) fn regression_state() -> AppState {
        AppState {
            route_label: "chain tip".to_string(),
            tip: ChainTipStatus {
                label: "tip 1".to_string(),
                healthy: true,
                height: Some(1),
                longest_chain: Some(1),
                last_block_time: Some(1),
                connections: Some(8),
            },
            store: ObjectStore::default(),
            dashboard: DashboardState {
                recent_blocks: vec!["old-block".to_string()],
                ..DashboardState::default()
            },
            mempool: MempoolState::default(),
            marketplace: MarketplaceState::default(),
            screen: Screen::Transaction {
                txid: "pending".to_string(),
                selected_flow: 0,
                mode: ViewMode::Overview,
                json: JsonState::new(),
                notarization: None,
            },
            screen_stack: vec![Screen::Block {
                target: BlockTarget::Hash("old-block".to_string()),
                selected_tx: 0,
                mode: ViewMode::Overview,
                json: JsonState::new(),
            }],
            loading_blocks: BTreeSet::new(),
            loading_transactions: BTreeSet::new(),
            transaction_retry_after: HashMap::new(),
            transaction_verified_tip: HashMap::new(),
            loading_addresses: BTreeSet::new(),
            loading_address_histories: BTreeSet::new(),
            loading_address_utxos: BTreeSet::new(),
            loading_address_balances: BTreeSet::new(),
            address_errors: HashMap::new(),
            address_retry_after: HashMap::new(),
            address_history_errors: HashMap::new(),
            address_history_retry_after: HashMap::new(),
            address_utxo_errors: HashMap::new(),
            address_utxo_retry_after: HashMap::new(),
            address_balance_errors: HashMap::new(),
            address_balance_retry_after: HashMap::new(),
            loading_outpoints: HashSet::new(),
            loading_identities: HashSet::new(),
            identity_load_attempted: HashSet::new(),
            loading_identity_names: HashSet::new(),
            identity_name_attempted: HashSet::new(),
            loading_address_identities: HashSet::new(),
            address_identities_attempted: HashSet::new(),
            loading_currencies: HashSet::new(),
            currency_load_attempted: HashSet::new(),
            unmask_identity_addresses: false,
            notice: None,
            clipboard_busy: false,
            save_prompt: None,
            cache_clear_prompt: None,
            cache_size_bytes: Some(0),
            search: SearchState::default(),
            auxiliary: None,
            help_return: None,
            peers: test_peer_state(),
            log_feed: test_log_feed_state(),
        }
    }

    pub(super) fn regression_config() -> Config {
        Config {
            chain: crate::config::ChainSelection {
                system_id: std::sync::Arc::new(std::sync::OnceLock::from(
                    crate::world::SystemId::try_from("iTestSystem".to_string()).unwrap(),
                )),
                name: "VRSC".into(),
                testnet: false,
            },
            daemon: crate::config::DaemonConfig {
                host: "127.0.0.1".into(),
                port: 1,
                user: None,
                password: None,
                config_file: None,
                observations: None,
                control: Default::default(),
                api_pool: Default::default(),
                background: false,
            },
            api_consent: true,
            cache_path: PathBuf::new(),
            emoji: crate::cli::EmojiMode::Off,
            poll_interval_seconds: 3,
        }
    }

    #[test]
    fn bookmark_open_preserves_history_and_source_without_live_membership_claims() {
        use crate::bookmarks::{Bookmark, Kind, Snapshot};
        let mut state = regression_state();
        let config = regression_config();
        let txid = "a".repeat(64);
        let bookmark = Bookmark::new(
            config.chain.system_id.get().unwrap().clone(),
            Kind::Transaction,
            txid.clone(),
            "Investigation".into(),
        )
        .unwrap();
        let prior = state.screen_stack.len();
        state.search.pending_request = Some(12);
        bookmark_ui::install(
            &mut state,
            &config,
            &[],
            &bookmark,
            Snapshot {
                kind: Kind::Transaction,
                source: "API:test".into(),
                raw: json!({"txid":txid,"confirmations":0,"vin":[],"vout":[]}),
            },
        )
        .unwrap();
        assert_eq!(state.screen_stack.len(), prior + 1);
        assert!(state.search.pending_request.is_none());
        assert_eq!(
            state.store.transaction(&txid).unwrap().source_id.as_deref(),
            Some("API:test")
        );
        assert!(matches!(&state.screen,Screen::Transaction {txid:id,..} if id==&txid));
        pop_screen(&mut state);
        assert!(matches!(&state.screen,Screen::Transaction {txid,..} if txid=="pending"));
    }

    #[test]
    fn regression_transaction_confirmation_replaces_mempool_record() {
        let mut store = ObjectStore::default();
        store.insert_transaction(transaction_record(
            "pending",
            json!({"txid": "pending", "confirmations": 0}),
        ));
        store.insert_transaction(transaction_record(
            "pending",
            json!({"txid": "pending", "confirmations": 1, "blockhash": "mined", "blocktime": 1000}),
        ));
        let transaction = store.transaction("pending").unwrap();
        assert_eq!(transaction.raw_json["confirmations"], 1);
        assert_eq!(transaction.block_hash.as_deref(), Some("mined"));
    }

    #[test]
    fn regression_open_transaction_refreshes_on_tip_changes_and_retries_failures() {
        use ratatui::backend::TestBackend;
        let mut state = regression_state();
        state.store.insert_transaction(transaction_record(
            "pending",
            json!({"txid":"pending", "confirmations":0}),
        ));
        state
            .store
            .insert_block(test_block(1, "tip-one", "another-tx"));
        state
            .transaction_verified_tip
            .insert("pending".into(), "tip-one".into());
        assert!(transaction_refresh_target(&state).is_none());
        let mut tip = state.tip.clone();
        tip.height = Some(2);
        handle_app_event(
            &mut state,
            AppEvent::DashboardRefresh(Ok(DashboardSnapshot {
                tip,
                blocks: vec![test_block(2, "tip-two", "pending")],
            })),
        );
        assert_eq!(
            transaction_refresh_target(&state),
            Some(("pending".into(), "tip-two".into()))
        );
        state.loading_transactions.insert("pending".into());
        assert!(transaction_refresh_target(&state).is_none());
        handle_app_event(
            &mut state,
            AppEvent::TransactionRefresh {
                txid: "pending".into(),
                tip: "tip-two".into(),
                result: Err("temporary RPC error".into()),
            },
        );
        assert!(transaction_refresh_target(&state).is_none());
        state
            .transaction_retry_after
            .insert("pending".into(), Instant::now());
        assert!(transaction_refresh_target(&state).is_some());
        handle_app_event(
            &mut state,
            AppEvent::TransactionRefresh {
                txid: "pending".into(),
                tip: "tip-two".into(),
                result: Ok(transaction_record(
                    "pending",
                    json!({"txid":"pending", "confirmations":1, "blockhash":"tip-two", "blocktime":2}),
                )),
            },
        );
        assert!(transaction_refresh_target(&state).is_none());
        let record = state.store.transaction("pending").unwrap();
        assert_eq!(record.block_height, Some(2));
        assert_eq!(record.raw_json["confirmations"], 1);
        state.notice = None;
        let config = regression_config();
        let theme = Theme::desert(config.emoji);
        let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
        terminal
            .draw(|frame| {
                render(frame, &config, &theme, &state, true);
            })
            .unwrap();
        let display: String = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect();
        assert!(display.contains("confirmed (1)"));
        assert!(matches!(&state.screen, Screen::Transaction { txid, .. } if txid == "pending"));
        // Same-height replacement also requires a live check.
        state
            .store
            .insert_block(test_block(2, "replacement", "other"));
        assert_eq!(
            transaction_refresh_target(&state),
            Some(("pending".into(), "replacement".into()))
        );
    }

    #[tokio::test]
    async fn regression_mempool_disk_cache_requires_live_lookup() {
        let mut config = regression_config();
        config.cache_path = std::env::temp_dir().join(format!(
            "verus-mempool-cache-{}.sqlite3",
            std::process::id()
        ));
        let cache = Cache::open(&config.cache_path).unwrap();
        cache
            .put_raw_rpc(
                &config.daemon_cache_key(),
                &config.chain_cache_key(),
                "getrawtransaction",
                "[\"pending\",1]",
                &json!({"result":{"txid":"pending","confirmations":0},"error":null}).to_string(),
            )
            .unwrap();
        // Port 1 is unavailable: returning cached success would conceal missing live status.
        assert!(load_transaction(&config, "pending").await.is_err());
        drop(cache);
        let _ = fs::remove_file(config.cache_path);
    }

    #[test]
    fn regression_header_fits_without_partial_fields() {
        let config = regression_config();
        let theme = Theme::desert(config.emoji);
        let tip = ChainTipStatus {
            label: "loaded".into(),
            healthy: true,
            height: Some(4_222_000),
            longest_chain: Some(4_222_003),
            last_block_time: Some(1000),
            connections: Some(8),
        };
        let text =
            |width| header_line(&config, &config.chain.name, &theme, &tip, 1180, width).to_string();
        assert!(text(110).contains("Verus Carbide | VRSC"));
        assert!(text(110).contains("4222000/4222003 (Δ3)"));
        assert!(text(65).contains("Carbide | VRSC"));
        assert!(text(65).contains("tip 4222000"));
        assert!(!text(65).contains('/'));
        assert_eq!(text(7), "4222000");
        for emoji in [crate::cli::EmojiMode::On, crate::cli::EmojiMode::Off] {
            let theme = Theme::desert(emoji);
            for width in 0..150 {
                for now in [1099, 1100, 1599, 1600, 2300, 1180, 1030] {
                    let line = header_line(&config, &config.chain.name, &theme, &tip, now, width);
                    assert!(line.width() <= usize::from(width));
                    if width >= 7 {
                        assert!(line.to_string().contains("4222000"));
                    }
                }
            }
        }
    }

    #[test]
    fn status_bar_uses_chain_identity_name_for_hex_configuration() {
        let mut config = regression_config();
        config.chain.name = "53fe39eea8c06bba32f1a4e20db67e5524f0309d".into();
        let id = "iHog9UCTrn95qpUBFCZ7kKz7qWdMA8MQ6N";
        config.chain.system_id = Arc::new(std::sync::OnceLock::from(
            crate::world::SystemId::try_from(id.to_string()).unwrap(),
        ));
        let mut state = regression_state();
        state
            .store
            .insert_resolved_identity_name(id.into(), "vDEX.VRSC@".into());
        let theme = Theme::desert(config.emoji);
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(160, 40)).unwrap();
        terminal
            .draw(|frame| {
                render(frame, &config, &theme, &state, true);
            })
            .unwrap();
        let header: String = terminal.backend().buffer().content[..160]
            .iter()
            .map(|cell| cell.symbol())
            .collect();
        assert!(header.contains("vDEX.VRSC@"), "{header}");
        assert!(!header.contains(&config.chain.name), "{header}");
    }

    #[tokio::test]
    async fn chain_identity_names_override_currency_hints_without_enabling_sources() {
        let mut state = regression_state();
        let config = regression_config();
        let id = config.chain_cache_key();
        state
            .store
            .currency_name_hints
            .insert(id.clone(), "short currency name".into());
        let (tx, _rx) = unbounded_channel();
        request_chain_identity_names(&mut state, &config, &tx, vec![id.clone(), id.clone()]);
        assert!(state.loading_identity_names.contains(&id));
        assert_eq!(state.loading_identity_names.len(), 1);
        config
            .daemon
            .control
            .set(crate::sources::ConnectionState::Disabled);
        let mut disabled_state = regression_state();
        request_chain_identity_names(&mut disabled_state, &config, &tx, vec![id]);
        assert!(disabled_state.loading_identity_names.is_empty());
    }

    #[test]
    fn regression_header_repaints_unchanged_cells() {
        use ratatui::backend::TestBackend;
        let config = regression_config();
        let theme = Theme::desert(config.emoji);
        let state = regression_state();
        let mut terminal = Terminal::new(TestBackend::new(90, 30)).unwrap();
        terminal
            .draw(|frame| {
                render(frame, &config, &theme, &state, true);
            })
            .unwrap();
        // A stale terminal digit can differ from Ratatui's previous buffer after a resize.
        // The next frame must emit the whole header, even cells whose logical value is unchanged.
        let before = terminal.backend().buffer().clone();
        terminal
            .draw(|frame| {
                render(frame, &config, &theme, &state, true);
            })
            .unwrap();
        let after = terminal.backend().buffer();
        assert_eq!(
            before
                .diff(after)
                .iter()
                .filter(|(_, y, _)| *y == 0)
                .count(),
            90
        );
    }

    fn test_peer_state() -> PeerState {
        PeerState {
            records: Vec::new(),
            controls: peer_controls::State::default(),
            selected: 0,
            loading: false,
            last_refresh: None,
            map_enabled: false,
            geoip_directory: PathBuf::new(),
            geoip_status: GeoIpStatus::Missing,
            geoip_database: None,
            geoip_downloading: false,
        }
    }

    fn test_log_feed_state() -> LogFeedState {
        LogFeedState {
            path: None,
            offset: None,
            partial: String::new(),
            lines: VecDeque::new(),
            loading: false,
            last_refresh: None,
            error: None,
        }
    }

    #[test]
    fn save_prompt_preserves_json_extension() {
        let prompt = SavePrompt {
            basename: "block-123".to_string(),
            extension: ".json".to_string(),
            selection_active: true,
            status: None,
        };

        assert_eq!(prompt.filename(), "block-123.json");
    }

    #[test]
    fn save_prompt_does_not_duplicate_json_extension() {
        let prompt = SavePrompt {
            basename: "custom.json".to_string(),
            extension: ".json".to_string(),
            selection_active: false,
            status: None,
        };

        assert_eq!(prompt.filename(), "custom.json");
    }

    #[test]
    fn verbose_mempool_snapshot_sums_sizes_and_sorts_newest_first() {
        let snapshot = parse_mempool_snapshot(HashMap::from([
            (
                "older".to_string(),
                json!({
                    "size": 250,
                    "fee": 0.0001,
                    "time": 100,
                    "height": 42,
                    "depends": []
                }),
            ),
            (
                "newer".to_string(),
                json!({
                    "size": 750,
                    "fee": 0.0002,
                    "time": 200,
                    "height": 43,
                    "depends": ["older"]
                }),
            ),
        ]));

        assert_eq!(snapshot.total_bytes, 1_000);
        assert_eq!(snapshot.entries[0].txid, "newer");
        assert_eq!(snapshot.entries[0].depends, vec!["older"]);
        assert_eq!(snapshot.entries[1].txid, "older");
    }

    #[test]
    fn data_sizes_use_binary_units() {
        assert_eq!(format_data_size(999), "999 B");
        assert_eq!(format_data_size(1_024), "1.0 KiB");
        assert_eq!(format_data_size(1_048_576), "1.00 MiB");
    }

    #[tokio::test]
    async fn transaction_loader_uses_persistent_cache_before_rpc() {
        let cache_path = std::env::temp_dir().join(format!(
            "verus-explorer-cache-first-{}.sqlite3",
            std::process::id()
        ));
        let config = Config {
            chain: crate::config::ChainSelection {
                system_id: std::sync::Arc::new(std::sync::OnceLock::from(
                    crate::world::SystemId::try_from("iTestSystem".to_string()).unwrap(),
                )),
                name: "VRSC".to_string(),
                testnet: false,
            },
            daemon: crate::config::DaemonConfig {
                host: "127.0.0.1".to_string(),
                port: 1,
                user: None,
                password: None,
                config_file: None,
                observations: None,
                control: Default::default(),
                api_pool: Default::default(),
                background: false,
            },
            api_consent: true,
            cache_path: cache_path.clone(),
            emoji: crate::cli::EmojiMode::Off,
            poll_interval_seconds: 3,
        };
        let txid = "cached-transaction";
        let params_json = serde_json::to_string(&[json!(txid), json!(1)]).unwrap();
        let cache = Cache::open(&cache_path).expect("open test cache");
        cache
            .put_raw_rpc(
                &config.daemon_cache_key(),
                &config.chain_cache_key(),
                "getrawtransaction",
                &params_json,
                &json!({
                    "result": {
                        "txid": txid,
                        "hex": "00",
                        "blockhash": "cached-block",
                        "blocktime": 123
                    },
                    "error": null
                })
                .to_string(),
            )
            .expect("store cached transaction");
        drop(cache);

        let transaction = load_transaction(&config, txid)
            .await
            .expect("load without reaching unavailable RPC port");

        assert_eq!(transaction.summary.txid, txid);
        assert_eq!(transaction.block_hash.as_deref(), Some("cached-block"));
        assert_eq!(transaction.block_time, 123);
        let _ = fs::remove_file(cache_path);
    }

    #[test]
    fn dashboard_refresh_does_not_invalidate_open_cached_objects() {
        let old_hash = "old-block-hash";
        let new_hash = "new-block-hash";
        let old_txid = "old-txid";
        let mut store = ObjectStore::default();
        store.insert_block(test_block(1, old_hash, old_txid));

        let mut state = AppState {
            route_label: "chain tip".to_string(),
            tip: ChainTipStatus {
                label: "tip 1".to_string(),
                healthy: true,
                height: Some(1),
                longest_chain: Some(1),
                last_block_time: Some(1),
                connections: Some(8),
            },
            store,
            dashboard: DashboardState {
                recent_blocks: vec![old_hash.to_string()],
                block_cursor: 1,
                ..DashboardState::default()
            },
            mempool: MempoolState::default(),
            marketplace: MarketplaceState::default(),
            screen: Screen::Transaction {
                txid: old_txid.to_string(),
                selected_flow: 0,
                mode: ViewMode::Overview,
                json: JsonState::new(),
                notarization: None,
            },
            screen_stack: vec![Screen::Block {
                target: BlockTarget::Hash(old_hash.to_string()),
                selected_tx: 0,
                mode: ViewMode::Overview,
                json: JsonState::new(),
            }],
            loading_blocks: BTreeSet::new(),
            loading_transactions: BTreeSet::new(),
            transaction_retry_after: HashMap::new(),
            transaction_verified_tip: HashMap::new(),
            loading_addresses: BTreeSet::new(),
            loading_address_histories: BTreeSet::new(),
            loading_address_utxos: BTreeSet::new(),
            loading_address_balances: BTreeSet::new(),
            address_errors: HashMap::new(),
            address_retry_after: HashMap::new(),
            address_history_errors: HashMap::new(),
            address_history_retry_after: HashMap::new(),
            address_utxo_errors: HashMap::new(),
            address_utxo_retry_after: HashMap::new(),
            address_balance_errors: HashMap::new(),
            address_balance_retry_after: HashMap::new(),
            loading_outpoints: HashSet::new(),
            loading_identities: HashSet::new(),
            identity_load_attempted: HashSet::new(),
            loading_identity_names: HashSet::new(),
            identity_name_attempted: HashSet::new(),
            loading_address_identities: HashSet::new(),
            address_identities_attempted: HashSet::new(),
            loading_currencies: HashSet::new(),
            currency_load_attempted: HashSet::new(),
            unmask_identity_addresses: false,
            notice: None,
            clipboard_busy: false,
            save_prompt: None,
            cache_clear_prompt: None,
            cache_size_bytes: Some(0),
            search: SearchState::default(),
            auxiliary: None,
            help_return: None,
            peers: test_peer_state(),
            log_feed: test_log_feed_state(),
        };

        handle_app_event(
            &mut state,
            AppEvent::DashboardRefresh(Ok(DashboardSnapshot {
                tip: ChainTipStatus {
                    label: "tip 2".to_string(),
                    healthy: true,
                    height: Some(2),
                    longest_chain: Some(3),
                    last_block_time: Some(2),
                    connections: Some(9),
                },
                blocks: vec![test_block(2, new_hash, "new-txid")],
            })),
        );

        assert!(state.store.block(old_hash).is_some());
        assert!(state.store.transaction(old_txid).is_some());
        assert_eq!(
            state.dashboard.recent_blocks,
            vec![new_hash.to_string(), old_hash.to_string()]
        );
        assert_eq!(
            state.dashboard.selected_block_hash().map(String::as_str),
            Some(old_hash)
        );
        assert!(matches!(
            &state.screen,
            Screen::Transaction { txid, .. } if txid == old_txid
        ));
        assert!(matches!(
            state.screen_stack.first(),
            Some(Screen::Block {
                target: BlockTarget::Hash(hash),
                ..
            }) if hash == old_hash
        ));

        let started = Instant::now();
        state.dashboard.loading_older_since = Some(started);
        handle_app_event(
            &mut state,
            AppEvent::DashboardPage {
                started,
                result: Ok(vec![test_block(0, "genesis", "genesis-tx")]),
            },
        );
        assert_eq!(
            state.dashboard.recent_blocks,
            vec![
                new_hash.to_string(),
                old_hash.to_string(),
                "genesis".to_string()
            ]
        );
        assert_eq!(
            state.dashboard.selected_block_hash().map(String::as_str),
            Some(old_hash)
        );
        assert!(state.dashboard.loading_older_since.is_none());
        assert!(!state.dashboard.has_more);
    }

    #[test]
    fn dashboard_paints_the_full_center_gap_with_the_page_background() {
        let config = regression_config();
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        let mut state = regression_state();
        state.dashboard.transactions_focused = true;
        for width in [80, 81] {
            let mut terminal =
                Terminal::new(ratatui::backend::TestBackend::new(width, 20)).unwrap();
            terminal
                .draw(|frame| {
                    render_dashboard(frame, &config, &theme, &state, 0, frame.area());
                })
                .unwrap();
            let buffer = terminal.backend().buffer();
            let midpoint = width / 2;
            assert_eq!(buffer[(midpoint + 2, 5)].bg, theme.accent);
            for y in 5..20 {
                for x in midpoint - 1..midpoint + 2 {
                    let cell = &buffer[(x, y)];
                    assert_eq!(cell.symbol(), " ");
                    assert_eq!(cell.bg, theme.background);
                }
            }
        }
    }

    #[test]
    fn dashboard_scrollbar_gutter_matches_the_gap_between_block_tiles() {
        let config = regression_config();
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        let mut state = regression_state();
        state.dashboard.recent_blocks.clear();
        for height in (1..=12).rev() {
            let hash = format!("{height:064x}");
            state
                .store
                .insert_block(test_block(height, &hash, "transaction"));
            state.dashboard.recent_blocks.push(hash);
        }
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(80, 20)).unwrap();
        for width in [80, 95] {
            terminal.backend_mut().resize(width, 20);
            terminal
                .draw(|frame| {
                    render_dashboard(frame, &config, &theme, &state, 0, frame.area());
                })
                .unwrap();
            let buffer = terminal.backend().buffer();
            let rail_x = width / 2 - 2;
            let gutter_x = rail_x - 1;
            let tile_x = gutter_x - 1;
            let first_block_y = 6 + SECTION_HEADING_HEIGHT;
            let first_block = state
                .store
                .block(&state.dashboard.recent_blocks[0])
                .unwrap();
            let tile_height =
                block_summary_tile_height(&theme, &state.store, first_block, gutter_x);
            let gap_y = first_block_y + tile_height - 1;
            let gutter = &buffer[(gutter_x, first_block_y)];
            assert_eq!(gutter.symbol(), " ");
            assert_eq!(gutter.bg, buffer[(tile_x, gap_y)].bg);
            assert_ne!(gutter.bg, buffer[(tile_x, first_block_y)].bg);
            assert!(matches!(
                buffer[(rail_x, first_block_y)].symbol(),
                "█" | "│"
            ));
        }
    }

    #[test]
    fn dashboard_requests_the_next_page_at_the_half_page_runway() {
        let mut dashboard = DashboardState {
            recent_blocks: (0..DASHBOARD_BLOCK_COUNT)
                .map(|height| format!("block-{height}"))
                .collect(),
            block_cursor: (DASHBOARD_BLOCK_COUNT / 2) as usize,
            ..DashboardState::default()
        };

        assert!(!dashboard_should_load_older(&dashboard));
        dashboard.block_cursor += 1;
        assert!(dashboard_should_load_older(&dashboard));

        dashboard.recent_blocks.extend(
            (DASHBOARD_BLOCK_COUNT..DASHBOARD_BLOCK_COUNT * 2)
                .map(|height| format!("block-{height}")),
        );
        assert!(!dashboard_should_load_older(&dashboard));
        dashboard.block_cursor =
            dashboard.recent_blocks.len() - (DASHBOARD_BLOCK_COUNT / 2) as usize + 1;
        assert!(dashboard_should_load_older(&dashboard));
    }

    #[tokio::test]
    async fn dashboard_end_requests_every_visible_transaction() {
        let mut state = regression_state();
        state.screen = Screen::Dashboard;
        let config = regression_config();
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        let txids: Vec<_> = (0..30).map(|index| format!("dash-tx-{index:02}")).collect();
        let blocks = (1..=20)
            .map(|height| {
                let mut block = test_block(height, &format!("block-{height}"), "");
                block.summary.tx = if height == 20 {
                    txids
                        .iter()
                        .map(|txid| TransactionSummary {
                            txid: txid.clone(),
                            raw: None,
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                block
            })
            .collect();
        merge_dashboard_blocks(&mut state, blocks);
        // Keep the block column's prefetch window away from these transactions.
        state.dashboard.block_cursor = state.dashboard.recent_blocks.len();
        state.dashboard.transactions_focused = true;
        select_summary_boundary(&mut state, true);

        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(160, 90)).unwrap();
        let mut viewport = None;
        terminal
            .draw(|frame| {
                viewport = render(frame, &config, &theme, &state, true);
            })
            .unwrap();
        let viewport = viewport.unwrap();
        let text: String = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect();
        let visible: Vec<_> = txids
            .iter()
            .filter(|txid| text.contains(txid.as_str()))
            .collect();
        assert!(visible.len() > TRANSACTION_LOAD_BATCH_SIZE);

        let (tx, _rx) = unbounded_channel();
        for _ in 0..visible.len().div_ceil(TRANSACTION_LOAD_BATCH_SIZE) {
            request_dashboard_transaction_hydration(&mut state, &config, &tx, viewport.clone());
        }
        let missing: Vec<_> = visible
            .into_iter()
            .filter(|txid| !state.loading_transactions.contains(*txid))
            .collect();
        assert!(
            missing.is_empty(),
            "Visible transactions never queued: {missing:?}"
        );
    }

    #[test]
    fn tip_status_shows_longest_chain_delta_and_recent_age() {
        let status = ChainTipStatus {
            label: "tip loaded".to_string(),
            healthy: true,
            height: Some(55_555),
            longest_chain: Some(55_558),
            last_block_time: Some(1_000),
            connections: Some(12),
        };

        assert_eq!(
            tip_status_text(&status, 1_123),
            "tip 55555/55558 (Δ3) | last 123s"
        );
    }

    #[test]
    fn summary_tiles_pad_both_sides_inside_the_tile_surface() {
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(10, 4)).unwrap();
        for selected in [false, true] {
            terminal
                .draw(|frame| {
                    render_summary_tile(
                        frame,
                        &theme,
                        SummaryTile {
                            title: None,
                            lines: vec![Line::raw("12345678")],
                            selected,
                            alignment: Alignment::Right,
                        },
                        frame.area(),
                    );
                })
                .unwrap();
            let buffer = terminal.backend().buffer();
            assert_eq!(buffer[(2, 0)].symbol(), "1");
            assert_eq!(buffer[(8, 0)].symbol(), "7");
            assert_eq!(buffer[(8, 1)].symbol(), "8");
            for x in [1, 9] {
                assert_eq!(buffer[(x, 0)].symbol(), " ");
                assert_eq!(Some(buffer[(x, 0)].bg), theme.tile_body(selected).bg);
            }
            assert_eq!(buffer[(9, 3)].bg, theme.panel);
        }
    }

    #[test]
    fn selected_tile_fills_ordinary_text_and_preserves_badges() {
        use ratatui::{Terminal, backend::TestBackend};
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        let mut terminal = Terminal::new(TestBackend::new(28, 2)).unwrap();
        for selected in [false, true] {
            terminal
                .draw(|frame| {
                    let line = Line::from(vec![
                        Span::styled("ID ", theme.link()),
                        Span::styled("Status", theme.status_badge()),
                    ]);
                    frame.render_widget(tile_paragraph(&theme, selected, vec![line]), frame.area());
                })
                .unwrap();
            let buffer = terminal.backend().buffer();
            assert_eq!(buffer[(0, 0)].bg, buffer[(27, 0)].bg);
            assert_eq!(buffer[(0, 0)].fg, theme.link);
            assert_eq!(Some(buffer[(3, 0)].bg), theme.status_badge().bg);
        }
    }

    #[test]
    fn wrapped_selector_preserves_cursor_column() {
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        for selected in [false, true] {
            let area = Rect::new(0, 0, 18, 2);
            let mut buffer = ratatui::buffer::Buffer::empty(area);
            ratatui::widgets::Widget::render(
                Paragraph::new(selection_line(&theme, selected, "Option label"))
                    .wrap(Wrap { trim: true }),
                area,
                &mut buffer,
            );
            assert_eq!(buffer[(2, 0)].symbol(), "O");
        }
    }

    #[test]
    fn peer_direction_labels_false_as_outbound() {
        assert_eq!(peer_direction(&json!({ "inbound": false })), "Outbound");
        assert_eq!(peer_direction(&json!({ "inbound": true })), "Inbound");
    }

    #[test]
    fn peer_addresses_parse_ipv4_and_bracketed_ipv6() {
        assert_eq!(
            peer_ip("136.243.31.96:27485"),
            Some("136.243.31.96".parse().unwrap())
        );
        assert_eq!(
            peer_ip("[2001:db8::1]:27485"),
            Some("2001:db8::1".parse().unwrap())
        );
        assert_eq!(peer_ip("example.onion:27485"), None);
    }

    #[test]
    fn peer_durations_include_each_significant_unit() {
        assert_eq!(human_duration(0), "0s");
        assert_eq!(human_duration(61), "1m 1s");
        assert_eq!(human_duration(90_061), "1d 1h 1m 1s");
        assert_eq!(human_duration(-12), "0s");
    }

    #[test]
    fn peer_ping_uses_milliseconds_below_one_second() {
        assert_eq!(format_ping_time(0.0424), "42ms");
        assert_eq!(format_ping_time(1.234), "1.23s");
    }

    #[test]
    fn fitted_content_has_no_scroll_range() {
        let lines = vec![Line::from("first"), Line::from("second")];
        assert_eq!(wrapped_max_scroll(&lines, 20, 2), 0);
        assert_eq!(wrapped_max_scroll(&lines, 20, 10), 0);
    }

    #[test]
    fn search_field_distinguishes_placeholder_input_and_status() {
        let mut state = regression_state();
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(100, 1)).unwrap();
        let render = |terminal: &mut Terminal<_>, state: &AppState| {
            terminal
                .draw(|frame| render_search_bar(frame, &theme, state, frame.area()))
                .unwrap();
        };

        render(&mut terminal, &state);
        let idle = terminal.backend().buffer();
        assert_eq!(idle[(3, 0)].symbol(), "S");
        assert_eq!(idle[(3, 0)].fg, theme.search_placeholder);
        assert_eq!(idle[(3, 0)].bg, idle[(99, 0)].bg);
        assert_ne!(idle[(3, 0)].bg, theme.background);
        let idle_background = idle[(3, 0)].bg;

        begin_search(&mut state);
        state.search.query = "42".into();
        render(&mut terminal, &state);
        let active = terminal.backend().buffer();
        assert_eq!(active[(3, 0)].symbol(), "4");
        assert_eq!(active[(3, 0)].fg, theme.text);
        assert_ne!(active[(3, 0)].bg, idle_background);
        assert_eq!(active[(5, 0)].bg, theme.accent);
        assert_eq!(active[(6, 0)].symbol(), " ");

        state.search.active = false;
        state.search.status = Some(SearchStatus {
            message: "No match".into(),
            error: true,
        });
        render(&mut terminal, &state);
        let status = terminal.backend().buffer();
        assert_eq!(status[(7, 0)].symbol(), "N");
        assert_eq!(status[(7, 0)].fg, theme.negative);
        assert_eq!(status[(7, 0)].bg, idle_background);
    }

    #[test]
    fn help_headings_and_search_examples_fit_narrow_terminals() {
        use ratatui::backend::TestBackend;

        let state = regression_state();
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        let mut terminal = Terminal::new(TestBackend::new(38, 128)).unwrap();
        terminal
            .draw(|frame| {
                render_help(
                    frame,
                    &theme,
                    &state,
                    &regression_config().cache_path,
                    0,
                    frame.area(),
                )
            })
            .unwrap();
        let rows: Vec<String> = terminal
            .backend()
            .buffer()
            .content
            .chunks(38)
            .map(|row| row[..37].iter().map(|cell| cell.symbol()).collect())
            .collect();
        assert_eq!(rows[3].trim_end(), "Search");
        assert!(rows[4].starts_with("/ focuses the search bar."));
        for example in [
            "VRSC@@",
            "unix:1700000000",
            "date:",
            "estimate:5000000",
            "F6 opens bookmarks",
        ] {
            assert!(
                rows.iter().any(|row| row.contains(example)),
                "{example}: {rows:?}"
            );
        }
        let mouse = rows
            .iter()
            .position(|row| row.trim_end() == "Mouse selection and paste")
            .unwrap();
        assert!(rows[mouse + 1].starts_with("Shift-M switches"));
        let mut terminal = Terminal::new(TestBackend::new(90, 1)).unwrap();
        terminal
            .draw(|frame| render_search_bar(frame, &theme, &state, frame.area()))
            .unwrap();
        let hint: String = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect();
        assert!(
            hint.contains(
                "block height/hash/unix time/datetime, TXID, name@, r/i-address, currency@@"
            ),
            "{hint}"
        );
    }

    #[test]
    fn help_footer_separator_stays_fixed_at_the_end_of_the_page() {
        let mut state = regression_state();
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        let config = regression_config();
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(100, 24)).unwrap();
        let area = Rect::new(0, 0, 100, 24);
        state.screen = block_screen(BlockTarget::Height(0));
        toggle_view_mode(&mut state);
        assert!(is_json_mode(&state));
        for scroll in [0, usize::MAX] {
            state.auxiliary = Some(AuxiliaryScreen::Help { scroll });
            clamp_scroll_positions(&mut state, &config, &theme, area, true);
            let divider = main_layout(&state, &theme, area, true)[3].bottom() - 1;
            terminal
                .draw(|frame| {
                    render(frame, &config, &theme, &state, true);
                })
                .unwrap();
            let buffer = terminal.backend().buffer();
            let line = |y| {
                (0..area.width)
                    .map(|x| buffer[(x, y)].symbol())
                    .collect::<String>()
            };
            assert_eq!(line(divider), "─".repeat(usize::from(area.width)));
            assert!(line(divider + 1).starts_with("controls j/k"));
            assert!(line(divider + 2).contains("F6 bookmarks"));
            if scroll == usize::MAX {
                assert!(line(divider - 1).contains("clear the local SQLite cache"));
            }
        }
    }

    #[test]
    fn wrapped_content_scroll_range_stops_at_the_last_line() {
        let lines = vec![Line::from("one two three four")];
        assert_eq!(wrapped_max_scroll(&lines, 6, 2), 3);
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(6, 2)).unwrap();
        terminal
            .draw(|frame| {
                let first = scroll_view::paragraph(
                    frame,
                    &theme,
                    Paragraph::new(lines).wrap(Wrap { trim: false }),
                    frame.area(),
                    usize::MAX,
                );
                assert_eq!(first, 3);
            })
            .unwrap();
        let buffer = terminal.backend().buffer();
        let last_line: String = (0..6).map(|x| buffer[(x, 1)].symbol()).collect();
        assert_eq!(last_line, "four █");
        assert_eq!(Some(buffer[(4, 1)].bg), theme.tile_gutter().bg);
    }

    #[test]
    fn peer_location_prefers_city_and_country_code() {
        let peer = PeerRecord {
            id: 1,
            ip: Some("136.243.31.96".parse().unwrap()),
            raw_json: json!({}),
            geo: Some(GeoLocation {
                city: Some("Falkenstein".to_string()),
                country_code: Some("DE".to_string()),
                country: Some("Germany".to_string()),
                ..GeoLocation::default()
            }),
        };
        assert_eq!(peer_location_label(&peer), "Falkenstein, DE");
    }

    #[test]
    fn unresolved_peer_positions_do_not_overlap_at_normal_counts() {
        let positions = (0..15)
            .map(|index| {
                let (longitude, latitude) = unresolved_peer_point(index, 5, 12.0);
                format!("{longitude},{latitude}")
            })
            .collect::<HashSet<_>>();
        assert_eq!(positions.len(), 15);
    }

    #[test]
    fn private_peer_positions_fill_a_south_pacific_region() {
        let positions = (0..12)
            .map(|index| private_peer_point(index, 12))
            .collect::<Vec<_>>();
        for (longitude, latitude) in &positions {
            assert!((-172.0..=-88.0).contains(longitude));
            assert!((-60.0..=-4.0).contains(latitude));
        }
        assert!(positions.iter().any(|(_, latitude)| *latitude < -45.0));
        assert_eq!(
            positions
                .iter()
                .map(|(longitude, latitude)| format!("{longitude},{latitude}"))
                .collect::<HashSet<_>>()
                .len(),
            12
        );
    }

    #[test]
    fn map_labels_reserve_later_pin_cells_and_omit_crowded_labels() {
        // At width 10 / height 7 these pins occupy (4, 3) and (5, 3).
        let pins = vec![
            PeerMapPin {
                point: (-20.0, 0.0),
                peer_id: 1,
                selected: false,
                kind: PeerMapPinKind::Located,
            },
            PeerMapPin {
                point: (20.0, 0.0),
                peer_id: 2,
                selected: true,
                kind: PeerMapPinKind::Located,
            },
        ];
        let labels = peer_map_label_cells(&pins, 10, 7);
        assert_eq!(labels[0], Some((5, 2))); // Same-row placement would cover pin 2.
        assert_eq!(labels[1], Some((6, 3)));
        for (width, height) in [(0, 0), (1, 1), (2, 7), (10, 1)] {
            assert!(
                peer_map_label_cells(&pins, width, height)
                    .iter()
                    .all(Option::is_none)
            );
        }
    }

    #[test]
    fn map_labels_never_cover_pins_or_other_labels_across_resizes() {
        let pins: Vec<_> = (0..32)
            .map(|id| PeerMapPin {
                point: (
                    -85.0 + (id % 8) as f64 * 12.0,
                    65.0 - (id / 8) as f64 * 16.0,
                ),
                peer_id: 100 + id,
                selected: id == 7,
                kind: PeerMapPinKind::Located,
            })
            .collect();
        for width in [1, 8, 28, 60, 120] {
            for height in [1, 3, 18, 35] {
                let mut occupied: HashSet<_> = pins
                    .iter()
                    .map(|pin| peer_map_pin_cell(pin, width, height))
                    .collect();
                for (pin, label) in pins.iter().zip(peer_map_label_cells(&pins, width, height)) {
                    if let Some((column, row)) = label {
                        assert!(row >= 1 && row < i32::from(height));
                        for offset in 0..pin.peer_id.to_string().len() as i32 + 2 {
                            assert!(column + offset < i32::from(width));
                            assert!(
                                occupied.insert((column + offset, row)),
                                "overlap at {width}x{height}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn map_pin_and_label_render_in_the_planned_terminal_cells() {
        use ratatui::backend::TestBackend;
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        let pins = vec![PeerMapPin {
            point: (-73.57, 45.50),
            peer_id: 123,
            selected: true,
            kind: PeerMapPinKind::Located,
        }];
        for (width, height) in [(28, 18), (60, 24), (120, 35)] {
            let area = Rect::new(3, 2, width, height);
            let mut terminal = Terminal::new(TestBackend::new(width + 6, height + 4)).unwrap();
            terminal
                .draw(|frame| render_peer_map_pins(frame, &theme, &pins, area))
                .unwrap();
            let buffer = terminal.backend().buffer();
            let (column, row) = peer_map_pin_cell(&pins[0], width, height);
            assert_eq!(
                buffer[(area.x + column as u16, area.y + row as u16)].symbol(),
                "●"
            );
            let (label_column, label_row) = peer_map_label_cells(&pins, width, height)[0].unwrap();
            assert_eq!((label_column, label_row), (column + 1, row));
            for (offset, ch) in "[123]".chars().enumerate() {
                assert_eq!(
                    buffer[(
                        area.x + label_column as u16 + offset as u16,
                        area.y + label_row as u16
                    )]
                        .symbol(),
                    ch.to_string()
                );
            }
        }
    }

    #[test]
    fn nearby_map_pins_receive_distinct_label_positions() {
        let pins = vec![
            PeerMapPin {
                point: (-73.57, 45.50),
                peer_id: 2,
                selected: false,
                kind: PeerMapPinKind::Located,
            },
            PeerMapPin {
                point: (-74.13, 40.83),
                peer_id: 5,
                selected: true,
                kind: PeerMapPinKind::Located,
            },
        ];
        let labels = peer_map_label_cells(&pins, 28, 18);
        assert_ne!(labels[0], labels[1]);
    }

    #[test]
    fn log_chunks_preserve_partial_lines_and_handle_reset() {
        let mut feed = test_log_feed_state();
        append_log_chunk(
            &mut feed,
            LogChunk {
                text: "one\ntw".to_string(),
                offset: 6,
                reset: false,
            },
        );
        append_log_chunk(
            &mut feed,
            LogChunk {
                text: "o\n".to_string(),
                offset: 8,
                reset: false,
            },
        );
        assert_eq!(
            feed.lines.iter().cloned().collect::<Vec<_>>(),
            ["one", "two"]
        );
        assert!(feed.partial.is_empty());

        append_log_chunk(
            &mut feed,
            LogChunk {
                text: "new\n".to_string(),
                offset: 4,
                reset: true,
            },
        );
        assert_eq!(feed.lines.iter().cloned().collect::<Vec<_>>(), ["new"]);
    }

    #[test]
    fn last_block_age_switches_to_minutes_at_ten_minutes() {
        assert_eq!(last_block_age_text(Some(1_000), 1_599), "599s");
        assert_eq!(last_block_age_text(Some(1_000), 1_600), "10:00");
        assert_eq!(last_block_age_text(Some(1_000), 1_665), "11:05");
    }

    #[test]
    fn short_ids_use_six_character_edges() {
        assert_eq!(
            short_id("0123456789abcdef0123456789abcdef"),
            "012345...abcdef"
        );
    }

    #[test]
    fn ambiguous_cached_hash_prefers_transaction() {
        let shared_hash = "a".repeat(64);
        let mut store = ObjectStore::default();
        store.insert_block(test_block(1, &shared_hash, "block-transaction"));
        store.insert_transaction(TransactionRecord {
            prepared: None,
            source_id: None,
            summary: TransactionSummary {
                txid: shared_hash.clone(),
                raw: Some(json!({ "txid": shared_hash })),
            },
            raw_json: json!({ "txid": shared_hash }),
            block_hash: None,
            block_height: None,
            block_time: 0,
        });

        assert_eq!(
            cached_hash_match(&store, &shared_hash),
            Some(CachedHashMatch::Transaction)
        );
    }

    #[test]
    fn block_summary_uses_blocktype_as_production_label() {
        let block = test_block(1, "hash", "txid");
        assert_eq!(block.summary.production_label(), "mined");

        let staked = BlockRecord {
            prepared_transactions: HashMap::new(),
            summary: BlockSummary {
                blocktype: Some("stake".to_string()),
                ..block.summary.clone()
            },
            ..block
        };
        assert_eq!(staked.summary.production_label(), "stake");
        assert_eq!(
            staked.summary.production_kind(),
            BlockProductionKind::Staked
        );
    }

    #[test]
    fn lightweight_block_does_not_seed_partial_transaction_cache() {
        let mut block = test_block(1, "hash", "txid");
        block.detail_loaded = false;
        block.summary.tx[0].raw = None;

        let mut store = ObjectStore::default();
        store.insert_block(block);

        assert!(store.block("hash").is_some());
        assert!(store.transaction("txid").is_none());
    }

    #[test]
    fn detailed_block_caches_complete_transaction_json() {
        let block = test_block(1, "hash", "txid");
        let mut store = ObjectStore::default();

        store.insert_block(block);

        assert_eq!(
            store.transaction("txid").map(|tx| &tx.raw_json),
            Some(&json!({ "txid": "txid" }))
        );
    }

    #[test]
    fn adjacent_navigation_prefers_header_hashes() {
        let mut block = test_block(10, "hash", "txid");
        block.summary.previousblockhash = Some("previous".to_string());
        block.summary.nextblockhash = Some("next".to_string());

        assert_eq!(
            adjacent_block_target(&block, Some(11), -1),
            Some(BlockTarget::Hash("previous".to_string()))
        );
        assert_eq!(
            adjacent_block_target(&block, Some(11), 1),
            Some(BlockTarget::Hash("next".to_string()))
        );
    }

    #[test]
    fn transaction_size_uses_verbose_size_or_raw_hex() {
        let verbose = TransactionSummary {
            txid: "verbose".to_string(),
            raw: Some(json!({ "size": 321 })),
        };
        let raw_hex = TransactionSummary {
            txid: "raw".to_string(),
            raw: Some(json!({ "hex": "00112233" })),
        };

        assert_eq!(transaction_size_label(&verbose), "321 bytes");
        assert_eq!(transaction_size_label(&raw_hex), "4 bytes");
    }

    #[test]
    fn coinbase_transactions_receive_coinbase_tag() {
        let transaction = TransactionSummary {
            txid: "coinbase".to_string(),
            raw: Some(json!({
                "vin": [{ "coinbase": "036c584000" }],
                "vout": []
            })),
        };

        assert_eq!(transaction.tags(), vec![TransactionTag::Coinbase]);
        assert_eq!(
            display_transaction_tags(&transaction, &ObjectStore::default()),
            ["Coinbase"]
        );
    }

    #[test]
    fn verus_stake_transactions_receive_stake_tag() {
        let transaction = TransactionSummary {
            txid: "stake".to_string(),
            raw: Some(json!({
                "vin": [{ "txid": "previous", "vout": 0 }],
                "vout": [
                    {
                        "valueSat": 16_925_377_268_323_i64,
                        "n": 0,
                        "scriptPubKey": {
                            "type": "pubkeyhash",
                            "addresses": ["RStaker"]
                        }
                    },
                    {
                        "valueSat": 0,
                        "n": 1,
                        "scriptPubKey": {
                            "type": "nulldata",
                            "spendableoutput": false,
                            "asm": "OP_RETURN 5642020000004e594000005a400005a9084a1403552b378d2a4fcc26f18205f9a97df560d4eeca5e7c8ab34eb8de02144c0113c6f53e49d5b5fb7e9eaaf3ea7417afc2b2",
                            "hex": "6a445642020000004e594000005a400005a9084a1403552b378d2a4fcc26f18205f9a97df560d4eeca5e7c8ab34eb8de02144c0113c6f53e49d5b5fb7e9eaaf3ea7417afc2b2"
                        }
                    }
                ]
            })),
        };

        assert_eq!(transaction.tags(), vec![TransactionTag::Stake]);
    }

    #[test]
    fn original_format_verus_stake_transactions_receive_stake_tag() {
        let inner_script = format!("520101010220{}", "00".repeat(32));
        let output_script = format!("6a{:02x}{inner_script}", inner_script.len() / 2);
        let transaction = TransactionSummary {
            txid: "original-stake".to_string(),
            raw: Some(json!({
                "vin": [{ "txid": "previous", "vout": 0 }],
                "vout": [
                    { "valueSat": 1, "n": 0, "scriptPubKey": { "type": "pubkeyhash" } },
                    { "valueSat": 0, "n": 1, "scriptPubKey": { "type": "nulldata", "hex": output_script } }
                ]
            })),
        };

        assert_eq!(transaction.tags(), vec![TransactionTag::Stake]);
    }

    #[test]
    fn stake_parameter_prefix_without_valid_payload_is_not_stake() {
        let transaction = TransactionSummary {
            txid: "invalid-stake-params".to_string(),
            raw: Some(json!({
                "vin": [{ "txid": "previous", "vout": 0 }],
                "vout": [
                    { "valueSat": 1, "n": 0, "scriptPubKey": { "type": "pubkeyhash" } },
                    { "valueSat": 0, "n": 1, "scriptPubKey": { "type": "nulldata", "hex": "6a0456420200" } }
                ]
            })),
        };

        assert!(transaction.tags().is_empty());
    }

    #[test]
    fn ordinary_zero_value_op_return_is_not_tagged_as_stake() {
        let transaction = TransactionSummary {
            txid: "op-return".to_string(),
            raw: Some(json!({
                "vin": [{ "txid": "previous", "vout": 0 }],
                "vout": [{
                    "valueSat": 0,
                    "n": 1,
                    "scriptPubKey": {
                        "type": "nulldata",
                        "asm": "OP_RETURN deadbeef",
                        "hex": "6a04deadbeef"
                    }
                }]
            })),
        };

        assert!(transaction.tags().is_empty());
    }

    #[test]
    fn unclassified_transactions_have_no_tags() {
        let transaction = TransactionSummary {
            txid: "plain".to_string(),
            raw: Some(json!({
                "vin": [{ "txid": "previous", "vout": 0 }],
                "vout": []
            })),
        };

        assert!(transaction.tags().is_empty());
    }

    #[test]
    fn unhydrated_transactions_have_no_blockchain_tags() {
        let transaction = TransactionSummary {
            txid: "unhydrated".to_string(),
            raw: None,
        };

        assert!(transaction.tags().is_empty());
        assert!(display_transaction_tags(&transaction, &ObjectStore::default()).is_empty());
    }

    #[test]
    fn defi_tags_resolve_pool_names_and_remain_attached_to_outputs() {
        let raw = json!({
            "vin": [{ "txid": "funding", "vout": 0 }],
            "vout": [{
                "n": 0,
                "scriptPubKey": {
                    "type": "cryptocondition",
                    "reservetransfer": {
                        "currencyvalues": { "iVRSC": 10.0 },
                        "convert": true,
                        "destinationcurrencyid": "iPool"
                    }
                }
            }]
        });
        let transaction = TransactionSummary {
            txid: "conversion".to_string(),
            raw: Some(raw.clone()),
        };
        let mut store = ObjectStore::default();
        store.insert_currency_definition(CurrencyDefinitionRecord {
            currency_id: "iPool".to_string(),
            name: "Bridge.vETH".to_string(),
            definition_json: json!({ "currencyid": "iPool" }),
        });

        assert_eq!(
            display_transaction_tags(&transaction, &store),
            vec!["Reserve Transfer", "Conversion: Bridge.vETH"]
        );
        let flows = transaction_flows(&raw, &store);
        let output = flows
            .iter()
            .find(|flow| flow.kind == TransactionFlowKind::Output)
            .expect("output flow");
        assert_eq!(output.tags.len(), 2);
        assert_eq!(
            display_output_tag(&output.tags[1], &store),
            "Conversion: Bridge.vETH"
        );
    }

    #[test]
    fn emission_timelock_tag_and_height_attach_to_locked_value_output() {
        let raw = json!({
            "vin": [{ "coinbase": "02c07b0101" }],
            "vout": [
                { "n": 0, "valueSat": 38_400_000_000_u64, "scriptPubKey": {
                    "type": "scripthash",
                    "hex": "a914c211e525380231b5dd364d4fed795af49281ec7c87"
                }},
                { "n": 1, "valueSat": 0, "scriptPubKey": {
                    "type": "nulldata",
                    "hex": "6a200103cf610fb17576a914630bcfd6abe8125b6a9aeeb4fde7f8801c7a6de688ac"
                }}
            ]
        });

        let flows = transaction_flows(&raw, &ObjectStore::default());
        let outputs = flows
            .iter()
            .filter(|flow| flow.kind == TransactionFlowKind::Output)
            .collect::<Vec<_>>();
        assert_eq!(outputs.len(), 2);
        assert_eq!(outputs[0].tags[0].kind, OutputTagKind::EmissionTimelock);
        assert_eq!(outputs[0].annotations, ["Unlock height 1008079"]);
        assert!(outputs[1].tags.is_empty());
        assert!(outputs[1].annotations.is_empty());
    }

    #[test]
    fn block_tags_aggregate_interesting_transaction_tags_only() {
        let mut block = test_block(1, "block", "coinbase-import");
        block.summary.tx[0].raw = Some(json!({
            "txid": "coinbase-import",
            "vin": [{ "coinbase": "00" }],
            "vout": [{ "scriptPubKey": { "crosschainimport": {
                "importcurrencyid": "iPool"
            }}}]
        }));
        let mut store = ObjectStore::default();
        store.insert_currency_definition(CurrencyDefinitionRecord {
            currency_id: "iPool".to_string(),
            name: "Bridge.vETH".to_string(),
            definition_json: json!({ "currencyid": "iPool" }),
        });
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        let labels = block_tag_spans(&theme, &block, &store)
            .into_iter()
            .map(|span| span.content.into_owned())
            .collect::<String>();

        assert!(!labels.contains(TransactionTag::Coinbase.label()));
        assert!(labels.contains("Import: Bridge.vETH"));
    }

    #[test]
    fn block_summary_tiles_grow_only_when_tags_need_space() {
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        let store = ObjectStore::default();
        let plain = test_block(1, "plain", "plain-tx");
        let mut tagged = test_block(2, "tagged", "tagged-tx");
        tagged.summary.tx[0].raw = Some(json!({
            "vin": [{ "txid": "source", "vout": 0 }],
            "vout": [{ "scriptPubKey": { "crosschainexport": {
                "destinationcurrencyid": "iPool"
            }}}]
        }));

        assert_eq!(block_summary_tile_height(&theme, &store, &plain, 80), 4);
        assert!(block_summary_tile_height(&theme, &store, &tagged, 80) > 4);
    }

    #[test]
    fn identity_name_commitment_outputs_receive_a_tag() {
        let transaction = TransactionSummary {
            txid: "commitment".to_string(),
            raw: Some(json!({
                "vin": [{ "txid": "funding", "vout": 5 }],
                "vout": [{
                    "valueZat": 0,
                    "n": 0,
                    "scriptPubKey": {
                        "type": "cryptocondition",
                        "commitmenthash": {
                            "version": 1,
                            "currencyvalues": {},
                            "hash": "82700075385210e1e3ef7fedc6ab0bf110101a75d4cef230e74273d8ede2d88d"
                        }
                    }
                }]
            })),
        };

        assert_eq!(transaction.tags(), vec![TransactionTag::NameCommitment]);
    }

    #[test]
    fn commitment_text_outside_an_output_script_does_not_receive_a_tag() {
        let transaction = TransactionSummary {
            txid: "not-a-commitment".to_string(),
            raw: Some(json!({
                "commitmenthash": "metadata only",
                "vin": [],
                "vout": []
            })),
        };

        assert!(transaction.tags().is_empty());
    }

    #[test]
    fn protocol_fees_are_not_inferred_from_visible_delta() {
        let raw = json!({"vin":[{"valueZat":100}], "vout":[{"n":0,"valueSat":300,
            "scriptPubKey":{"crosschainimport":{"version":1}}}]});
        let store = ObjectStore::default();
        let (labels, fee) =
            transaction_values_labels(&raw, &transaction_flows(&raw, &store), "VRSC", &store, None);
        assert_eq!(fee, "unknown (protocol accounting)");
        assert!(
            labels
                .iter()
                .any(|span| span.content.contains("Public native delta"))
        );
        // The same rule applies when protocol evidence appears only in a spent output.
        let input = json!({"vin":[{"prevout":{"scriptPubKey":{"reservedeposit":{}}}}],"vout":[]});
        assert!(protocol_value_accounting(&input, &store));
    }

    #[test]
    fn reserve_transfer_custody_includes_fees_but_not_mint_principal_or_native_twice() {
        let mut store = ObjectStore::default();
        store.chain.chain_id = Some("native".into());
        let mut output = json!({"scriptPubKey":{"reservetransfer":{"flags":1,
            "currencyvalues":{"reserve":10,"native":3},"feecurrencyid":"reserve","fees":0.1,
            "destination":{"type":130,"gateway":"native","fees":0.2}},
            "reserve_balance":{"display-name":999}}});
        assert_eq!(
            monetary_reserve_amounts(&output, &store),
            BTreeMap::from([("reserve".into(), 1_030_000_000)])
        );
        output["scriptPubKey"]["reservetransfer"]["flags"] = json!(33);
        assert_eq!(
            monetary_reserve_amounts(&output, &store),
            BTreeMap::from([("reserve".into(), 30_000_000)])
        );
        output["scriptPubKey"]["reservetransfer"]["destination"]["type"] = json!(2);
        assert_eq!(
            monetary_reserve_amounts(&output, &store),
            BTreeMap::from([("reserve".into(), 10_000_000)])
        );
    }

    #[test]
    fn missing_sapling_public_balance_remains_unknown() {
        let mut raw = json!({"vShieldedSpend":[{"nullifier":"opaque"}]});
        assert_eq!(
            ShieldedAccounting::from_raw(&raw).net_to_transparent_zat(),
            None
        );
        let spans = shielded_accounting_spans(
            ShieldedAccounting::from_raw(&raw),
            "VRSC",
            &Theme::desert(crate::cli::EmojiMode::Off),
        )
        .unwrap();
        assert!(
            spans
                .iter()
                .any(|span| span.content.contains("balance unavailable"))
        );
        for malformed in [
            json!({"vShieldedSpend":"unavailable"}),
            json!({"vShieldedOutput":null}),
            json!({"vJoinSplit":{}}),
        ] {
            assert_eq!(
                ShieldedAccounting::from_raw(&malformed).net_to_transparent_zat(),
                None
            );
        }
        raw["valueBalanceZat"] = json!(17);
        assert_eq!(
            ShieldedAccounting::from_raw(&raw).net_to_transparent_zat(),
            Some(17)
        );
        raw["valueBalanceZat"] = json!("malformed");
        raw["valueBalance"] = json!(0);
        assert_eq!(
            ShieldedAccounting::from_raw(&raw).net_to_transparent_zat(),
            None
        );
        let mixed = json!({"vShieldedSpend":[{}],"valueBalanceZat":17,"vJoinSplit":{}});
        let accounting = ShieldedAccounting::from_raw(&mixed);
        assert!(accounting.sapling_complete && !accounting.sprout_complete);
        assert_eq!(accounting.net_to_transparent_zat(), None);
        let spans = shielded_accounting_spans(
            accounting,
            "VRSC",
            &Theme::desert(crate::cli::EmojiMode::Off),
        )
        .unwrap();
        assert!(
            spans
                .iter()
                .any(|span| span.content.contains("Sprout public balance unavailable"))
        );
        assert!(
            !spans
                .iter()
                .any(|span| span.content.contains("Sapling public balance unavailable"))
        );
    }

    #[test]
    fn sapling_private_components_receive_directional_tags_and_flows() {
        let raw = json!({
            "vin": [{ "txid": "transparent", "vout": 0, "valueZat": 6_539_929_854_i64 }],
            "vout": [{ "n": 0, "valueZat": 1 }],
            "valueBalance": -65.39919854,
            "vShieldedSpend": [{ "nullifier": "one" }, { "nullifier": "two" }],
            "vShieldedOutput": [{ "cmu": "three" }]
        });
        let transaction = TransactionSummary {
            txid: "private".to_string(),
            raw: Some(raw.clone()),
        };

        assert_eq!(
            transaction.tags(),
            vec![TransactionTag::PrivateInput, TransactionTag::PrivateOutput]
        );
        let flows = transaction_flows(&raw, &ObjectStore::default());
        assert_eq!(flows.iter().filter(|flow| flow.private).count(), 3);
        assert_eq!(
            flows
                .iter()
                .filter(|flow| flow.private && flow.kind == TransactionFlowKind::Input)
                .count(),
            2
        );
        assert!(flows.iter().filter(|flow| flow.private).all(|flow| {
            flow.value_zat.is_none()
                && flow.value_label == "hidden"
                && flow.script_type.as_deref() == Some("Sapling")
        }));

        let accounting = ShieldedAccounting::from_raw(&raw);
        assert!(accounting.complete);
        assert_eq!(accounting.sapling_value_balance_zat, -6_539_919_854);
        assert_eq!(accounting.net_to_transparent_zat(), Some(-6_539_919_854));
    }

    #[test]
    fn sprout_joinsplits_count_nullifiers_and_commitments_as_private_flows() {
        let raw = json!({
            "vin": [],
            "vout": [],
            "vjoinsplit": [{
                "nullifiers": ["one", "two"],
                "commitments": ["three", "four"]
            }]
        });

        assert_eq!(private_input_count(&raw), 2);
        assert_eq!(private_output_count(&raw), 2);
        assert_eq!(transaction_flows(&raw, &ObjectStore::default()).len(), 4);
    }

    #[test]
    fn shielded_pool_boundaries_are_included_in_transaction_fees() {
        let shielding = json!({
            "vin": [{ "valueZat": 6_539_929_854_i64 }],
            "vout": [],
            "valueBalance": -65.39919854,
            "vShieldedSpend": [],
            "vShieldedOutput": [{ "cmu": "shielded-note" }]
        });
        let shielding_flows = transaction_flows(&shielding, &ObjectStore::default());
        let shielded_output = shielding_flows
            .iter()
            .find(|flow| flow.private && flow.kind == TransactionFlowKind::Output)
            .expect("shielded output");
        assert_eq!(shielded_output.value_zat, Some(6_539_919_854));
        assert_eq!(
            shielded_output.annotations,
            vec!["Derived from Sapling valueBalance"]
        );
        let (_, shielding_fee) = transaction_values_labels(
            &shielding,
            &shielding_flows,
            "VRSC",
            &ObjectStore::default(),
            None,
        );
        assert_eq!(shielding_fee, "0.00010000 VRSC");

        let unshielding = json!({
            "vin": [],
            "vout": [{ "n": 0, "valueZat": 1_199_980_000_i64 }],
            "valueBalance": 11.99990000,
            "vShieldedSpend": [{ "nullifier": "shielded-note" }],
            "vShieldedOutput": []
        });
        let unshielding_flows = transaction_flows(&unshielding, &ObjectStore::default());
        let shielded_input = unshielding_flows
            .iter()
            .find(|flow| flow.private && flow.kind == TransactionFlowKind::Input)
            .expect("shielded input");
        assert_eq!(shielded_input.value_zat, Some(1_199_990_000));
        let (_, unshielding_fee) = transaction_values_labels(
            &unshielding,
            &unshielding_flows,
            "VRSC",
            &ObjectStore::default(),
            None,
        );
        assert_eq!(unshielding_fee, "0.00010000 VRSC");
    }

    #[test]
    fn sapling_individual_values_stay_hidden_when_only_an_aggregate_is_known() {
        let raw = json!({
            "vin": [],
            "vout": [],
            "valueBalance": -3.0,
            "vShieldedSpend": [],
            "vShieldedOutput": [{ "cmu": "one" }, { "cmu": "two" }]
        });

        let flows = transaction_flows(&raw, &ObjectStore::default());
        assert!(
            flows
                .iter()
                .filter(|flow| flow.private)
                .all(|flow| flow.value_zat.is_none())
        );
        assert_eq!(
            ShieldedAccounting::from_raw(&raw).sapling_value_balance_zat,
            -300_000_000
        );
    }

    #[test]
    fn sprout_public_values_use_integer_zat_fields_for_accounting() {
        let raw = json!({
            "vin": [{ "valueZat": 200_000_000 }],
            "vout": [{ "n": 0, "valueZat": 124_990_000 }],
            "vjoinsplit": [{
                "vpub_old": 0.75,
                "vpub_oldZat": 75_000_000,
                "vpub_new": 0.0,
                "vpub_newZat": 0,
                "nullifiers": ["one", "two"],
                "commitments": ["three", "four"]
            }]
        });
        let accounting = ShieldedAccounting::from_raw(&raw);
        assert_eq!(accounting.transparent_to_sprout_zat, 75_000_000);
        assert_eq!(accounting.sprout_to_transparent_zat, 0);
        assert_eq!(accounting.net_to_transparent_zat(), Some(-75_000_000));

        let flows = transaction_flows(&raw, &ObjectStore::default());
        let (_, fee) =
            transaction_values_labels(&raw, &flows, "VRSC", &ObjectStore::default(), None);
        assert_eq!(fee, "0.00010000 VRSC");
    }

    #[test]
    fn address_tiles_include_and_prioritize_private_flows() {
        let transaction = TransactionRecord {
            prepared: None,
            source_id: None,
            summary: TransactionSummary {
                txid: "private".to_string(),
                raw: None,
            },
            raw_json: json!({
                "vin": [{ "address": "ROther" }],
                "vout": [{ "n": 0, "valueZat": 1, "scriptPubKey": { "addresses": ["ROther"] } }],
                "vShieldedSpend": [{ "nullifier": "one" }],
                "vShieldedOutput": [{ "cmu": "two" }]
            }),
            block_hash: None,
            block_height: None,
            block_time: 0,
        };

        let (inputs, outputs) =
            address_tile_flows(&transaction, &ObjectStore::default(), "VRSC", "RViewed");

        assert!(inputs[0].private);
        assert!(outputs[0].private);
        assert_eq!(
            address_transaction_tile_height(&transaction, &ObjectStore::default(), 160),
            5
        );
    }

    #[test]
    fn json_viewport_holds_at_both_ends_until_cursor_reaches_the_middle() {
        let mut json = JsonState::new();
        json.cursor = 99;
        assert_eq!(json.viewport_start(11, 100), 89);
        for cursor in (94..99).rev() {
            json.cursor = cursor;
            assert_eq!(json.viewport_start(11, 100), 89);
            assert_eq!(json.viewport_start(11, 100), 89, "redraw must not drift");
        }
        json.cursor = 93;
        assert_eq!(json.viewport_start(11, 100), 88);
        json.cursor = 0;
        assert_eq!(json.viewport_start(11, 100), 0);
        for cursor in 1..=5 {
            json.cursor = cursor;
            assert_eq!(json.viewport_start(11, 100), 0);
        }
        json.cursor = 6;
        assert_eq!(json.viewport_start(11, 100), 1);
        json.cursor = 5;
        assert_eq!(json.viewport_start(11, 100), 0);
        // Shrinking/collapsing and resizing cannot leave an empty tail.
        json.cursor = 3;
        assert_eq!(json.viewport_start(11, 4), 0);
        assert_eq!(json.viewport_start(1, 4), 3);
    }

    #[test]
    fn expanded_long_json_string_remains_one_scalar_entry() {
        let raw_hex = "ab".repeat(JSON_STRING_PREVIEW_CHARS);
        let document = json!({ "hex": raw_hex });
        let mut json = JsonState::new();
        json.expanded.insert("$.hex".to_string());

        let entries = json.visible_entries(&document);

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[1].path, "$.hex");
        assert_eq!(entries[1].scalar_value.as_deref(), Some(raw_hex.as_str()));
        assert!(entries.iter().all(|entry| !entry.path.contains("chunk")));
    }

    #[test]
    fn transaction_flows_resolve_prevout_address_and_value_from_store() {
        let mut store = ObjectStore::default();
        let previous_raw = json!({
            "txid": "previous",
            "vout": [{
                "n": 0,
                "value": 1.25,
                "valueZat": 125_000_000,
                "scriptPubKey": {
                    "type": "pubkeyhash",
                    "addresses": ["RResolvedAddress"]
                }
            }]
        });
        store.insert_transaction(TransactionRecord {
            prepared: None,
            source_id: None,
            summary: TransactionSummary {
                txid: "previous".to_string(),
                raw: Some(previous_raw.clone()),
            },
            raw_json: previous_raw,
            block_hash: None,
            block_height: None,
            block_time: 0,
        });
        let current = json!({
            "vin": [{ "txid": "previous", "vout": 0, "sequence": 1 }],
            "vout": [{ "n": 0, "valueZat": 124_000_000 }]
        });

        let flows = transaction_flows(&current, &store);

        assert_eq!(flows[0].addresses, vec!["RResolvedAddress"]);
        assert_eq!(flows[0].value_zat, Some(125_000_000));
        assert_eq!(flows[0].script_type.as_deref(), Some("pubkeyhash"));
        assert_eq!(
            sum_known_values(&[&flows[0]])
                .zip(sum_known_values(&[&flows[1]]))
                .map(|(inputs, outputs)| inputs - outputs),
            Some(1_000_000)
        );
    }

    #[test]
    fn adjacent_block_browsing_replaces_current_breadcrumb_node() {
        let mut current = test_block(10, "current", "current-tx");
        current.summary.nextblockhash = Some("next".to_string());
        let next = test_block(11, "next", "next-tx");
        let mut store = ObjectStore::default();
        store.insert_block(current);
        store.insert_block(next);
        let mut state = AppState {
            route_label: "chain tip".to_string(),
            tip: ChainTipStatus {
                label: "tip 11".to_string(),
                healthy: true,
                height: Some(11),
                longest_chain: Some(11),
                last_block_time: Some(11),
                connections: Some(3),
            },
            store,
            dashboard: DashboardState::default(),
            mempool: MempoolState::default(),
            marketplace: MarketplaceState::default(),
            screen: Screen::Block {
                target: BlockTarget::Hash("current".to_string()),
                selected_tx: 0,
                mode: ViewMode::Overview,
                json: JsonState::new(),
            },
            screen_stack: vec![Screen::Dashboard],
            loading_blocks: BTreeSet::new(),
            loading_transactions: BTreeSet::new(),
            transaction_retry_after: HashMap::new(),
            transaction_verified_tip: HashMap::new(),
            loading_addresses: BTreeSet::new(),
            loading_address_histories: BTreeSet::new(),
            loading_address_utxos: BTreeSet::new(),
            loading_address_balances: BTreeSet::new(),
            address_errors: HashMap::new(),
            address_retry_after: HashMap::new(),
            address_history_errors: HashMap::new(),
            address_history_retry_after: HashMap::new(),
            address_utxo_errors: HashMap::new(),
            address_utxo_retry_after: HashMap::new(),
            address_balance_errors: HashMap::new(),
            address_balance_retry_after: HashMap::new(),
            loading_outpoints: HashSet::new(),
            loading_identities: HashSet::new(),
            identity_load_attempted: HashSet::new(),
            loading_identity_names: HashSet::new(),
            identity_name_attempted: HashSet::new(),
            loading_address_identities: HashSet::new(),
            address_identities_attempted: HashSet::new(),
            loading_currencies: HashSet::new(),
            currency_load_attempted: HashSet::new(),
            unmask_identity_addresses: false,
            notice: None,
            clipboard_busy: false,
            save_prompt: None,
            cache_clear_prompt: None,
            cache_size_bytes: Some(0),
            search: SearchState::default(),
            auxiliary: None,
            help_return: None,
            peers: test_peer_state(),
            log_feed: test_log_feed_state(),
        };
        let config = Config {
            chain: crate::config::ChainSelection {
                system_id: std::sync::Arc::new(std::sync::OnceLock::from(
                    crate::world::SystemId::try_from("iTestSystem".to_string()).unwrap(),
                )),
                name: "VRSC".to_string(),
                testnet: false,
            },
            daemon: crate::config::DaemonConfig {
                host: "127.0.0.1".to_string(),
                port: 1,
                user: None,
                password: None,
                config_file: None,
                observations: None,
                control: Default::default(),
                api_pool: Default::default(),
                background: false,
            },
            api_consent: true,
            cache_path: std::path::PathBuf::new(),
            emoji: crate::cli::EmojiMode::Off,
            poll_interval_seconds: 3,
        };
        let (tx, _rx) = unbounded_channel();

        navigate_adjacent_block(&mut state, &config, &tx, 1);

        assert_eq!(state.screen_stack.len(), 1);
        assert!(matches!(
            state.screen,
            Screen::Block {
                target: BlockTarget::Hash(ref hash),
                ..
            } if hash == "next"
        ));

        navigate_adjacent_block(&mut state, &config, &tx, 1);
        assert_eq!(state.screen_stack.len(), 1);
        assert_eq!(
            state.notice.as_ref().map(|notice| notice.message.as_str()),
            Some("Already at chain tip")
        );
    }

    #[test]
    fn output_amounts_include_native_and_every_reserve_currency() {
        let output = json!({
            "value": 1.25,
            "scriptPubKey": {
                "currencyvalues": {
                    "iCurrencyOne": "2.50000001",
                    "iCurrencyTwo": { "valueZat": 42 }
                }
            }
        });

        let amounts = output_currency_amounts(&output, "VRSC");

        assert_eq!(amounts.get("VRSC"), Some(&125_000_000));
        assert_eq!(amounts.get("iCurrencyOne"), Some(&250_000_001));
        assert_eq!(amounts.get("iCurrencyTwo"), Some(&42));
    }

    #[test]
    fn address_flows_put_the_viewed_address_first() {
        let transaction = TransactionRecord {
            prepared: None,
            source_id: None,
            summary: TransactionSummary {
                txid: "current".to_string(),
                raw: None,
            },
            raw_json: json!({
                "txid": "current",
                "vin": [],
                "vout": [
                    { "n": 0, "valueZat": 1, "scriptPubKey": { "addresses": ["ROther"] } },
                    { "n": 1, "valueZat": 2, "scriptPubKey": { "addresses": ["RViewed"] } },
                    { "n": 2, "valueZat": 3, "scriptPubKey": { "addresses": ["RViewed", "RSecond"] } }
                ]
            }),
            block_hash: None,
            block_height: None,
            block_time: 0,
        };

        let (_, outputs) =
            address_tile_flows(&transaction, &ObjectStore::default(), "VRSC", "RViewed");

        assert_eq!(
            outputs.iter().map(|flow| flow.index).collect::<Vec<_>>(),
            [1, 2, 0]
        );
        assert_eq!(outputs[1].addresses, ["RViewed", "RSecond"]);
    }

    #[test]
    fn address_utxos_accept_daemon_index_field_variants() {
        for (field, index) in [
            ("outputIndex", 1_u64),
            ("outputindex", 2),
            ("vout", 3),
            ("index", 4),
        ] {
            let mut utxo = serde_json::Map::new();
            utxo.insert("txid".to_string(), Value::from("txid"));
            utxo.insert(field.to_string(), Value::from(index));

            assert_eq!(
                utxo_outpoint(&Value::Object(utxo)),
                Some(OutPoint {
                    txid: "txid".to_string(),
                    index,
                })
            );
        }
    }

    #[test]
    fn address_transaction_tiles_shrink_to_their_flow_content() {
        assert_eq!(address_transaction_tile_height_for_counts(160, 1, 1), 4);
        assert_eq!(address_transaction_tile_height_for_counts(159, 1, 1), 6);
        assert_eq!(address_transaction_tile_height_for_counts(160, 7, 2), 10);
        assert_eq!(address_transaction_tile_height_for_counts(159, 7, 2), 13);
    }

    #[test]
    fn address_index_is_visible_before_transactions_are_hydrated() {
        let address = "RAddress".to_string();
        let newest = "newest".to_string();
        let older = "older".to_string();
        let mut store = ObjectStore::default();

        insert_address_load(
            &mut store,
            AddressLoadResult {
                address: AddressRecord {
                    address: address.clone(),
                    transaction_count: 2,
                    transaction_ids: vec![newest.clone(), older.clone()],
                    first_txid: Some(older),
                    latest_txid: Some(newest.clone()),
                    unspent_outpoints: HashSet::new(),
                    balance_response: None,
                    utxos_complete: false,
                    history_complete: false,
                },
            },
        );

        let record = store.address(&address).expect("address index inserted");
        assert_eq!(record.transaction_ids[0], newest);
        assert!(store.transaction(&record.transaction_ids[0]).is_none());
    }

    #[tokio::test]
    async fn daemon_name_hints_avoid_extra_name_fetches_and_do_not_guess_ambiguous_maps() {
        let id = "i6j1rzjgrDhSmUYiTtp21J8Msiudv5hgt9";
        let other = "i7neKkvaZPnvfL6bcCnpe32vim4kaMDbWQ";
        let mut state = regression_state();
        state.store.learn_currency_data(&json!({"scriptPubKey":{
            "reserveoutput":{"currencyvalues":{(id):2}},"reserve_balance":{"Bridge":2}}}));
        assert_eq!(state.store.currency_name(id), Some("Bridge"));
        assert!(!state.store.currencies_by_id.contains_key(id));
        let (tx, _rx) = unbounded_channel();
        request_currency_ids(&mut state, &regression_config(), &tx, vec![id.into()]);
        assert!(state.loading_currencies.is_empty());
        request_identity_names(
            &mut state,
            &regression_config(),
            &tx,
            BTreeSet::from([id.into()]),
        );
        assert!(state.loading_identity_names.is_empty());
        let mut hints = HashMap::new();
        collect_currency_name_hints(
            &json!({"reserveoutput":{"currencyvalues":{(id):2,(other):2}},
            "reserve_balance":{"one":2,"two":2}}),
            &mut hints,
        );
        assert!(hints.is_empty());
        collect_currency_name_hints(&json!({"currencynames":{(id):id}}), &mut hints);
        assert!(hints.is_empty());
        state
            .store
            .insert_currency_definition(CurrencyDefinitionRecord {
                currency_id: id.into(),
                name: "Bridge.vDEX".into(),
                definition_json: Value::Null,
            });
        assert_eq!(state.store.currency_name(id), Some("Bridge.vDEX"));
    }

    #[test]
    fn cached_currency_names_are_shared_by_amount_json_marketplace_and_trail_labels() {
        let mut store = ObjectStore::default();
        let id = "i6j1rzjgrDhSmUYiTtp21J8Msiudv5hgt9";
        let name = "Long Reserve🦉.vDEX";
        store.insert_currency_definition(CurrencyDefinitionRecord {
            currency_id: id.into(),
            name: name.into(),
            definition_json: json!({"currencyid":id,"name":name}),
        });
        assert_eq!(
            currency_amount_label(currency_display_value(&store, id), 100_000_000),
            format!("1 {name}")
        );
        assert!(marketplace_asset_label(&json!({(id):1}), &store).contains(name));
        let document = json!({"currencyid":id});
        let entries = JsonState::new().visible_entries(&document);
        let entry = entries
            .iter()
            .find(|entry| entry.label == "currencyid")
            .unwrap();
        assert!(json_entry_display_value(entry, &store, false).contains(name));
        assert!(json_entry_display_value(entry, &store, true).contains(id));
        let mut trail = crate::defi::Trail::default();
        trail.notes.push(format!("fee currency {id}"));
        let mut panel = DefiPanel::default();
        panel.report.replace(trail);
        assert!(
            defi_lines(&panel, &store, &Theme::desert(crate::cli::EmojiMode::Off))
                .iter()
                .any(|line| line.spans.iter().any(|span| span.content.contains(name)))
        );
        assert_eq!(document["currencyid"], id);
    }

    #[test]
    fn native_labels_use_daemon_name_for_id_or_hex_chain_invocations() {
        let mut config = regression_config();
        config.chain.name = "53fe39eea8c06bba32f1a4e20db67e5524f0309d".into();
        let mut store = ObjectStore::default();
        store.chain.chain_name = Some("vDEX".into());
        assert_eq!(native_currency_name(&config, &store), "vDEX");
    }

    #[test]
    fn identity_activity_burn_inputs_show_currency_names() {
        let mut store = ObjectStore::default();
        let bridge = "i6j1rzjgrDhSmUYiTtp21J8Msiudv5hgt9";
        let pump = "i7neKkvaZPnvfL6bcCnpe32vim4kaMDbWQ";
        let address = "iExBJfZYK7KREDpuhj6PzZBzqMAKaFg7d2";
        for (id, name) in [(bridge, "Bridge.vDEX"), (pump, "PUMP")] {
            store.insert_currency_definition(CurrencyDefinitionRecord {
                currency_id: id.into(),
                name: name.into(),
                definition_json: json!({"name":name}),
            });
        }
        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/vdex-burn-prevouts.json"))
                .unwrap();
        let sources = fixture["sources"].as_array().unwrap();
        for source in sources {
            let mut outputs = vec![json!({}); 7];
            for output in source["vout"].as_array().unwrap() {
                outputs[output["n"].as_u64().unwrap() as usize] = output.clone();
            }
            let txid = source["txid"].as_str().unwrap();
            store.insert_transaction(transaction_record(
                txid,
                json!({"txid":txid,"vout":outputs}),
            ));
        }
        let transaction = transaction_record(
            "burn",
            json!({"txid":"burn","vin":[
            {"txid":sources[0]["txid"],"vout":5,"valueSat":0},{"txid":sources[0]["txid"],"vout":6,"valueSat":0},
            {"txid":sources[1]["txid"],"vout":0,"valueSat":10000}],
            "vout":[{"n":0,"valueSat":0,"scriptPubKey":{"type":"cryptocondition","evalnone":"valid","spendableoutput":true,"addresses":[address]}}]}),
        );
        let flows = transaction_flows(&transaction.raw_json, &store);
        let (summary, _) =
            transaction_values_labels(&transaction.raw_json, &flows, "vDEX", &store, None);
        let summary: String = summary.iter().map(|s| s.content.as_ref()).collect();
        assert!(summary.contains("12.12500001 Bridge.vDEX"), "{summary}");
        assert!(summary.contains("34.37500002 PUMP"), "{summary}");
        let tags = display_transaction_tags(&transaction.summary, &store);
        assert!(tags.contains(&"Consumption Burn".into()));
        assert!(!tags.contains(&"Burn".into()));
        let (inputs, _) = address_tile_flows(&transaction, &store, "vDEX", address);
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        for width in [80, 160] {
            let height = address_transaction_tile_height(&transaction, &store, width);
            let mut terminal =
                Terminal::new(ratatui::backend::TestBackend::new(width, height)).unwrap();
            let context = AddressTileRenderContext {
                theme: &theme,
                store: &store,
                viewed_address: address,
                native_currency: "vDEX",
                unmask_identity_addresses: false,
            };
            terminal
                .draw(|frame| {
                    render_address_transaction_tile(
                        frame,
                        &context,
                        &transaction,
                        true,
                        frame.area(),
                    )
                })
                .unwrap();
            let text: String = terminal
                .backend()
                .buffer()
                .content
                .iter()
                .map(|cell| cell.symbol())
                .collect();
            assert!(text.contains(" Consumption Burn "), "{width}: {text}");
            assert!(text.contains("34.37500002 PUMP"), "{width}: {text}");
            assert!(text.contains("OUTPUTS"), "{width}: {text}");
        }
        let context = TransactionFlowRenderContext {
            transaction: &transaction,
            theme: &theme,
            store: &store,
            currency: "vDEX",
            unmask_identity_addresses: false,
        };
        let tiles: String = flows
            .iter()
            .map(|flow| transaction_flow_tile(&context, flow, None, 80))
            .flat_map(|tile| tile.lines)
            .flat_map(|line| line.spans)
            .map(|span| span.content.into_owned())
            .collect();
        assert!(tiles.contains("12.12500001 Bridge.vDEX"), "{tiles}");
        assert!(tiles.contains("34.37500002 PUMP"), "{tiles}");
        let rendered: String = inputs
            .iter()
            .map(|flow| address_flow_line(&theme, &store, address, false, flow, false))
            .flat_map(|line| line.spans.into_iter().map(|span| span.content.into_owned()))
            .collect();
        assert!(rendered.contains("12.12500001 Bridge.vDEX"), "{rendered}");
        assert!(rendered.contains("34.37500002 PUMP"), "{rendered}");
        assert!(
            !rendered.contains(&short_id(bridge)) && !rendered.contains(&short_id(pump)),
            "{rendered}"
        );
    }

    #[test]
    fn burns_require_decoded_instructions_or_complete_ordinary_token_accounting() {
        for transfer in [
            json!({"burnchangeprice":true}),
            json!({"burnchangeweight":true}),
            json!({"flags":128}),
            json!({"flags":256}),
        ] {
            let output = json!({"scriptPubKey":{"reservetransfer":transfer}});
            assert!(
                output_tags(&output)
                    .iter()
                    .any(|tag| tag.kind == OutputTagKind::Burn)
            );
            let transaction = TransactionSummary {
                txid: "protocol-burn".into(),
                raw: Some(json!({"vin":[],"vout":[output]})),
            };
            let labels = display_transaction_tags(&transaction, &ObjectStore::default());
            assert!(labels.contains(&"Burn".into()));
            assert!(!labels.contains(&"Consumption Burn".into()));
        }
        for transfer in [
            json!({"burn":true}),
            json!({"burnchangeprice":false,"flags":64}),
            json!({"memo":"burn"}),
        ] {
            let output = json!({"scriptPubKey":{"reservetransfer":transfer}});
            assert!(
                !output_tags(&output)
                    .iter()
                    .any(|tag| tag.kind == OutputTagKind::Burn)
            );
        }
        let mut store = ObjectStore::default();
        store.chain.chain_id = Some("iNative".into());
        let prevout = json!({"valueSat":10000,"scriptPubKey":{"reserveoutput":{"currencyvalues":{"iToken":10}}}});
        let output =
            json!({"valueSat":0,"scriptPubKey":{"type":"cryptocondition","evalnone":"valid"}});
        let raw = json!({"vin":[{"txid":"missing","vout":0,"prevout":prevout}],"vout":[output]});
        assert_eq!(
            consumed_token_burns(&raw, &store).get("iToken"),
            Some(&1_000_000_000)
        );
        let mut partial = raw.clone();
        partial["vout"][0]["scriptPubKey"] =
            json!({"reserveoutput":{"currencyvalues":{"iToken":4}}});
        assert_eq!(
            consumed_token_burns(&partial, &store).get("iToken"),
            Some(&600_000_000)
        );
        partial["vout"][0]["scriptPubKey"]["reserveoutput"]["currencyvalues"]["iToken"] = json!(10);
        assert!(consumed_token_burns(&partial, &store).is_empty());
        for protocol in [
            "reservetransfer",
            "reservedeposit",
            "crosschainexport",
            "crosschainimport",
            "currencydefinition",
            "identityprimary",
        ] {
            let mut other = raw.clone();
            other["vout"][0]["scriptPubKey"][protocol] = json!({"currencyvalues":{"iToken":10}});
            assert!(
                consumed_token_burns(&other, &store).is_empty(),
                "{protocol}"
            );
            let mut input_protocol = raw.clone();
            input_protocol["vin"][0]["prevout"]["scriptPubKey"][protocol] =
                json!({"currencyvalues":{"iToken":10}});
            assert!(
                consumed_token_burns(&input_protocol, &store).is_empty(),
                "{protocol}"
            );
        }
        let mut incomplete = raw.clone();
        incomplete["vin"]
            .as_array_mut()
            .unwrap()
            .push(json!({"txid":"unknown","vout":0}));
        assert!(consumed_token_burns(&incomplete, &store).is_empty());
        let mut native_fee = raw.clone();
        native_fee["vin"][0]["prevout"]["scriptPubKey"] = json!({"type":"pubkeyhash"});
        assert!(consumed_token_burns(&native_fee, &store).is_empty());
        native_fee["vin"][0]["prevout"]["scriptPubKey"] =
            json!({"reserveoutput":{"currencyvalues":{"iNative":1}}});
        assert!(consumed_token_burns(&native_fee, &store).is_empty());
        let mut shielded = raw.clone();
        shielded["vShieldedOutput"] = json!([{"cmu":"note"}]);
        assert!(consumed_token_burns(&shielded, &store).is_empty());
        let mut tiny = raw.clone();
        tiny["vin"][0]["prevout"]["scriptPubKey"]["reserveoutput"]["currencyvalues"]["iToken"] =
            json!(0.00000001);
        assert_eq!(consumed_token_burns(&tiny, &store).get("iToken"), Some(&1));
        assert_eq!(decimal_string_to_zat("1e-8"), Some(1));
        assert_eq!(decimal_string_to_zat("1e-9"), None);
        assert_eq!(decimal_string_to_zat("-2.5e1"), Some(-2_500_000_000));
        let mut malformed = raw.clone();
        malformed["vin"][0]["prevout"]["scriptPubKey"]["reserveoutput"]["currencyvalues"]["iToken"] =
            json!("unknown");
        assert!(consumed_token_burns(&malformed, &store).is_empty());
    }

    #[test]
    fn transaction_summary_totals_tokens_on_both_sides_without_embedded_metadata() {
        let mut store = ObjectStore::default();
        store.chain.chain_id = Some("iNative".into());
        let token = json!({"valueSat":10000,"scriptPubKey":{"reserveoutput":{"currencyvalues":{"iToken":2}}}});
        let raw = json!({"vin":[{"prevout":token}],"vout":[token,
            {"valueSat":0,"scriptPubKey":{"crosschainimport":{"currencyvalues":{"iMetadata":999}}}}]});
        let flows = transaction_flows(&raw, &store);
        let (labels, _) = transaction_values_labels(&raw, &flows, "vDEX", &store, None);
        let text: String = labels.iter().map(|s| s.content.as_ref()).collect();
        assert!(
            text.contains("Inputs 1: 0.00010000 vDEX + 2 iToken"),
            "{text}"
        );
        assert!(
            text.contains("Outputs 2: 0.00010000 vDEX + 2 iToken"),
            "{text}"
        );
        assert!(!text.contains("iMetadata"));
        assert_eq!(flows[0].reserve_amounts.get("iToken"), Some(&200_000_000));
        assert_eq!(flows[1].reserve_amounts.get("iToken"), Some(&200_000_000));
    }

    #[test]
    fn zero_balances_require_positive_received_history() {
        let mut store = ObjectStore::default();
        store.chain.chain_id = Some("iNative".into());
        for (response, expected) in [
            (
                json!({"balance":0,"received":0,"currencybalance":{"iNative":0,"iNever":0}}),
                BTreeMap::new(),
            ),
            (
                json!({"balance":0,"received":1}),
                BTreeMap::from([("VRSC".into(), 0)]),
            ),
            (
                json!({"balance":0,"received":0,
                "currencybalance":{"iHeld":0,"iNever":0},
                "currencyreceived":{"iHeld":2,"iNever":0},
                "currencynames":{"iHeld":"Held"}}),
                BTreeMap::from([("Held".into(), 0)]),
            ),
            (
                json!({"balance":0,"received":0,"currencyreceived":{"iSpent":2}}),
                BTreeMap::from([("iSpent".into(), 0)]),
            ),
            (
                json!({"balance":0,"received":0,"currencybalance":{"iCurrent":1}}),
                BTreeMap::from([("iCurrent".into(), 100_000_000)]),
            ),
            (
                json!({"balance":0,"received":0,"currencybalance":{"iNative":0},
                "currencyreceived":{"iNative":0,"iNegative":-1}}),
                BTreeMap::new(),
            ),
        ] {
            assert_eq!(
                address_balance_amounts(&response, &store, "VRSC"),
                expected,
                "{response}"
            );
        }
    }

    #[test]
    fn authoritative_address_balance_uses_friendly_currency_names() {
        let response = json!({
            "balance": 125_000_000,
            "currencybalance": {
                "iNative": 1.25,
                "iReserve": 2.5
            },
            "currencynames": {
                "iNative": "VRSC",
                "iReserve": "Bridge.vETH"
            }
        });

        let balances = address_balance_amounts(&response, &ObjectStore::default(), "VRSC");

        assert_eq!(balances.get("VRSC"), Some(&125_000_000));
        assert_eq!(balances.get("Bridge.vETH"), Some(&250_000_000));
        assert_eq!(balances.len(), 2);
    }

    #[test]
    fn authoritative_address_balance_infers_unresolved_native_entry() {
        let response = json!({
            "balance": 125_000_000,
            "currencybalance": {
                "iNative": 1.25,
                "iReserve": 2.5
            }
        });

        let balances = address_balance_amounts(&response, &ObjectStore::default(), "VRSC");

        assert_eq!(balances.get("VRSC"), Some(&125_000_000));
        assert!(!balances.contains_key("iNative"));
        assert_eq!(balances.get("iReserve"), Some(&250_000_000));
    }

    #[test]
    fn recent_address_index_requests_are_height_bounded() {
        let params = address_index_range_params_json("RAddress", 100, 200)
            .expect("serialize bounded address request");
        let params: Value = serde_json::from_str(&params).expect("parse bounded address request");

        assert_eq!(params[0]["addresses"], json!(["RAddress"]));
        assert_eq!(params[0]["start"], 100);
        assert_eq!(params[0]["end"], 200);
    }

    #[test]
    fn variable_height_range_keeps_selection_visible_and_fills_space() {
        let heights = [4, 10, 4, 4];

        assert_eq!(variable_height_visible_range(&heights, 1, 14), (1, 3));
        assert_eq!(variable_height_visible_range(&heights, 2, 12), (2, 4));
        assert_eq!(variable_height_visible_range(&heights, 3, 8), (2, 4));
    }

    #[test]
    fn identity_store_selects_versions_and_resolves_display_names() {
        let address = "i5w5MuNik5NtLcYmNzcvaoixooEebB6MGV";
        let mut store = ObjectStore::default();
        store.insert_identity_version("VRSC@", test_identity_version(address, "VRSC@", 100));
        store.insert_identity_version("VRSC@", test_identity_version(address, "VRSC@", 250));

        assert_eq!(
            store
                .identity_for_target(&IdentityTarget {
                    query: "VRSC@".to_string(),
                    height: None,
                })
                .map(|identity| identity.block_height),
            Some(250)
        );
        assert_eq!(
            store
                .identity_for_target(&IdentityTarget {
                    query: address.to_string(),
                    height: Some(100),
                })
                .map(|identity| identity.block_height),
            Some(100)
        );
        assert_eq!(identity_display_value(&store, address, false), "VRSC@");
        assert_eq!(identity_display_value(&store, address, true), address);
    }

    #[test]
    fn identity_json_keeps_its_address_and_friendly_name_stable_while_resolving_authorities() {
        let id = "i6j1rzjgrDhSmUYiTtp21J8Msiudv5hgt9";
        let authority = "i5v3h9FWVdRFbNHU7DfcpGykQjRaHtMqu7";
        let mut store = ObjectStore::default();
        store.insert_identity_version(
            "Bridge.vDEX@",
            test_identity_version(id, "Bridge.vDEX.VRSC@", 100),
        );
        store.insert_resolved_identity_name(authority.into(), "Verus Coin Foundation.VRSC@".into());
        let root = json!({"friendlyname":"Bridge.vDEX@","fullyqualifiedname":"Bridge.vDEX.VRSC@",
            "identity":{"identityaddress":id,"revocationauthority":authority,"recoveryauthority":authority}});
        let mut state = JsonState::new();
        state.expanded.insert("$.identity".into());
        let entries = state.visible_entries(&root);
        let display = |label: &str, raw| {
            json_entry_display_value(
                entries.iter().find(|e| e.label == label).unwrap(),
                &store,
                raw,
            )
        };
        for raw in [false, true] {
            assert_eq!(display("friendlyname", raw), "\"Bridge.vDEX@\"");
            assert_eq!(display("identityaddress", raw), format!("{id:?}"));
            assert_eq!(display("fullyqualifiedname", raw), "\"Bridge.vDEX.VRSC@\"");
        }
        assert_eq!(
            display("revocationauthority", false),
            "\"Verus Coin Foundation@\""
        );
        assert_eq!(display("recoveryauthority", true), format!("{authority:?}"));
        assert_eq!(root["identity"]["identityaddress"], id);
        assert_eq!(store.identity_name(id), Some("Bridge.vDEX.VRSC@"));
        let mut view = regression_state();
        view.store = store;
        assert_eq!(
            identity_authority_label(&view, &root["identity"], "revocationauthority", id),
            "Verus Coin Foundation@"
        );
    }

    #[test]
    fn friendly_names_omit_only_the_final_assumed_root() {
        for (qualified, friendly) in [
            ("vDEX.VRSC@", "vDEX@"),
            ("Bridge.vDEX.VRSC@", "Bridge.vDEX@"),
            ("SUPER🛒.vrsc@", "SUPER🛒@"),
            ("Bridge.vDEX.VRSC", "Bridge.vDEX"),
            ("VRSC@", "VRSC@"),
            ("vDEX.VRSCTEST@", "vDEX.VRSCTEST@"),
            ("vDEX.VRSC.Other@", "vDEX.VRSC.Other@"),
        ] {
            assert_eq!(friendly_identity_name(qualified), friendly);
        }
        let document = json!({"friendlyname":"Bridge.vDEX.VRSC@"});
        let entries = JsonState::new().visible_entries(&document);
        let entry = entries.iter().find(|e| e.label == "friendlyname").unwrap();
        for raw in [false, true] {
            assert_eq!(
                json_entry_display_value(entry, &ObjectStore::default(), raw),
                "\"Bridge.vDEX@\""
            );
        }
    }

    #[test]
    fn identity_display_case_comes_from_latest_active_version() {
        let address = "i5w5MuNik5NtLcYmNzcvaoixooEebB6MGV";
        let mut store = ObjectStore::default();
        let mut revoked = test_identity_version(address, "VrSc@", 400);
        revoked.raw_json["status"] = Value::from("revoked");

        store.insert_identity_version("vrsc@", test_identity_version(address, "VRSC@", 250));
        store.insert_identity_version("vrsc@", revoked);
        store.insert_identity_version("vrsc@", test_identity_version(address, "vrsc@", 100));

        assert_eq!(store.identity_name(address), Some("VRSC@"));
        assert_eq!(store.identity_local_name(address), Some("VRSC"));
        assert_eq!(store.identity_address_for_query("vRsC@"), Some(address));
        store.insert_resolved_identity_name(address.to_string(), "wrong@".to_string());
        assert_eq!(store.identity_name(address), Some("VRSC@"));
    }

    #[test]
    fn json_yank_uses_plain_scalars_and_serialized_subtrees() {
        let root = json!({
            "identity": {
                "primaryaddresses": ["RAddress"]
            }
        });
        let mut json = JsonState::new();
        json.expanded.insert("$.identity".to_string());
        json.expanded
            .insert("$.identity.primaryaddresses".to_string());
        let entries = json.visible_entries(&root);
        let array = entries
            .iter()
            .find(|entry| entry.label == "primaryaddresses")
            .unwrap();
        let item = entries.iter().find(|entry| entry.label == "[0]").unwrap();

        assert_eq!(
            json_yank_text(root.pointer(&array.pointer).unwrap()),
            "[\"RAddress\"]"
        );
        assert_eq!(
            json_yank_text(root.pointer(&item.pointer).unwrap()),
            "RAddress"
        );
    }

    #[test]
    fn identity_version_navigation_uses_adjacent_cached_updates() {
        let heights = BTreeSet::from([100, 250, 400]);

        assert_eq!(adjacent_identity_height(&heights, 250, -1), Some(100));
        assert_eq!(adjacent_identity_height(&heights, 250, 1), Some(400));
        assert_eq!(adjacent_identity_height(&heights, 100, -1), None);
        assert_eq!(adjacent_identity_height(&heights, 400, 1), None);
    }

    #[test]
    fn identity_reference_collection_covers_nested_values_and_keys() {
        let first = "i5w5MuNik5NtLcYmNzcvaoixooEebB6MGV";
        let second = "iExBJfZYK7KREDpuhj6PzZBzqMAKaFg7d2";
        let raw = json!({
            "identity": {
                "identityaddress": first,
                "parent": second,
                "primaryaddresses": [first, "RTransparentAddress"],
                "contentmultimap": { (second): [{ "delegate": first }] }
            }
        });
        let mut references = BTreeSet::new();

        collect_identity_references(&raw, &mut references);

        assert_eq!(
            references,
            BTreeSet::from([first.to_string(), second.to_string()])
        );
    }

    #[tokio::test]
    async fn block_json_requests_stake_destination_names() {
        let id = "i5w5MuNik5NtLcYmNzcvaoixooEebB6MGV";
        let other = "iExBJfZYK7KREDpuhj6PzZBzqMAKaFg7d2";
        let mut state = regression_state();
        let mut block = test_block(100, "block-hash", "transaction");
        block.raw_json["postxddest"] = json!(id);
        block.raw_json["posrewarddest"] = json!(other);
        let hash = state.store.insert_block(block);
        state.screen = Screen::Block {
            target: BlockTarget::Hash(hash),
            selected_tx: 0,
            mode: ViewMode::Json,
            json: JsonState::new(),
        };
        let (tx, _rx) = unbounded_channel();
        request_current_activity_identity_names(&mut state, &regression_config(), &tx);
        assert!(
            state.loading_identity_names.contains(id),
            "block JSON must hydrate postxddest"
        );
        assert!(
            state.loading_identity_names.contains(other),
            "block JSON must hydrate posrewarddest"
        );
    }

    #[test]
    fn identity_timelock_distinguishes_delay_absolute_height_and_unlocked() {
        assert_eq!(
            identity_timelock_label(
                &json!({ "version": 3, "flags": 2, "timelock": 1440 }),
                Some(100_000),
            ),
            "locked (1440 blocks unlock delay)"
        );
        assert_eq!(
            identity_timelock_label(
                &json!({ "version": 3, "flags": 0, "timelock": 101_000 }),
                Some(100_000),
            ),
            "locked (unlocks at height 101000)"
        );
        assert_eq!(
            identity_timelock_label(
                &json!({ "version": 3, "flags": 0, "timelock": 99_999 }),
                Some(100_000),
            ),
            "unlocked"
        );
        assert_eq!(
            identity_timelock_label(
                &json!({ "version": 1, "flags": 2, "timelock": 1440 }),
                Some(100_000),
            ),
            "unlocked"
        );
        assert_eq!(
            identity_timelock_label(
                &json!({ "version": 3, "flags": 32770, "timelock": 1440 }),
                Some(100_000),
            ),
            "unlocked"
        );
    }

    #[test]
    fn identity_associations_filter_sort_and_deduplicate_reverse_results() {
        let address = "RPrimaryAddress";
        let results = vec![
            json!({
                "identityaddress": "iSecond",
                "name": "zebra",
                "primaryaddresses": [address]
            }),
            json!({
                "identityaddress": "iFirst",
                "name": "Alpha",
                "primaryaddresses": [address, "ROther"]
            }),
            json!({
                "identityaddress": "iFirst",
                "name": "alpha-duplicate",
                "primaryaddresses": [address]
            }),
            json!({
                "identityaddress": "iUnrelated",
                "name": "unrelated",
                "primaryaddresses": ["ROther"]
            }),
        ];

        let associations = parse_identity_associations(address, results);

        assert_eq!(associations.len(), 2);
        assert_eq!(associations[0].identity_address, "iFirst");
        assert_eq!(associations[0].display_name, "Alpha@");
        assert_eq!(associations[1].identity_address, "iSecond");
    }

    #[test]
    fn relationship_selection_crosses_into_transaction_activity() {
        let mut selected_link = Some(0);
        let mut selected_tx = 4;

        advance_link_or_transaction(&mut selected_link, &mut selected_tx, 2, 3);
        assert_eq!(selected_link, Some(1));
        advance_link_or_transaction(&mut selected_link, &mut selected_tx, 2, 3);
        assert_eq!(selected_link, None);
        assert_eq!(selected_tx, 0);

        retreat_link_or_transaction(&mut selected_link, &mut selected_tx, 2);
        assert_eq!(selected_link, Some(1));
    }

    #[test]
    fn activity_boundaries_include_relationships_and_transactions() {
        let mut selected_link = None;
        let mut selected_tx = 2;

        set_activity_selection_boundary(&mut selected_link, &mut selected_tx, 2, 3, false);
        assert_eq!(selected_link, Some(0));
        assert_eq!(selected_tx, 0);

        set_activity_selection_boundary(&mut selected_link, &mut selected_tx, 2, 3, true);
        assert_eq!(selected_link, None);
        assert_eq!(selected_tx, 2);
    }

    #[test]
    fn identity_primary_addresses_preserve_full_ordered_values() {
        let mut version = test_identity_version("iIdentity", "name@", 10);
        version.raw_json["identity"]["primaryaddresses"] =
            json!(["RFullPrimaryAddressOne", "RFullPrimaryAddressTwo"]);

        assert_eq!(
            identity_primary_addresses(&version),
            ["RFullPrimaryAddressOne", "RFullPrimaryAddressTwo"]
        );
    }

    #[test]
    fn search_result_replaces_navigation_root() {
        let mut current = transaction_screen("old-transaction".to_string());
        let mut stack = vec![Screen::Dashboard, identity_screen("old@".to_string(), None)];

        replace_navigation_root(
            &mut stack,
            &mut current,
            identity_screen("new@".to_string(), None),
        );

        assert!(stack.is_empty());
        assert!(matches!(
            current,
            Screen::Identity {
                target: IdentityTarget { ref query, .. },
                ..
            } if query == "new@"
        ));
    }

    #[test]
    fn dashboard_navigation_replaces_the_current_root() {
        let mut current = transaction_screen("old-transaction".to_string());
        let mut stack = vec![Screen::Dashboard, identity_screen("old@".to_string(), None)];

        replace_navigation_root(&mut stack, &mut current, Screen::Dashboard);

        assert!(stack.is_empty());
        assert!(matches!(current, Screen::Dashboard));
    }

    #[test]
    fn bundle_offer_shows_the_asking_amount_instead_of_the_daemon_index_ratio() {
        let raw: Value =
            serde_json::from_str(include_str!("../tests/fixtures/marketplace-bundle.json"))
                .unwrap();
        let mut state = regression_state();
        state.screen = Screen::Marketplace;
        state.marketplace.offers = parse_marketplace_offers(&raw);
        assert_eq!(state.marketplace.offers.len(), 1);
        state
            .store
            .insert_currency_definition(CurrencyDefinitionRecord {
                currency_id: "i5w5MuNik5NtLcYmNzcvaoixooEebB6MGV".into(),
                name: "VRSC".into(),
                definition_json: json!({"name":"VRSC"}),
            });
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(160, 12)).unwrap();
        terminal
            .draw(|frame| render_marketplace_tiles(frame, &theme, &state, frame.area()))
            .unwrap();
        let text: String = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect();
        assert!(text.contains("Ask 258 VRSC"), "{text}");
        assert!(!text.contains("price 0.00000039"), "{text}");
    }

    #[test]
    fn marketplace_response_parser_finds_nested_offer_records() {
        let response = json!({
            "id_for_currency": [{
                "identityid": "iExample",
                "price": 12.5,
                "offer": {
                    "offer": { "name": "Example", "identityid": "iExample" },
                    "accept": { "VRSC": 12.5 },
                    "blockexpiry": 500,
                    "txid": "offer-txid"
                }
            }]
        });

        let offers = parse_marketplace_offers(&response);

        assert_eq!(offers.len(), 1);
        assert_eq!(offers[0].txid, "offer-txid");
        assert_eq!(offers[0].subject_id.as_deref(), Some("iExample"));
        assert_eq!(offers[0].expiry_height, 500);
    }

    #[test]
    fn historical_marketplace_links_settlement_and_rejects_a_changed_payment() {
        let funding = "1".repeat(64);
        let proof = "2".repeat(64);
        let taken = "3".repeat(64);
        let mut store = ObjectStore {
            workspace_system: crate::world::system_id("VRSC", crate::world::Network::Mainnet).ok(),
            ..Default::default()
        };
        store.insert_transaction(test_transaction(
            &funding,
            json!({
                "txid": funding, "vin": [],
                "vout": [
                    {"n": 0, "valueSat": 90000, "spentTxId": taken, "scriptPubKey": {
                        "commitmenthash": {"currencyvalues": {"iSynthetic": 1}}
                    }},
                    {"n": 1, "scriptPubKey": {"type": "pubkeyhash"}}
                ]
            }),
        ));
        store.insert_transaction(test_transaction(
            &proof,
            json!({
                "txid": proof, "vin": [{"txid": funding, "vout": 1}],
                "vout": [{"n": 0, "scriptPubKey": {
                    "hex": marketplace_trace::test_publication(1, 50000)
                }}]
            }),
        ));
        let mut settled = json!({
            "txid": taken, "height": 50, "version": 4, "overwintered": true,
            "expiryheight": 100, "locktime": 0,
            "vin": [{"txid": funding, "vout": 0, "sequence": 4294967295u32, "scriptSig": {"hex": "51"}},
                    {"txid": "other", "vout": 0}],
            "vout": [
                {"n": 0, "valueSat": 50000, "scriptPubKey": {"hex": "52"}},
                {"n": 1, "valueSat": 10000,
                 "scriptPubKey": {"reserveoutput": {"currencyvalues": {"iSynthetic": 1}}}}
            ]
        });
        store.insert_transaction(test_transaction(&taken, settled.clone()));
        assert!(
            store
                .marketplace_tags(&funding)
                .any(|tag| tag == MarketplaceTag::Offer)
        );
        assert!(
            store
                .marketplace_tags(&proof)
                .any(|tag| tag == MarketplaceTag::OfferProof)
        );
        assert!(
            store
                .marketplace_tags(&taken)
                .any(|tag| tag == MarketplaceTag::OfferTaken)
        );
        let flows = transaction_flows(&settled, &store);
        assert_eq!(flows[0].annotations, ["Marketplace Offer Settlement"]);
        assert_eq!(flows[2].annotations, ["Marketplace Seller Consideration"]);
        assert_eq!(flows[3].annotations, ["Marketplace Taker Delivery"]);
        assert_eq!(flows[2].references.links.len(), 2);
        let funding_flows =
            transaction_flows(&store.transaction(&funding).unwrap().raw_json, &store);
        assert_eq!(funding_flows[0].references.links.len(), 2);
        settled["vout"][0]["valueSat"] = json!(50001);
        store.replace_transaction(test_transaction(&taken, settled.clone()));
        assert!(
            !store
                .marketplace_tags(&taken)
                .any(|tag| tag == MarketplaceTag::OfferTaken)
        );
        let flows = transaction_flows(&settled, &store);
        assert_eq!(
            flows[0].annotations,
            ["Marketplace Funding Spend · outcome unverified"]
        );
        assert!(flows[2].annotations.is_empty());
    }

    #[test]
    fn marketplace_close_validates_the_refund_and_links_embedded_publication() {
        let funding = "1".repeat(64);
        let posting = "2".repeat(64);
        let mut store = ObjectStore::default();
        store.insert_transaction(test_transaction(
            &funding,
            json!({
                "txid": funding, "vin": [], "vout": [
                    {"n": 0, "valueSat": 90000, "scriptPubKey": {"commitmenthash": {}}},
                    {"n": 1, "scriptPubKey": {"type": "pubkeyhash"}}
                ]
            }),
        ));
        store.insert_transaction(test_transaction(
            &posting,
            json!({
                "txid": posting, "vin": [], "vout": [
                    {"n": 0, "valueSat": 10000, "scriptPubKey": {"commitmenthash": {}}},
                    {"n": 1, "scriptPubKey": {"hex": marketplace_trace::test_publication(1, 50000)}}
                ]
            }),
        ));
        assert!(
            store
                .marketplace_tags(&posting)
                .any(|tag| tag == MarketplaceTag::OfferProof)
        );
        let mut close = json!({
            "txid": "close",
            "vin": [{"txid": funding, "vout": 0}],
            "vout": [{"n": 0, "valueSat": 80000, "scriptPubKey": {"type": "cryptocondition", "evalnone": "valid", "spendableoutput": true}}]
        });
        let flows = transaction_flows(&close, &store);
        assert_eq!(flows[0].annotations, ["Marketplace Offer Cancellation"]);
        assert_eq!(flows[1].annotations, ["Marketplace Offer Refund"]);
        close["vout"][0]["valueSat"] = json!(80001);
        let flows = transaction_flows(&close, &store);
        assert_eq!(
            flows[0].annotations,
            ["Marketplace Funding Spend · outcome unverified"]
        );
        assert!(flows[1].annotations.is_empty());
    }

    fn notes_offer(id: &str, name: &str, price: Option<f64>) -> MarketplaceOffer {
        MarketplaceOffer {
            txid: id.into(),
            category: "offers".into(),
            subject_id: Some(format!("i{name}")),
            asking_price: price
                .and_then(|price| marketplace_asking_price(&json!({"iOriginal":price}))),
            offered: json!({"identityid":format!("i{name}"),"name":name}),
            accepted: json!({"iOriginal": 1}),
            expiry_height: 1000,
        }
    }

    #[test]
    fn marketplace_shift_cycles_wrap_and_preserve_the_selected_association() {
        let mut state = regression_state();
        state.screen = Screen::Marketplace;
        state.marketplace.offers = vec![notes_offer("a", "First", Some(1.0))];
        assert!(handle_marketplace_cycle_key(
            &mut state,
            KeyCode::Char('A'),
            KeyModifiers::NONE
        ));
        assert_eq!(state.marketplace.selected_link, 2);
        assert_eq!(
            state.marketplace.preferred_link,
            Some(MarketplaceLink::Currency("iOriginal".into()))
        );
        handle_marketplace_cycle_key(&mut state, KeyCode::Char('a'), KeyModifiers::NONE);
        assert_eq!(state.marketplace.selected_link, 0);
        handle_marketplace_cycle_key(&mut state, KeyCode::Char('a'), KeyModifiers::SHIFT);
        assert_eq!(state.marketplace.selected_link, 2);
        handle_marketplace_cycle_key(&mut state, KeyCode::Char('A'), KeyModifiers::SHIFT);
        assert_eq!(state.marketplace.selected_link, 1);
        assert_eq!(
            state.marketplace.preferred_link,
            Some(MarketplaceLink::Identity("iFirst".into()))
        );
        assert!(!handle_marketplace_cycle_key(
            &mut state,
            KeyCode::Char('a'),
            KeyModifiers::CONTROL
        ));
        assert_eq!(state.marketplace.selected_link, 1);
        state.marketplace.offers.clear();
        handle_marketplace_cycle_key(&mut state, KeyCode::Char('A'), KeyModifiers::NONE);
    }

    #[test]
    fn marketplace_shift_sorts_cycle_backward_and_return_to_top() {
        let mut state = regression_state();
        state.screen = Screen::Marketplace;
        state.marketplace.offers = vec![
            notes_offer("a", "First", Some(1.0)),
            notes_offer("b", "Second", Some(2.0)),
        ];
        for (key, modifiers, expected) in [
            ('S', KeyModifiers::NONE, MarketplaceSort::Age),
            ('s', KeyModifiers::SHIFT, MarketplaceSort::Price),
            ('S', KeyModifiers::SHIFT, MarketplaceSort::Name),
            ('s', KeyModifiers::NONE, MarketplaceSort::Price),
        ] {
            state.marketplace.selected = 1;
            handle_marketplace_cycle_key(&mut state, KeyCode::Char(key), modifiers);
            assert_eq!(state.marketplace.sort, expected);
            assert_eq!(state.marketplace.selected, 0);
        }
        for sort in [
            MarketplaceSort::Expiry,
            MarketplaceSort::Name,
            MarketplaceSort::Price,
            MarketplaceSort::Age,
        ] {
            assert_eq!(sort.next().previous(), sort);
            assert_eq!(sort.previous().next(), sort);
        }
    }

    #[test]
    fn peer_detail_accepts_shift_tab_in_both_view_modes() {
        let mut state = regression_state();
        state.auxiliary = Some(AuxiliaryScreen::PeerDetail {
            peer_id: 1,
            mode: ViewMode::Overview,
            json: JsonState::new(),
            scroll: 0,
        });
        let config = regression_config();
        let theme = Theme::desert(config.emoji);
        let (tx, _rx) = unbounded_channel();
        for _ in 0..2 {
            handle_auxiliary_key(&mut state, &theme, &config, &tx, KeyCode::BackTab, 30);
            assert!(matches!(
                state.auxiliary,
                Some(AuxiliaryScreen::PeerDetail {
                    mode: ViewMode::Json,
                    ..
                })
            ));
            handle_auxiliary_key(&mut state, &theme, &config, &tx, KeyCode::BackTab, 30);
            assert!(matches!(
                state.auxiliary,
                Some(AuxiliaryScreen::PeerDetail {
                    mode: ViewMode::Overview,
                    ..
                })
            ));
        }
    }

    #[test]
    fn marketplace_sort_setting_changes_return_to_top_but_refreshes_keep_selection() {
        let mut state = regression_state();
        state.screen = Screen::Marketplace;
        state.marketplace.offers = vec![
            notes_offer("a", "Zebra", Some(3.0)),
            notes_offer("b", "Alpha", Some(1.0)),
            notes_offer("c", "Beta", Some(2.0)),
        ];
        state.marketplace.selected = 2;
        state.marketplace.sort = MarketplaceSort::Name;
        sort_marketplace_from_top(&mut state);
        assert_eq!(state.marketplace.selected, 0);
        assert_eq!(state.marketplace.offers[0].txid, "b");
        state.marketplace.selected = 2;
        state.marketplace.descending = true;
        sort_marketplace_from_top(&mut state);
        assert_eq!(state.marketplace.selected, 0);
        assert_eq!(state.marketplace.offers[0].txid, "a");
        state.marketplace.selected = 1;
        let selected = state.marketplace.offers[1].txid.clone();
        update_marketplace_sort(&mut state);
        assert_eq!(
            state.marketplace.offers[state.marketplace.selected].txid,
            selected
        );
    }

    #[test]
    fn marketplace_identity_names_have_a_background_separate_from_the_id_label() {
        let mut state = regression_state();
        state.screen = Screen::Marketplace;
        state.marketplace.offers = vec![notes_offer("emoji", "🐉Mushroom🍄", Some(1.0))];
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        for width in [80, 160] {
            let mut terminal =
                Terminal::new(ratatui::backend::TestBackend::new(width, 10)).unwrap();
            terminal
                .draw(|frame| render_marketplace_tiles(frame, &theme, &state, frame.area()))
                .unwrap();
            let cells = &terminal.backend().buffer().content;
            for emoji in ["🐉", "🍄"] {
                let cell = cells.iter().find(|cell| cell.symbol() == emoji).unwrap();
                assert_eq!(Some(cell.bg), theme.status_badge().bg);
                assert_ne!(cell.bg, theme.background);
            }
        }
        state
            .store
            .insert_resolved_identity_name("iEmoji".into(), "🐉Mushroom🍄@".into());
        for asset in [
            json!({"name":"🐉Mushroom🍄"}),
            json!({"identityid":"iEmoji"}),
        ] {
            let spans = marketplace_asset_spans(&asset, &state.store, &theme, theme.positive());
            assert_eq!(spans[0].content, "ID ");
            assert_ne!(spans[0].style.bg, spans[1].style.bg);
            assert_eq!(spans[1].style.bg, theme.status_badge().bg);
        }
    }

    #[test]
    fn marketplace_asks_use_exact_payment_amounts_and_keep_units_separate() {
        assert_eq!(
            marketplace_asking_price(&json!({"iA":0.00000001}))
                .unwrap()
                .amount,
            1
        );
        assert!(marketplace_asking_price(&json!({"iA":1,"iB":2})).is_none());
        assert!(marketplace_asking_price(&json!({"identityid":"iIdentity"})).is_none());
        let mut state = regression_state();
        state.screen = Screen::Marketplace;
        let mut a = notes_offer("a", "A", Some(1000.0));
        let mut b = notes_offer("b", "B", Some(1.0));
        a.asking_price.as_mut().unwrap().currency = "iA".into();
        b.asking_price.as_mut().unwrap().currency = "iB".into();
        state.marketplace.offers = vec![b, a];
        state.marketplace.sort = MarketplaceSort::Price;
        update_marketplace_sort(&mut state);
        assert_eq!(state.marketplace.offers[0].txid, "a");
        state.marketplace.descending = true;
        update_marketplace_sort(&mut state);
        assert_eq!(state.marketplace.offers[0].txid, "a");
    }

    #[test]
    fn marketplace_sorting_handles_directions_unknown_age_and_retains_selected_offer() {
        let mut state = regression_state();
        state.screen = Screen::Marketplace;
        state.marketplace.offers = vec![
            notes_offer("a", "Zebra", Some(3.0)),
            notes_offer("b", "alpha", Some(1.0)),
            notes_offer("c", "Beta", None),
        ];
        for (id, time) in [("a", 100), ("b", 200)] {
            state.store.insert_transaction(transaction_record(
                id,
                json!({"txid":id,"blockhash":"block","blocktime":time}),
            ));
        }
        for (sort, ascending, descending) in [
            (
                MarketplaceSort::Name,
                vec!["b", "c", "a"],
                vec!["a", "c", "b"],
            ),
            (
                MarketplaceSort::Price,
                vec!["b", "a", "c"],
                vec!["a", "b", "c"],
            ),
            (
                MarketplaceSort::Age,
                vec!["b", "a", "c"],
                vec!["a", "b", "c"],
            ),
        ] {
            state.marketplace.sort = sort;
            for (reversed, expected) in [(false, ascending), (true, descending)] {
                state.marketplace.descending = reversed;
                update_marketplace_sort(&mut state);
                assert_eq!(
                    state
                        .marketplace
                        .offers
                        .iter()
                        .map(|offer| offer.txid.as_str())
                        .collect::<Vec<_>>(),
                    expected
                );
                assert_eq!(
                    state.marketplace.offers[state.marketplace.selected].txid,
                    "a"
                );
            }
        }
    }

    #[test]
    fn marketplace_keeps_asset_association_across_orders_and_breadcrumb_return() {
        let mut state = regression_state();
        state.screen = Screen::Marketplace;
        let first = notes_offer("a", "First", Some(1.0));
        let mut second = notes_offer("b", "Second", Some(2.0));
        second.offered = json!({"iOriginal":1});
        second.accepted = json!({"identityid":"iSecond","name":"Second"});
        state.marketplace.offers = vec![first, second];
        select_marketplace_link(&mut state, 1);
        select_marketplace_link(&mut state, 1);
        assert_eq!(
            state.marketplace.preferred_link,
            Some(MarketplaceLink::Currency("iOriginal".into()))
        );
        select_next_marketplace_offer(&mut state);
        assert_eq!(state.marketplace.selected_link, 1);
        push_screen(&mut state, transaction_screen("trail".into()));
        push_screen(&mut state, transaction_screen("deeper".into()));
        pop_screen(&mut state);
        pop_screen(&mut state);
        assert!(matches!(state.screen, Screen::Marketplace));
        assert_eq!(state.marketplace.selected_link, 1);
        select_previous_marketplace_offer(&mut state);
        assert_eq!(state.marketplace.selected_link, 2);
    }

    #[test]
    fn identity_json_initial_focus_waits_for_data_and_does_not_reset_user_selection() {
        let mut state = regression_state();
        state.screen = identity_screen("VRSC@".into(), None);
        initialize_identity_json(&mut state);
        state.store.insert_identity_version(
            "VRSC@",
            test_identity_version("i5w5MuNik5NtLcYmNzcvaoixooEebB6MGV", "VRSC@", 100),
        );
        initialize_identity_json(&mut state);
        let Screen::Identity { json, target, .. } = &mut state.screen else {
            panic!()
        };
        let record = state.store.identity_for_target(target).unwrap();
        assert_eq!(json.current_path(&record.raw_json), "$.identity");
        assert!(json.expanded.contains("$.identity"));
        json.cursor = 0;
        initialize_identity_json(&mut state);
        let Screen::Identity { json, .. } = &state.screen else {
            panic!()
        };
        assert_eq!(json.cursor, 0);
    }

    #[test]
    fn identity_offer_markers_distinguish_sides_and_clear_on_expiry_or_failed_refresh() {
        let mut state = regression_state();
        let id = "i5w5MuNik5NtLcYmNzcvaoixooEebB6MGV";
        let mut sell = notes_offer("sale", "VRSC", Some(1.0));
        sell.subject_id = Some(id.into());
        sell.offered = json!({"identityid": id, "name": "VRSC"});
        let mut buy = notes_offer("buy", "Other", Some(2.0));
        buy.accepted = json!({"identityid":id});
        handle_app_event(
            &mut state,
            AppEvent::IdentityOffers {
                identity: id.into(),
                result: Ok(vec![sell, buy]),
            },
        );
        assert_eq!(
            identity_offer_labels(&state, id),
            vec!["Buy offers", "Sell offers"]
        );
        state.tip.height = Some(1000);
        assert!(identity_offer_labels(&state, id).is_empty());
        state.tip.height = Some(1);
        handle_app_event(
            &mut state,
            AppEvent::IdentityOffers {
                identity: id.into(),
                result: Err("offline".into()),
            },
        );
        assert!(identity_offer_labels(&state, id).is_empty());
        handle_app_event(
            &mut state,
            AppEvent::IdentityOffers {
                identity: id.into(),
                result: Ok(vec![]),
            },
        );
        assert!(identity_offer_labels(&state, id).is_empty());
    }

    #[test]
    fn transaction_view_uses_width_for_block_hash_and_quiet_derived_pool_label() {
        use ratatui::backend::TestBackend;
        let mut state = regression_state();
        let hash = "b".repeat(64);
        let record = transaction_record(
            "pending",
            json!({"txid":"pending","blockhash":hash,"height":40,"blocktime":1000,"valueBalance":-3.0,"vShieldedOutput":[{}]}),
        );
        let flows = transaction_flows(&record.raw_json, &state.store);
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        let flow = flows
            .iter()
            .find(|flow| flow.private && flow.kind == TransactionFlowKind::Output)
            .unwrap();
        let tile = transaction_flow_tile(
            &TransactionFlowRenderContext {
                transaction: &record,
                theme: &theme,
                store: &state.store,
                currency: "VRSC",
                unmask_identity_addresses: false,
            },
            flow,
            None,
            100,
        );
        let text = tile
            .lines
            .iter()
            .map(Line::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            text.contains("derived value 3.00000000 VRSC (Sapling)"),
            "{text}"
        );
        assert!(!text.contains("Derived from"));
        state.store.insert_transaction(record);
        let config = regression_config();
        for width in [60, 120] {
            let mut terminal = Terminal::new(TestBackend::new(width, 35)).unwrap();
            terminal
                .draw(|frame| {
                    render(frame, &config, &theme, &state, true);
                })
                .unwrap();
            let text: String = terminal
                .backend()
                .buffer()
                .content
                .iter()
                .map(|cell| cell.symbol())
                .collect();
            assert_eq!(text.contains(&hash), width == 120);
        }
    }

    #[test]
    fn marketplace_offer_links_funding_assets_and_addresses() {
        let mut store = ObjectStore::default();
        store.insert_transaction(test_transaction(
            "funding",
            json!({
                "txid": "funding",
                "vout": [{ "scriptPubKey": { "addresses": ["RChange"] } }]
            }),
        ));
        let offer = MarketplaceOffer {
            txid: "funding".to_string(),
            category: "offers".to_string(),
            subject_id: None,
            asking_price: None,
            offered: json!({ "iAsset": 1.0 }),
            accepted: json!({ "identityid": "iIdentity", "name": "Example" }),
            expiry_height: 10,
        };

        assert_eq!(
            marketplace_links(&offer, &store),
            vec![
                MarketplaceLink::Funding("funding".to_string()),
                MarketplaceLink::Currency("iAsset".to_string()),
                MarketplaceLink::Identity("iIdentity".to_string()),
                MarketplaceLink::Address("RChange".to_string()),
            ]
        );
    }

    #[test]
    fn stakeguard_enforcement_requires_a_stakeguard_prevout_and_cheat_evidence() {
        let mut store = ObjectStore::default();
        store.insert_transaction(test_transaction(
            "stake-reward",
            json!({
                "txid": "stake-reward",
                "vout": [{ "scriptPubKey": { "stakeguard": "" } }]
            }),
        ));
        let enforcement_raw = json!({
            "txid": "enforcement",
            "vin": [{ "txid": "stake-reward", "vout": 0 }],
            "vout": [{ "scriptPubKey": { "type": "nulldata", "hex": "6a035301aa" } }]
        });
        let enforcement = TransactionSummary {
            txid: "enforcement".to_string(),
            raw: Some(enforcement_raw),
        };
        let protected_stake = store
            .transaction("stake-reward")
            .expect("protected stake fixture");

        assert!(display_transaction_tags(&protected_stake.summary, &store).is_empty());
        assert!(
            display_transaction_tags(&enforcement, &store)
                .contains(&"StakeGuard Enforcement".to_string())
        );
    }

    #[test]
    fn import_marks_consensus_ordered_auto_arbitrage_input() {
        let mut store = ObjectStore::default();
        store.insert_transaction(test_transaction(
            "prior-import",
            json!({
                "txid": "prior-import",
                "vout": [{ "scriptPubKey": { "crosschainimport": { "version": 1 } } }]
            }),
        ));
        store.insert_transaction(test_transaction(
            "arb-take",
            json!({
                "txid": "arb-take",
                "vout": [{ "scriptPubKey": { "reservetransfer": {
                    "arbitrageonly": true,
                    "convert": true,
                    "destinationcurrencyid": "iPool"
                } } }]
            }),
        ));
        store.insert_transaction(test_transaction(
            "prior-notarization",
            json!({
                "txid": "prior-notarization",
                "vout": [{ "scriptPubKey": { "acceptednotarization": { "version": 2 } } }]
            }),
        ));
        let raw = json!({
            "txid": "import",
            "vin": [
                { "txid": "prior-import", "vout": 0 },
                { "txid": "arb-take", "vout": 0 },
                { "txid": "prior-notarization", "vout": 0 }
            ],
            "vout": [{ "scriptPubKey": { "crosschainimport": {
                "version": 1, "importcurrencyid": "iPool"
            } } }]
        });
        let summary = TransactionSummary {
            txid: "import".to_string(),
            raw: Some(raw.clone()),
        };

        let flows = transaction_flows(&raw, &store);
        assert!(flows[0].annotations.is_empty());
        assert_eq!(flows[1].annotations, ["Auto-Arbitrage Input"]);
        assert!(flows[2].annotations.is_empty());
        assert!(
            display_transaction_tags(&summary, &store)
                .contains(&"Auto-Arbitrage Import".to_string())
        );
    }

    #[test]
    fn identity_currency_badge_uses_the_selected_versions_protocol_flag() {
        let mut version = test_identity_version("i5w5MuNik5NtLcYmNzcvaoixooEebB6MGV", "VRSC@", 1);
        assert!(!identity_has_currency(&version));
        version.raw_json["identity"]["flags"] = json!(1);
        assert!(identity_has_currency(&version));
        let state = regression_state();
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        let text = identity_header_lines(&regression_config(), &theme, &state, &version, None, 120)
            .0
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("Currency defined"));
        assert!(text.contains("c opens currency"));
        version.raw_json["identity"]["flags"] = json!(2);
        assert!(!identity_has_currency(&version));
    }

    #[test]
    fn detail_header_selection_keeps_values_in_the_same_cells() {
        let config = regression_config();
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        let mut state = regression_state();
        let address = "primary-address";
        let identity_address = crate::world::system_id("VRSC", crate::world::Network::Mainnet)
            .unwrap()
            .to_string();
        let mut version = test_identity_version(&identity_address, "VRSC@", 1);
        version.raw_json["identity"]["primaryaddresses"] = json!([address]);
        state.store.insert_identity_version("VRSC@", version);
        state.store.insert_identity_associations(
            address.to_string(),
            AddressIdentityAssociations {
                identities: vec![IdentityAssociation {
                    identity_address,
                    display_name: "VRSC@".into(),
                }],
                source: IdentityAssociationSource::LocalDaemon,
            },
        );
        let target = IdentityTarget {
            query: "VRSC@".into(),
            height: None,
        };
        for (identity, label) in [(true, address), (false, "VRSC@")] {
            let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(40, 45)).unwrap();
            let mut position = None;
            for link in [None, Some(0)] {
                let selection = ActivitySelection {
                    transaction: 0,
                    link,
                };
                terminal
                    .draw(|frame| {
                        let area = frame.area();
                        if identity {
                            render_identity_view(
                                frame, &config, &theme, &state, &target, selection, None, area,
                            );
                        } else {
                            render_address_view(
                                frame, &config, &theme, &state, address, selection, area,
                            );
                        }
                    })
                    .unwrap();
                let buffer = terminal.backend().buffer();
                let (column, row) = buffer
                    .content
                    .chunks(40)
                    .enumerate()
                    .find_map(|(row, cells)| {
                        let line: String = cells.iter().map(|cell| cell.symbol()).collect();
                        line.find(label).map(|column| (column, row as u16))
                    })
                    .unwrap();
                if let Some(previous) = position {
                    assert_eq!((column, row), previous);
                } else {
                    position = Some((column, row));
                }
                assert_eq!(column, 1);
                assert_eq!(
                    buffer[(0, row)].symbol(),
                    if link.is_some() {
                        SELECTION_MARKER
                    } else {
                        " "
                    }
                );
            }
        }
    }

    fn test_identity_version(
        address: &str,
        fully_qualified_name: &str,
        block_height: u64,
    ) -> IdentityVersionRecord {
        IdentityVersionRecord {
            identity_address: address.to_string(),
            fully_qualified_name: fully_qualified_name.to_string(),
            block_height,
            block_time: block_height,
            raw_json: json!({
                "fullyqualifiedname": fully_qualified_name,
                "status": "active",
                "blockheight": block_height,
                "identity": {
                    "name": fully_qualified_name.trim_end_matches('@'),
                    "identityaddress": address
                }
            }),
        }
    }

    fn test_transaction(txid: &str, raw_json: Value) -> TransactionRecord {
        TransactionRecord {
            prepared: None,
            source_id: None,
            summary: TransactionSummary {
                txid: txid.to_string(),
                raw: Some(raw_json.clone()),
            },
            raw_json,
            block_hash: None,
            block_height: None,
            block_time: 0,
        }
    }

    fn test_block(height: u64, hash: &str, txid: &str) -> BlockRecord {
        BlockRecord {
            prepared_transactions: HashMap::new(),
            summary: BlockSummary {
                hash: hash.to_string(),
                height,
                time: height,
                size: Some(1),
                flags: None,
                blocktype: Some("mined".to_string()),
                previousblockhash: None,
                nextblockhash: None,
                tx: vec![TransactionSummary {
                    txid: txid.to_string(),
                    raw: Some(json!({ "txid": txid })),
                }],
            },
            raw_json: json!({
                "hash": hash,
                "height": height,
                "time": height,
                "tx": [{ "txid": txid }],
            }),
            detail_loaded: true,
        }
    }
    #[test]
    fn obsolete_currency_activity_and_batch_results_do_not_replace_newer_context() {
        let mut state = regression_state();
        state.screen = currency_ui::screen("iPool".into(), "iSystem".into());
        if let Screen::Currency(view) = &mut state.screen {
            view.activity_request = Some(22);
            view.transfer_request = Some(33);
        }
        let page = crate::currency_activity::Page {
            system: "iSystem".into(),
            currency: "iPool".into(),
            source: "local".into(),
            anchor: crate::cache::BlockContext {
                height: 10,
                hash: "block".into(),
            },
            range: crate::currency_activity::Range {
                first: 10,
                last: 10,
            },
            records: Vec::new(),
            truncated: false,
        };
        handle_app_event(
            &mut state,
            AppEvent::CurrencyActivityLoaded {
                query: "iPool".into(),
                request_id: 21,
                result: Ok(page.clone()),
            },
        );
        handle_app_event(
            &mut state,
            AppEvent::CurrencyTransfersLoaded {
                query: "iPool".into(),
                request_id: 32,
                result: Err("obsolete".into()),
            },
        );
        if let Screen::Currency(view) = &state.screen {
            assert!(view.activity.is_none());
            assert!(view.error.is_none());
            assert_eq!(view.activity_request, Some(22));
        }
        handle_app_event(
            &mut state,
            AppEvent::CurrencyActivityLoaded {
                query: "iPool".into(),
                request_id: 22,
                result: Ok(page),
            },
        );
        if let Screen::Currency(view) = &mut state.screen {
            assert!(view.activity.is_some());
            currency_ui::clear_activity(view);
        }
        handle_app_event(
            &mut state,
            AppEvent::CurrencyTransfersLoaded {
                query: "iPool".into(),
                request_id: 33,
                result: Err("late batch".into()),
            },
        );
        if let Screen::Currency(view) = &state.screen {
            assert!(view.error.is_none());
            assert!(view.activity.is_none());
        }
    }
}
