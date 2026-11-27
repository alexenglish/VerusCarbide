//! Live daemon observations, separate from immutable objects and classification tags.
use serde_json::Value;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct RpcObservation {
    pub authorization: Option<(std::sync::Arc<crate::sources::Control>, u64, u64)>,
    pub order: u64,
    pub method: String,
    pub params: Vec<Value>,
    pub result: Value,
}

impl RpcObservation {
    pub fn same_scalar_snapshot(&self, other: &Self) -> bool {
        self.method == other.method
            && match self.method.as_str() {
                "getblockcount" => self.result.is_u64() && other.result.is_u64(),
                "getbestblockhash" => self.result.is_string() && other.result.is_string(),
                _ => false,
            }
            && match (&self.authorization, &other.authorization) {
                (Some((left, generation, epoch)), Some((right, other_generation, other_epoch))) => {
                    std::sync::Arc::ptr_eq(left, right)
                        && generation == other_generation
                        && epoch == other_epoch
                }
                _ => false,
            }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusBadge {
    ChainTip,
    LastObservedTip,
    Mempool,
    Immature,
    StakeAgePending,
    Active,
    Orphaned,
    Unverified,
    LastObservedActive,
}
impl StatusBadge {
    pub fn label(self) -> &'static str {
        match self {
            Self::ChainTip => "Chain tip",
            Self::LastObservedTip => "Last observed tip",
            Self::Mempool => "Mempool",
            Self::Immature => "Immature",
            Self::StakeAgePending => "Stake age pending",
            Self::Active => "Active",
            Self::Orphaned => "Orphaned",
            Self::Unverified => "Unverified",
            Self::LastObservedActive => "Last observed active",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ObservedBlock {
    pub hash: String,
    pub height: u64,
    pub time: u64,
    previous: Option<String>,
    order: u64,
}

/// Immutable branch indexes prepared by the reconciliation worker.
#[derive(Debug, Default)]
pub struct PreparedBranch {
    pub state: crate::reconciliation::State,
    canonical_by_hash: HashMap<String, ObservedBlock>,
    pub detached_count: usize,
    pub attached_count: usize,
    pub changed_hashes: Vec<String>,
}

impl From<crate::reconciliation::Change> for PreparedBranch {
    fn from(change: crate::reconciliation::Change) -> Self {
        let canonical_by_hash = change
            .state
            .canonical
            .values()
            .map(|header| {
                (
                    header.hash.clone(),
                    ObservedBlock {
                        hash: header.hash.clone(),
                        height: header.height,
                        time: header.time,
                        previous: header.parent.clone(),
                        order: 0,
                    },
                )
            })
            .collect();
        Self {
            state: change.state,
            canonical_by_hash,
            detached_count: change.detached.len(),
            attached_count: change.attached.len(),
            changed_hashes: change
                .attached
                .iter()
                .chain(&change.detached)
                .map(|header| header.hash.clone())
                .collect(),
        }
    }
}

impl std::ops::Deref for PreparedBranch {
    type Target = crate::reconciliation::State;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl PreparedBranch {
    pub fn canonical_block(&self, hash: &str) -> Option<&ObservedBlock> {
        self.canonical_by_hash.get(hash)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ChainState {
    pub reconciled: Option<std::sync::Arc<PreparedBranch>>,
    pub reconciling: bool,
    pub height: Option<u64>,
    pub longest: Option<u64>,
    pub online: bool,
    pub last_block_time: Option<u64>,
    pub chain_id: Option<String>,
    pub chain_name: Option<String>,
    height_order: u64,
    longest_order: u64,
    tip_order: u64,
    tip_hash: Option<String>,
    last_tip: Option<String>,
    pub blocks: HashMap<String, ObservedBlock>,
    canonical: HashMap<u64, String>,
    mempool: HashSet<String>,
    membership_order: HashMap<String, u64>,
    mempool_order: u64,
}

impl ChainState {
    pub fn remember_block(
        &mut self,
        hash: String,
        height: u64,
        time: u64,
        previous: Option<String>,
    ) {
        self.blocks.entry(hash.clone()).or_insert(ObservedBlock {
            hash,
            height,
            time,
            previous,
            order: 0,
        });
    }
    pub fn apply_reconciliation(&mut self, branch: std::sync::Arc<PreparedBranch>) {
        if self
            .reconciled
            .as_ref()
            .is_none_or(|old| !std::sync::Arc::ptr_eq(old, &branch))
        {
            if branch.detached_count != 0 {
                self.mempool.clear();
            }
            if let Some(tip) = branch.tip() {
                self.height = Some(tip.height);
                self.tip_hash = Some(tip.hash.clone());
                self.last_tip = Some(tip.hash.clone());
                self.last_block_time = Some(tip.time);
            }
            let old = self.reconciled.replace(branch);
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn_blocking(move || drop(old));
            }
        }
        self.reconciling = false;
        self.online = true;
    }
    fn observe_height(&mut self, height: u64, order: u64) {
        if order < self.height_order {
            return;
        }
        if self.height != Some(height) {
            if self.height.is_some_and(|previous| height < previous) {
                self.canonical.clear();
                self.mempool.clear();
            }
            self.tip_hash = None;
        }
        self.height = Some(height);
        self.height_order = order;
    }

    pub fn observe(&mut self, observation: RpcObservation) {
        let RpcObservation {
            order,
            method,
            params,
            result,
            authorization,
        } = observation;
        if authorization.is_some_and(|(control, generation, epoch)| {
            !control.accepts(generation)
                || control
                    .branch_epoch
                    .load(std::sync::atomic::Ordering::Acquire)
                    != epoch
        }) {
            return;
        }
        if let Some(branch) = &self.reconciled
            && method != "getrawmempool"
        {
            if method == "getrawtransaction" {
                let active = result["blockhash"]
                    .as_str()
                    .is_some_and(|hash| branch.canonical_block(hash).is_some());
                if active
                    && let Some(txid) = result["txid"].as_str()
                    && order >= *self.membership_order.get(txid).unwrap_or(&0)
                {
                    self.mempool.remove(txid);
                    self.membership_order.insert(txid.into(), order);
                }
            }
            if method == "getblock"
                && result["hash"]
                    .as_str()
                    .is_some_and(|hash| branch.canonical_block(hash).is_some())
            {
                for tx in result["tx"].as_array().into_iter().flatten() {
                    if let Some(id) = tx.as_str().or_else(|| tx["txid"].as_str())
                        && order >= *self.membership_order.get(id).unwrap_or(&0)
                    {
                        self.mempool.remove(id);
                        self.membership_order.insert(id.into(), order);
                    }
                }
            }
            if method == "getinfo" {
                self.longest = result["longestchain"].as_u64().or(self.longest);
                self.chain_id = result["chainid"]
                    .as_str()
                    .map(str::to_owned)
                    .or(self.chain_id.take());
            }
            return;
        }
        match method.as_str() {
            "getinfo" | "getblockchaininfo" => {
                self.online = true;
                if order >= self.height_order
                    && let Some(time) = result.get("tiptime").and_then(Value::as_u64)
                {
                    if result.get("blocks").and_then(Value::as_u64) == self.height
                        && self
                            .last_block_time
                            .is_some_and(|previous| previous != time)
                    {
                        self.tip_hash = None;
                        self.canonical.clear();
                    }
                    self.last_block_time = Some(time);
                }
                if let Some(id) = result.get("chainid").and_then(Value::as_str) {
                    self.chain_id = Some(id.into());
                }
                if let Some(name) = result.get("name").and_then(Value::as_str) {
                    self.chain_name = Some(name.into());
                }
                if let Some(height) = result
                    .get("blocks")
                    .or_else(|| result.get("height"))
                    .or_else(|| result.get("blockheight"))
                    .and_then(Value::as_u64)
                {
                    self.observe_height(height, order);
                }
                if order >= self.longest_order
                    && let Some(longest) = result.get("longestchain").and_then(Value::as_u64)
                {
                    self.longest = Some(longest);
                    self.longest_order = order;
                }
                if let Some(hash) = result.get("bestblockhash").and_then(Value::as_str) {
                    self.observe_tip_hash(hash, order);
                }
            }
            "getblockcount" => {
                if let Some(height) = result.as_u64() {
                    self.online = true;
                    self.observe_height(height, order);
                }
            }
            "getbestblockhash" => {
                if let Some(hash) = result.as_str() {
                    self.online = true;
                    self.observe_tip_hash(hash, order);
                }
            }
            "getblock" | "getblockheader" => self.observe_block(&result, order),
            "getblockhash" => {
                if let Some(hash) = result.as_str() {
                    let height = params
                        .first()
                        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()));
                    if let Some(height) = height
                        && order >= self.height_order
                    {
                        if self
                            .canonical
                            .get(&height)
                            .is_some_and(|known| known != hash)
                        {
                            self.canonical.clear();
                            self.tip_hash = None;
                        }
                        if self.height.is_none_or(|known| height > known) {
                            self.observe_height(height, order);
                        }
                        self.canonical.insert(height, hash.into());
                        if self.height == Some(height) {
                            self.observe_tip_hash(hash, order);
                        }
                    }
                }
            }
            "gettxout" => {
                if let Some(hash) = result.get("bestblock").and_then(Value::as_str) {
                    self.observe_tip_hash(hash, order);
                }
            }
            "getcurrency" => {
                if result.get("currencyid").and_then(Value::as_str) == self.chain_id.as_deref()
                    && let Some(height) = result.get("bestheight").and_then(Value::as_u64)
                {
                    self.observe_height(height, order);
                }
            }
            "getrawtransaction" => {
                if let (Some(hash), Some(height), Some(time)) = (
                    result.get("blockhash").and_then(Value::as_str),
                    result.get("height").and_then(Value::as_u64),
                    result
                        .get("blocktime")
                        .or_else(|| result.get("time"))
                        .and_then(Value::as_u64),
                ) {
                    let previous = self
                        .blocks
                        .get(hash)
                        .and_then(|block| block.previous.clone());
                    self.observe_block(&serde_json::json!({"hash":hash,"height":height,"time":time,"confirmations":result.get("confirmations"),"previousblockhash":previous}), order);
                } else if let Some(confirmations) = result
                    .get("confirmations")
                    .and_then(Value::as_u64)
                    .filter(|c| *c > 0)
                    && let Some(block) = result
                        .get("blockhash")
                        .and_then(Value::as_str)
                        .and_then(|hash| self.blocks.get(hash))
                    && let Some(height) = block.height.checked_add(confirmations - 1)
                {
                    self.observe_height(height, order);
                }
                let txid = result
                    .get("txid")
                    .and_then(Value::as_str)
                    .or_else(|| params.first().and_then(Value::as_str));
                if let Some(txid) = txid
                    && order >= *self.membership_order.get(txid).unwrap_or(&0)
                {
                    // Zero confirmations alone is not proof of mempool membership.
                    if result
                        .get("confirmations")
                        .and_then(Value::as_i64)
                        .is_some_and(|c| c > 0)
                    {
                        self.mempool.remove(txid);
                        self.membership_order.insert(txid.into(), order);
                    }
                }
            }
            "getrawmempool" if order >= self.mempool_order => {
                let ids: HashSet<String> = if let Some(map) = result.as_object() {
                    map.keys().cloned().collect()
                } else if let Some(array) = result.as_array() {
                    array
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect()
                } else {
                    return;
                };
                let changed: HashSet<String> = self.mempool.union(&ids).cloned().collect();
                for txid in changed {
                    if order >= *self.membership_order.get(&txid).unwrap_or(&0) {
                        if ids.contains(&txid) {
                            self.mempool.insert(txid.clone());
                        } else {
                            self.mempool.remove(&txid);
                        }
                        self.membership_order.insert(txid, order);
                    }
                }
                self.mempool_order = order;
            }
            _ => {}
        }
    }

    fn observe_tip_hash(&mut self, hash: &str, order: u64) {
        if order < self.tip_order || order < self.height_order {
            return;
        }
        if self.tip_hash.as_deref().is_some_and(|old| old != hash) {
            self.canonical.clear();
        }
        self.tip_hash = Some(hash.into());
        self.tip_order = order;
        if let Some(block) = self.tip() {
            self.last_block_time = Some(block.time);
            self.last_tip = Some(hash.into());
        }
    }

    fn observe_block(&mut self, raw: &Value, order: u64) {
        let (Some(hash), Some(height), Some(time)) = (
            raw.get("hash").and_then(Value::as_str),
            raw.get("height").and_then(Value::as_u64),
            raw.get("time").and_then(Value::as_u64),
        ) else {
            return;
        };
        if self
            .blocks
            .get(hash)
            .is_some_and(|block| block.order > order)
        {
            return;
        }
        let block = ObservedBlock {
            hash: hash.into(),
            height,
            time,
            previous: raw
                .get("previousblockhash")
                .and_then(Value::as_str)
                .map(str::to_owned),
            order,
        };
        self.blocks.insert(hash.into(), block.clone());
        let confirmations = raw
            .get("confirmations")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        if confirmations <= 0 {
            if order < self.height_order {
                return;
            }
            if self.canonical.get(&height).is_some_and(|old| old == hash) {
                self.canonical
                    .retain(|known_height, _| *known_height < height);
                self.tip_hash = None;
            }
            if self.tip_hash.as_deref() == Some(hash) {
                self.tip_hash = None;
            }
            return;
        }
        let Some(tip_height) = height.checked_add(confirmations as u64 - 1) else {
            return;
        };
        // An older in-flight response must not reinstate a displaced branch.
        if order < self.height_order {
            return;
        }
        self.online = true;
        self.observe_height(tip_height, order);
        let replaced = self.canonical.get(&height).is_some_and(|old| old != hash);
        let broken_parent = height
            .checked_sub(1)
            .and_then(|h| self.canonical.get(&h))
            .zip(block.previous.as_ref())
            .is_some_and(|(known, parent)| known != parent);
        if replaced || broken_parent {
            self.tip_hash = None;
            self.canonical.clear();
            self.mempool.clear();
        }
        self.canonical.insert(height, hash.into());
        // Certify known ancestors by hash, never by a cached height alias.
        let mut cursor = block.previous.clone();
        let mut expected = height.checked_sub(1);
        while let Some(parent) = cursor.and_then(|hash| self.blocks.get(&hash)) {
            if Some(parent.height) != expected {
                break;
            }
            self.canonical.insert(parent.height, parent.hash.clone());
            expected = parent.height.checked_sub(1);
            cursor = parent.previous.clone();
        }
        if confirmations == 1 {
            self.last_block_time = Some(time);
            self.tip_hash = Some(hash.into());
            self.tip_order = order;
            self.last_tip = Some(hash.into());
        }
        if let Some(txs) = raw.get("tx").and_then(Value::as_array) {
            for tx in txs {
                if let Some(txid) = tx
                    .as_str()
                    .or_else(|| tx.get("txid").and_then(Value::as_str))
                    && order >= *self.membership_order.get(txid).unwrap_or(&0)
                {
                    self.mempool.remove(txid);
                    self.membership_order.insert(txid.into(), order);
                }
            }
        }
    }

    pub fn tip(&self) -> Option<&ObservedBlock> {
        self.tip_hash
            .as_ref()
            .and_then(|hash| {
                self.reconciled
                    .as_ref()
                    .and_then(|branch| branch.canonical_block(hash))
                    .or_else(|| self.blocks.get(hash))
            })
            .filter(|block| Some(block.height) == self.height)
    }

    pub fn block_badge(&self, hash: &str) -> Option<StatusBadge> {
        if let Some(branch) = &self.reconciled {
            if self.reconciling {
                return Some(StatusBadge::Unverified);
            }
            if branch.detached.contains_key(hash) {
                return Some(StatusBadge::Orphaned);
            }
            if branch.canonical_block(hash).is_none() {
                return Some(
                    if self
                        .blocks
                        .get(hash)
                        .is_some_and(|header| branch.canonical.contains_key(&header.height))
                    {
                        StatusBadge::Orphaned
                    } else {
                        StatusBadge::Unverified
                    },
                );
            }
            if !self.online {
                return Some(if self.last_tip.as_deref() == Some(hash) {
                    StatusBadge::LastObservedTip
                } else {
                    StatusBadge::LastObservedActive
                });
            }
            return Some(
                if self.tip().is_some_and(|b| b.hash == hash) && self.height == self.longest {
                    StatusBadge::ChainTip
                } else {
                    StatusBadge::Active
                },
            );
        }
        if !self.online {
            return (self.last_tip.as_deref() == Some(hash))
                .then_some(StatusBadge::LastObservedTip);
        }
        (self.tip().is_some_and(|block| block.hash == hash) && self.height == self.longest)
            .then_some(StatusBadge::ChainTip)
    }

    pub fn confirmations(&self, hash: &str) -> Option<u64> {
        if !self.online || self.reconciling {
            return None;
        }
        if let Some(branch) = &self.reconciled {
            return self
                .height?
                .checked_sub(branch.canonical_block(hash)?.height)?
                .checked_add(1);
        }
        let block = self.blocks.get(hash)?;
        if self.canonical.get(&block.height)? != hash {
            return None;
        }
        self.height?.checked_sub(block.height)?.checked_add(1)
    }

    pub fn remove_confirmed_from_mempool(&mut self, confirmed: &HashSet<String>) {
        self.mempool.retain(|txid| !confirmed.contains(txid));
    }

    pub fn in_mempool(&self, txid: &str) -> bool {
        self.online && !self.reconciling && self.mempool.contains(txid)
    }
}

pub fn coinbase_maturity(block_time: u64, chips777_testnet: bool) -> Option<u64> {
    if block_time == 0 {
        return None;
    }
    Some(if chips777_testnet || block_time >= 60 {
        100
    } else {
        (6000 / block_time).clamp(100, 149)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn observe(state: &mut ChainState, order: u64, method: &str, result: Value) {
        state.observe(RpcObservation {
            authorization: None,
            order,
            method: method.into(),
            params: Vec::new(),
            result,
        });
    }
    fn block(hash: &str, height: u64, confirmations: i64, parent: &str) -> Value {
        json!({"hash":hash,"height":height,"time":height*60,"confirmations":confirmations,"previousblockhash":parent,"tx":["included"]})
    }

    #[test]
    fn tip_badge_tracks_sync_hash_changes_and_offline_state() {
        let mut state = ChainState::default();
        observe(
            &mut state,
            1,
            "getinfo",
            json!({"blocks":10,"longestchain":10}),
        );
        observe(&mut state, 2, "getblock", block("ten", 10, 1, "nine"));
        assert_eq!(state.block_badge("ten"), Some(StatusBadge::ChainTip));
        observe(
            &mut state,
            3,
            "getinfo",
            json!({"blocks":10,"longestchain":11}),
        );
        assert_eq!(state.block_badge("ten"), None);
        observe(&mut state, 4, "getblockcount", json!(11));
        assert_eq!(state.block_badge("ten"), None);
        observe(&mut state, 5, "getblock", block("eleven", 11, 1, "ten"));
        assert_eq!(state.block_badge("eleven"), Some(StatusBadge::ChainTip));
        assert_eq!(state.confirmations("ten"), Some(2));
        observe(&mut state, 6, "getbestblockhash", json!("replacement"));
        assert_eq!(state.block_badge("eleven"), None);
        observe(
            &mut state,
            7,
            "getblock",
            block("replacement", 11, 1, "ten"),
        );
        assert_eq!(
            state.block_badge("replacement"),
            Some(StatusBadge::ChainTip)
        );
        assert_eq!(state.confirmations("eleven"), None);
        state.online = false;
        assert_eq!(
            state.block_badge("replacement"),
            Some(StatusBadge::LastObservedTip)
        );
        assert_eq!(state.confirmations("replacement"), None);
    }

    #[test]
    fn older_in_flight_responses_cannot_restore_a_stale_tip_or_longestchain() {
        let mut state = ChainState::default();
        observe(
            &mut state,
            10,
            "getinfo",
            json!({"blocks":10,"longestchain":10}),
        );
        observe(&mut state, 11, "getblock", block("ten", 10, 1, "nine"));
        observe(
            &mut state,
            12,
            "getinfo",
            json!({"blocks":11,"longestchain":11}),
        );
        observe(
            &mut state,
            8,
            "getinfo",
            json!({"blocks":9,"longestchain":9}),
        );
        observe(&mut state, 9, "getblock", block("ten", 10, 1, "nine"));
        assert_eq!(state.height, Some(11));
        assert_eq!(state.longest, Some(11));
        assert_eq!(state.block_badge("ten"), None);
    }

    #[test]
    fn a_live_historical_block_can_reveal_a_new_tip_height_without_becoming_the_tip() {
        let mut state = ChainState::default();
        observe(
            &mut state,
            1,
            "getinfo",
            json!({"blocks":10,"longestchain":10}),
        );
        observe(&mut state, 2, "getblock", block("ten", 10, 1, "nine"));
        observe(
            &mut state,
            3,
            "getblockheader",
            block("old", 1, 11, "genesis"),
        );
        assert_eq!(state.height, Some(11));
        assert_eq!(state.block_badge("ten"), None);
        assert_eq!(state.block_badge("old"), None);
    }

    #[test]
    fn mempool_requires_membership_and_can_return_after_disconnection() {
        let mut state = ChainState::default();
        observe(
            &mut state,
            1,
            "getinfo",
            json!({"blocks":10,"longestchain":10}),
        );
        observe(
            &mut state,
            2,
            "getrawtransaction",
            json!({"txid":"included","confirmations":0}),
        );
        assert!(!state.in_mempool("included"));
        observe(&mut state, 3, "getrawmempool", json!({"included":{}}));
        assert!(state.in_mempool("included"));
        observe(&mut state, 4, "getblock", block("ten", 10, 1, "nine"));
        assert!(!state.in_mempool("included"));
        observe(&mut state, 2, "getrawmempool", json!({"included":{}}));
        assert!(!state.in_mempool("included"));
        observe(
            &mut state,
            5,
            "getblockheader",
            block("ten", 10, -1, "nine"),
        );
        assert!(!state.in_mempool("included"));
        observe(&mut state, 6, "getrawmempool", json!(["included"]));
        assert!(state.in_mempool("included"));
        observe(&mut state, 7, "getrawmempool", json!([]));
        assert!(!state.in_mempool("included"));
    }

    #[test]
    fn transaction_metadata_updates_live_height_and_same_height_tip_time_invalidates_badge() {
        let mut state = ChainState::default();
        observe(
            &mut state,
            1,
            "getinfo",
            json!({"blocks":10,"longestchain":10,"tiptime":600}),
        );
        observe(
            &mut state,
            2,
            "getrawtransaction",
            json!({"txid":"reward","blockhash":"ten","height":10,"blocktime":600,"confirmations":1}),
        );
        assert_eq!(state.block_badge("ten"), Some(StatusBadge::ChainTip));
        assert_eq!(state.last_block_time, Some(600));
        observe(
            &mut state,
            3,
            "getinfo",
            json!({"blocks":10,"longestchain":10,"tiptime":650}),
        );
        assert_eq!(state.block_badge("ten"), None);
        assert_eq!(state.confirmations("ten"), None);
        assert_eq!(state.last_block_time, Some(650));
        observe(
            &mut state,
            2,
            "getinfo",
            json!({"blocks":10,"longestchain":10,"tiptime":600}),
        );
        assert_eq!(state.last_block_time, Some(650));
    }

    #[test]
    fn disconnected_ancestor_invalidates_descendants() {
        let mut state = ChainState::default();
        observe(
            &mut state,
            1,
            "getinfo",
            json!({"blocks":10,"longestchain":10}),
        );
        observe(&mut state, 2, "getblock", block("nine", 9, 2, "eight"));
        observe(&mut state, 3, "getblock", block("ten", 10, 1, "nine"));
        observe(&mut state, 4, "getblock", block("nine", 9, -1, "eight"));
        assert_eq!(state.confirmations("ten"), None);
        assert_eq!(state.block_badge("ten"), None);
    }

    #[test]
    fn maturity_matches_daemon_chain_rules() {
        assert_eq!(coinbase_maturity(60, false), Some(100));
        assert_eq!(coinbase_maturity(120, false), Some(100));
        assert_eq!(coinbase_maturity(50, false), Some(120));
        assert_eq!(coinbase_maturity(10, false), Some(149));
        assert_eq!(coinbase_maturity(10, true), Some(100));
        assert_eq!(coinbase_maturity(0, false), None);
    }

    #[test]
    fn reconciled_mapping_resists_rpc_membership_claims_and_clears_confirmed_mempool_entries() {
        let header = crate::reconciliation::Header {
            hash: "active".into(),
            height: 10,
            parent: Some("parent".into()),
            time: 600,
        };
        let mut branch = crate::reconciliation::State::default();
        branch.canonical.insert(10, header.clone());
        let change = crate::reconciliation::Change {
            state: branch,
            attached: vec![header],
            detached: vec![],
            ancestor: None,
        };
        let mut state = ChainState::default();
        state.apply_reconciliation(std::sync::Arc::new(change.into()));
        observe(&mut state, 1, "getrawmempool", json!(["tx"]));
        assert!(state.in_mempool("tx"));
        observe(
            &mut state,
            2,
            "getblockheader",
            block("other", 10, 999, "parent"),
        );
        assert_eq!(state.confirmations("active"), Some(1));
        assert_eq!(state.confirmations("other"), None);
        observe(
            &mut state,
            3,
            "getrawtransaction",
            json!({"txid":"tx","blockhash":"active","confirmations":1}),
        );
        assert!(!state.in_mempool("tx"));
        state.reconciling = true;
        assert_eq!(state.confirmations("active"), None);
        assert_eq!(state.block_badge("active"), Some(StatusBadge::Unverified));
        state.reconciling = false;
        state.online = false;
        assert_eq!(
            state.block_badge("active"),
            Some(StatusBadge::LastObservedTip)
        );
    }
}
