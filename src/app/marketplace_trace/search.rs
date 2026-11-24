use super::super::{
    AppEvent, AppState, Config, Screen, TransactionFlowKind, TransactionRecord, TransientNotice,
    Value, is_hash_like, marketplace_funding_shape, transaction_flow_selections, transaction_flows,
    transaction_record,
};
use super::publication;
use crate::rpc::RpcClient;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{sync::mpsc::UnboundedSender, time::Instant};

const BLOCKS_PER_PASS: u64 = 8;
const RPC_TIMEOUT: Duration = Duration::from_secs(8);

#[derive(Debug, Clone)]
pub(in crate::app) struct Cursor {
    request: Option<Arc<()>>,
    next: u64,
    last_hash: Option<String>,
}

#[derive(Debug)]
pub(in crate::app) struct ResultPage {
    next: u64,
    last_hash: Option<String>,
    result: Result<Option<TransactionRecord>, &'static str>,
    end: u64,
}

fn notice(state: &mut AppState, message: String) {
    state.notice = Some(TransientNotice {
        message,
        until: Instant::now() + Duration::from_secs(12),
    });
}

pub(in crate::app) fn start(
    state: &mut AppState,
    config: &Config,
    sender: &UnboundedSender<AppEvent>,
) {
    let Screen::Transaction {
        txid,
        selected_flow,
        ..
    } = &state.screen
    else {
        return;
    };
    let Some(viewed) = state.store.transaction(txid) else {
        return;
    };
    let flows = transaction_flows(&viewed.raw_json, &state.store);
    let selected_input = transaction_flow_selections(&flows)
        .get(*selected_flow)
        .and_then(|(index, _)| flows.get(*index))
        .filter(|flow| flow.kind == TransactionFlowKind::Input && flow.source_vout == Some(0))
        .and_then(|flow| flow.source_txid.as_ref());
    let candidates = selected_input.into_iter().chain(std::iter::once(txid));
    let target = candidates
        .filter_map(|candidate| {
            let tx = state.store.transaction(candidate)?;
            if !marketplace_funding_shape(&tx.raw_json)
                || state.store.marketplace_contract(candidate).is_some()
                || super::orphan(&tx.raw_json)
            {
                return None;
            }
            Some((
                candidate.clone(),
                tx.block_height?,
                tx.block_hash.clone()?,
                tx.source_id.clone()?,
                tx.raw_json["vout"][0]["spentHeight"]
                    .as_u64()
                    .filter(|height| *height > 0)
                    .or_else(|| {
                        (selected_input == Some(candidate) && viewed.source_id == tx.source_id)
                            .then_some(viewed.block_height)
                            .flatten()
                    }),
            ))
        })
        .next();
    let Some((funding, start, hash, source, spent_height)) = target else {
        notice(
            state,
            "No confirmed funding candidate needs an offer proof search".into(),
        );
        return;
    };
    let cursor = state
        .store
        .marketplace_searches
        .entry(funding.clone())
        .or_insert(Cursor {
            request: None,
            next: start,
            last_hash: None,
        });
    if cursor.request.is_some() {
        notice(
            state,
            "Searching confirmed blocks for the offer proof…".into(),
        );
        return;
    }
    let request = Arc::new(());
    cursor.request = Some(request.clone());
    let next = cursor.next;
    let last_hash = cursor.last_hash.clone();
    let mut daemon = config.daemon.clone();
    daemon.background = true;
    daemon.observations = None;
    let sender = sender.clone();
    notice(
        state,
        format!("Searching for the offer proof from block {next}…"),
    );
    tokio::spawn(async move {
        let page = if let Ok(client) = RpcClient::new(daemon) {
            scan(
                &client,
                &source,
                &funding,
                start,
                &hash,
                (next, last_hash),
                spent_height,
            )
            .await
        } else {
            ResultPage {
                next,
                last_hash,
                end: next,
                result: Err("Source connection could not be initialized"),
            }
        };
        let _ = sender.send(AppEvent::MarketplaceSearch {
            funding,
            request,
            page: Box::new(page),
        });
    });
}

async fn rpc(client: &RpcClient, source: &str, method: &str, params: &[Value]) -> Option<Value> {
    tokio::time::timeout(RPC_TIMEOUT, client.call_on(source, method, params))
        .await
        .ok()?
        .ok()
}

async fn scan(
    client: &RpcClient,
    source: &str,
    funding: &str,
    start: u64,
    hash: &str,
    resume: (u64, Option<String>),
    spent_height: Option<u64>,
) -> ResultPage {
    let (next, last_hash) = resume;
    let mut page = ResultPage {
        next,
        last_hash,
        end: next,
        result: Ok(None),
    };
    let start_hash = rpc(client, source, "getblockhash", &[json!(start)]).await;
    if start_hash.as_ref().and_then(Value::as_str) != Some(hash) {
        page.result = Err("Could not verify the funding block on its source");
        return page;
    }
    if let Some(previous) = &page.last_hash {
        let observed = rpc(client, source, "getblockhash", &[json!(next - 1)]).await;
        match observed.as_ref().and_then(Value::as_str) {
            Some(current) if current == previous => {}
            Some(_) => {
                page.next = start;
                page.last_hash = None;
                page.result = Err("Previously searched blocks changed");
                return page;
            }
            None => {
                page.result = Err("Could not verify the last searched block");
                return page;
            }
        }
    }
    let tip = rpc(client, source, "getblockcount", &[])
        .await
        .and_then(|v| v.as_u64());
    let Some(tip) = tip else {
        page.result = Err("Could not read the source chain height");
        return page;
    };
    page.end = spent_height.unwrap_or(tip).min(tip);
    let last = page.end.min(next.saturating_add(BLOCKS_PER_PASS - 1));
    for height in next..=last {
        let Some(block) = rpc(
            client,
            source,
            "getblock",
            &[json!(height.to_string()), json!(2)],
        )
        .await
        else {
            page.result = Err("Could not read full transactions at the next block");
            return page;
        };
        let observed = block["hash"].as_str().filter(|value| is_hash_like(value));
        let Some(block_hash) = observed.filter(|_| {
            block["height"].as_u64() == Some(height)
                && !super::orphan(&block)
                && (height != start || observed == Some(hash))
                && page
                    .last_hash
                    .as_deref()
                    .is_none_or(|parent| block["previousblockhash"].as_str() == Some(parent))
        }) else {
            page.result = Err("Block membership changed during the search");
            return page;
        };
        let Some(txs) = block["tx"]
            .as_array()
            .filter(|items| !items.is_empty() && items.iter().all(Value::is_object))
        else {
            page.result = Err("Source cannot supply full blocks for proof discovery");
            return page;
        };
        for raw in txs {
            if publication(raw).is_some_and(|offer| offer.funding == funding)
                && let Some(txid) = raw["txid"].as_str().filter(|id| is_hash_like(id))
            {
                let mut record = transaction_record(txid, raw.clone());
                let verified = rpc(client, source, "getblockhash", &[json!(height)]).await;
                if verified.as_ref().and_then(Value::as_str) != Some(block_hash) {
                    page.result = Err("Proof block changed before it could be verified");
                    return page;
                }
                record.block_hash = Some(block_hash.into());
                record.block_height = Some(height);
                record.source_id = Some(source.into());
                page.result = Ok(Some(record));
                return page;
            }
        }
        page.last_hash = Some(block_hash.to_owned());
        page.next = height + 1;
    }
    page
}

pub(in crate::app) fn finish(
    state: &mut AppState,
    funding: String,
    request: Arc<()>,
    page: ResultPage,
) {
    let Some(cursor) = state
        .store
        .marketplace_searches
        .get_mut(&funding)
        .filter(|cursor| {
            cursor
                .request
                .as_ref()
                .is_some_and(|active| Arc::ptr_eq(active, &request))
        })
    else {
        return;
    };
    cursor.request = None;
    cursor.next = page.next;
    cursor.last_hash = page.last_hash;
    let message = match page.result {
        Ok(Some(proof)) => {
            state.store.insert_transaction(proof);
            "Signed offer publication found. Follow the new link in the transaction.".into()
        }
        Ok(None) if page.next <= page.end => format!(
            "No proof through block {}. Press e to search from {}.",
            page.next - 1,
            page.next
        ),
        Ok(None) => format!("No matching publication through block {}.", page.end),
        Err(reason) => format!("{reason}. Press e to retry at block {}.", page.next),
    };
    notice(state, message);
}
