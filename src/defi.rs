//! Bounded, output-centered DeFi trail with guarded import-output replay.
use crate::{config::Config, rpc::RpcClient, world::OutpointRef};
use serde_json::{Value, json};
use tokio::sync::mpsc::UnboundedSender;

mod fee_pool;
#[cfg(test)]
mod fixtures;
mod import;
mod import_rewards;
mod launch_funding;
pub mod pool;
mod replay;
mod route;
mod summary;
mod supply_burn;
pub(crate) use replay::{address_payload, amount_sats};
pub use route::{SearchPlan, investigate};
pub use summary::{Amount, LegSummary, ReceiptKind, Stage};
pub(crate) use supply_burn::issuer_instruction;

pub const IMPORT_WINDOW: u64 = 1_000;
pub const MAX_SCAN_WINDOWS: u32 = 32;

#[derive(Debug, Clone, Copy, Default)]
pub enum ImportSearch {
    #[default]
    Initial,
    WindowEndingAt(u64),
    BackwardFrom(u64),
}

#[derive(Debug, Clone)]
pub struct SearchProgress {
    pub system: String,
    pub first: u64,
    pub last: u64,
    pub tip: u64,
    pub windows: u32,
    pub more_older: bool,
}

#[derive(Debug, Clone)]
pub struct Stop {
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct Entry {
    pub output: OutpointRef,
    pub label: String,
    pub detail: String,
}

#[derive(Debug, Clone)]
pub struct LifecycleEdge {
    pub source: OutpointRef,
    pub target: OutpointRef,
    pub relation: String,
    pub resolution: String,
    pub detail: String,
}

#[derive(Debug, Clone)]
pub struct Continuation {
    pub output: OutpointRef,
    pub transaction: Value,
}

#[derive(Debug, Default, Clone)]
pub struct Trail {
    pub entries: Vec<Entry>,
    pub notes: Vec<String>,
    pub stop: Option<Stop>,
    pub search: Option<SearchProgress>,
    pub searching: bool,
    pub search_root: Option<OutpointRef>,
    pub import_heights: Vec<(OutpointRef, u64)>,
    pub next_leg: Option<Continuation>,
    pub edges: Vec<LifecycleEdge>,
    pub legs: Vec<LegSummary>,
}

/// Keep structural and attributed relationships distinct. A destination match
/// remains a candidate even when both transactions are confirmed.
pub fn lifecycle_edges(root: &OutpointRef, trail: &Trail) -> Vec<LifecycleEdge> {
    if !trail.edges.is_empty() {
        return trail.edges.clone();
    }
    let mut edges = Vec::new();
    let export = trail.entries.iter().find(|entry| entry.label == "Export");
    if let Some(export) = export {
        edges.push(LifecycleEdge {
            source: root.clone(),
            target: export.output.clone(),
            relation: "spent-by-export".into(),
            resolution: "structural".into(),
            detail: export.detail.clone(),
        });
        if let Some(import) = trail.entries.iter().find(|entry| entry.label == "Import") {
            edges.push(LifecycleEdge {
                source: export.output.clone(),
                target: import.output.clone(),
                relation: "export-import".into(),
                resolution: "structural".into(),
                detail: import.detail.clone(),
            });
            for result in trail.entries.iter().filter(|entry| {
                matches!(
                    entry.label.as_str(),
                    "Replayed next-leg transfer"
                        | "Replayed launch proceeds"
                        | "Replayed preconversion result"
                        | "Replayed local gateway payout"
                        | "Replayed refund result"
                        | "Replayed reserve result"
                        | "Replayed conversion result"
                        | "Replayed transfer result"
                        | "Direct transfer result"
                        | "Destination-address candidate"
                )
            }) {
                let attributed = result.label != "Destination-address candidate";
                if result.label == "Replayed next-leg transfer" {
                    edges.push(LifecycleEdge {
                        source: root.clone(),
                        target: result.output.clone(),
                        relation: "next-leg".into(),
                        resolution: "attributed".into(),
                        detail: result.detail.clone(),
                    });
                }

                edges.push(LifecycleEdge {
                    source: import.output.clone(),
                    target: result.output.clone(),
                    relation: "import-generated-output".into(),
                    resolution: if attributed {
                        "attributed"
                    } else {
                        "candidate"
                    }
                    .into(),
                    detail: format!("{}: {}", result.label, result.detail),
                });
            }
        }
    }
    edges
}

pub fn retained_trail(
    root: &OutpointRef,
    edges: &[crate::cache::defi::StoredLifecycleEdge],
) -> Option<Trail> {
    if edges.is_empty() {
        return None;
    }
    let mut trail = Trail {
        searching: true,
        ..Trail::default()
    };
    trail.entries.push(entry(
        root,
        "Selected transfer",
        "Retained lifecycle evidence",
    ));
    for edge in edges {
        let status = if edge.source_membership == "orphan" || edge.target_membership == "orphan" {
            "orphaned"
        } else if edge.source_membership == "member" && edge.target_membership == "member" {
            "last observed member"
        } else {
            "membership unverified"
        };
        trail.entries.push(entry(
            &edge.target,
            format!("Retained {} ({})", edge.relation, edge.resolution),
            format!(
                "{}; {status}; source {}:{} via {} at {} -> target {}:{} via {} at {}",
                edge.detail,
                edge.source_height,
                edge.source_block_hash,
                edge.source_id,
                edge.source.txid,
                edge.target_height,
                edge.target_block_hash,
                edge.target_source_id,
                edge.target.txid
            ),
        ));
    }
    trail.notes.push("Retained lifecycle edges are source/block-qualified observations; refresh to establish current membership.".into());
    Some(trail)
}

struct Spender {
    txid: String,
    input: Option<u64>,
}

pub fn roots(transaction: &Value) -> Vec<(u32, String)> {
    transaction["vout"]
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
        .filter_map(|(i, output)| {
            if output["scriptPubKey"]["feepool"].is_object() {
                return Some((i as u32, "Fee pool checkpoint".into()));
            }
            if let Some(import) = output["scriptPubKey"].get("crosschainimport") {
                return (import["flags"]
                    .as_u64()
                    .is_none_or(|flags| flags & 0x60 == 0))
                .then(|| (i as u32, "Currency import".into()));
            }
            let transfer = output["scriptPubKey"].get("reservetransfer")?;
            Some((i as u32, transfer_label(transfer).into()))
        })
        .collect()
}

fn transfer_label(transfer: &Value) -> &'static str {
    if transfer["preconvert"].as_bool() == Some(true) {
        "Launch contribution"
    } else if transfer["convert"].as_bool() == Some(true) {
        "Conversion"
    } else if transfer["crosssystem"].as_bool() == Some(true) {
        "Cross-system transfer"
    } else {
        "Reserve transfer"
    }
}

fn output(tx: &Value, vout: u32) -> Option<&Value> {
    tx["vout"].as_array()?.get(vout as usize)
}

fn entry(root: &OutpointRef, label: impl Into<String>, detail: impl Into<String>) -> Entry {
    Entry {
        output: root.clone(),
        label: label.into(),
        detail: detail.into(),
    }
}

fn stop(trail: &mut Trail, message: impl Into<String>) -> Trail {
    trail.stop = Some(Stop {
        message: message.into(),
    });
    trail.clone()
}

fn valid_txid(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

fn matched_export(tx: &Value, input: usize) -> Option<(u32, &Value)> {
    tx["vout"]
        .as_array()?
        .iter()
        .enumerate()
        .find_map(|(index, output)| {
            let export = output["scriptPubKey"].get("crosschainexport")?;
            let first = usize::try_from(export["firstinput"].as_u64()?).ok()?;
            let count = usize::try_from(export["numinputs"].as_u64()?).ok()?;
            (input >= first && input < first.checked_add(count)?).then_some((index as u32, export))
        })
}

fn matches_import(import: &Value, source_system: &str, export_id: &str, export_vout: u32) -> bool {
    import["exporttxid"].as_str() == Some(export_id)
        && import["exporttxout"].as_u64() == Some(u64::from(export_vout))
        && import["sourcesystemid"].as_str() == Some(source_system)
}

/// Mirror CReserveTransfer::GetImportCurrency. For reserve-to-reserve RPC
/// JSON, `via` represents the serialized destCurrencyID (the processing pool).
/// IMPORT_TO_SOURCE instead selects the one supplied currency.
fn transfer_import_currency(transfer: &Value) -> Option<&str> {
    let flags = transfer["flags"].as_u64()?;
    if flags & 0x200 != 0 {
        let values = transfer["currencyvalues"].as_object()?;
        if values.len() != 1 {
            return None;
        }
        values.keys().next().map(String::as_str)
    } else if flags & 0x400 != 0 {
        transfer["via"].as_str()
    } else {
        transfer["destinationcurrencyid"].as_str()
    }
}

fn transfer_matches(source: &Value, imported: &Value) -> bool {
    // The RPC decoder can add display fields, so compare the consensus-facing
    // transfer fields rather than the entire JSON object.
    [
        "currencyvalues",
        "flags",
        "feecurrencyid",
        "fees",
        "destinationcurrencyid",
        "via",
        "exportto",
        "destination",
    ]
    .into_iter()
    .all(|key| source.get(key) == imported.get(key))
}

pub(crate) fn generated_region<'a>(
    tx: &'a Value,
    import: &Value,
    import_vout: u32,
) -> Option<(usize, &'a [Value])> {
    let outputs = tx["vout"].as_array()?;
    let count = usize::try_from(import["numoutputs"].as_u64()?).ok()?;
    let mut context_end = import_vout as usize;
    for (index, out) in outputs.iter().enumerate().skip(context_end + 1) {
        let script = &out["scriptPubKey"];
        if script.get("acceptednotarization").is_some()
            || script.get("earnednotarization").is_some()
            || script.get("notaryevidence").is_some()
            || script["crosschainexport"]["issupplemental"].as_bool() == Some(true)
        {
            context_end = index;
        }
    }
    let start = context_end.checked_add(1)?;
    let end = start.checked_add(count)?;
    Some((start, outputs.get(start..end)?))
}

fn plain_transfer(transfer: &Value) -> bool {
    matches!(transfer["flags"].as_u64(), Some(1 | 65))
        && !(transfer["convert"].as_bool() == Some(true)
            || transfer["reservetoreserve"].as_bool() == Some(true)
            || transfer["burn"].as_bool() == Some(true)
            || transfer["mint"].as_bool() == Some(true)
            || transfer["refund"].as_bool() == Some(true))
}

fn output_matches_transfer(transfer: &Value, output: &Value) -> bool {
    let script = &output["scriptPubKey"];
    let address = transfer["destination"]["address"].as_str();
    let matches_address = script["addresses"]
        .as_array()
        .is_some_and(|addresses| addresses.iter().any(|item| item.as_str() == address))
        || script["reservetransfer"]["destination"]["address"].as_str() == address;
    let ordinary_result = script["spendableoutput"].as_bool() == Some(true)
        || script.get("reserveoutput").is_some()
        || script.get("reservetransfer").is_some();
    matches_address && ordinary_result
}

fn direct_result(transfer: &Value, batch: &[Value], region: &[Value], candidate: usize) -> bool {
    batch.len() == 1
        && region.len() == 1
        && candidate == 0
        && plain_transfer(transfer)
        && region[0]["scriptPubKey"].get("reserveoutput").is_none()
        && output_matches_transfer(transfer, &region[0])
}

fn ordered_plain_result(
    batch: &[Value],
    region: &[Value],
    selected: usize,
    candidate: usize,
    import_notarization: &Value,
) -> bool {
    batch.len() > 1
        && batch.len() == region.len()
        && selected == candidate
        && import_notarization["launchcomplete"].as_bool() == Some(true)
        && import_notarization["launchconfirmed"].as_bool() == Some(true)
        && batch.iter().all(plain_transfer)
        && batch.iter().zip(region).all(|(transfer, output)| {
            output["scriptPubKey"].get("reserveoutput").is_none()
                && output_matches_transfer(transfer, output)
        })
}

/// Uses decoded spent metadata first; the daemon may require a spent index for
/// `getspentinfo`, so absence of that index is reported rather than guessed.
async fn investigate_leg(
    configs: Vec<Config>,
    root: OutpointRef,
    seed: Value,
    search: ImportSearch,
    updates: Option<route::Progress>,
    fresh_seed: bool,
) -> Trail {
    let mut trail = Trail::default();
    let Some(config) = configs
        .iter()
        .find(|c| c.chain.system_id.get() == Some(&root.system_id))
    else {
        return stop(
            &mut trail,
            format!("No source configured for {}", root.system_id),
        );
    };
    let Ok(client) = RpcClient::for_config(config) else {
        return stop(&mut trail, "Cannot construct source RPC client");
    };
    let tx = if fresh_seed {
        seed
    } else {
        match client.get_raw_transaction(&root.txid).await {
            Ok(response) => response.result,
            Err(error) => {
                trail
                    .notes
                    .push(format!("Fresh source transaction unavailable: {error}"));
                seed
            }
        }
    };
    if tx["txid"].as_str() != Some(&root.txid) {
        return stop(
            &mut trail,
            "Source transaction ID differs from selected output",
        );
    }
    let Some(source_output) = output(&tx, root.vout) else {
        return stop(&mut trail, "Selected output is absent");
    };
    let Some(transfer) = source_output["scriptPubKey"].get("reservetransfer") else {
        return stop(&mut trail, "Selected output is not a reserve transfer");
    };
    trail.legs.push(LegSummary::new(root.clone(), transfer));
    if tx["confirmations"].as_i64().is_some_and(|count| count <= 0) {
        trail.legs[0].stage = Stage::Unconfirmed;
        return stop(
            &mut trail,
            "Source transfer is unconfirmed or outside the active chain; its route is provisional",
        );
    }
    trail.entries.push(entry(
        &root,
        transfer_label(transfer),
        format!(
            "values={}  fee={} {}  import currency={}  destination={}  address={}",
            transfer["currencyvalues"],
            transfer["fees"],
            transfer["feecurrencyid"].as_str().unwrap_or("?"),
            transfer_import_currency(transfer).unwrap_or("—"),
            transfer["destinationcurrencyid"].as_str().unwrap_or("?"),
            transfer["destination"]["address"].as_str().unwrap_or("?"),
        ),
    ));
    let Some(spend_id) = source_output["spentTxId"]
        .as_str()
        .filter(|s| valid_txid(s))
    else {
        let result = client
            .call_with_raw::<Value>(
                "getspentinfo",
                &[json!({"txid": root.txid, "index": root.vout})],
            )
            .await;
        match result {
            Ok(spent) => {
                return continue_from_spender(
                    configs,
                    root,
                    transfer.clone(),
                    Spender {
                        txid: spent.result["txid"].as_str().unwrap_or_default().to_owned(),
                        input: spent.result["index"].as_u64(),
                    },
                    trail,
                    search,
                    updates,
                )
                .await;
            }
            Err(error) => {
                let import_currency = transfer_import_currency(transfer);
                if let Some(currency) = import_currency {
                    match client
                        .call_nullable_with_raw::<Value>("getpendingtransfers", &[json!(currency)])
                        .await
                    {
                        Ok(response) => {
                            let pending = response
                                .result
                                .as_ref()
                                .and_then(Value::as_array)
                                .is_some_and(|items| {
                                    items.iter().any(|item| {
                                        item["txid"].as_str() == Some(&root.txid)
                                            && item["n"].as_u64() == Some(u64::from(root.vout))
                                    })
                                });
                            if pending {
                                trail.legs[0].stage = Stage::Pending;
                                return stop(
                                    &mut trail,
                                    format!(
                                        "Transfer remains in getpendingtransfers for {currency}; no export observed"
                                    ),
                                );
                            }
                        }
                        Err(pending_error) => trail.notes.push(format!(
                            "Pending-transfer lookup unavailable: {pending_error}"
                        )),
                    }
                }
                match client.get_tx_out(&root.txid, u64::from(root.vout)).await {
                    Ok(response) if response.result.is_some() => {
                        trail.legs[0].stage = Stage::Unspent;
                        return stop(&mut trail, "Output is unspent, but no pending-transfer record was observed. No export has been established.");
                    },
                    Ok(_) => trail.notes.push("gettxout returned null: spent or unknown; it does not identify the spender.".into()),
                    Err(out_error) => trail.notes.push(format!("gettxout unavailable: {out_error}")),
                }
                return stop(
                    &mut trail,
                    format!(
                        "Spender unresolved: getspentinfo unavailable ({error}). A spent-indexed daemon or another confirmed spender reference is needed."
                    ),
                );
            }
        }
    };
    continue_from_spender(
        configs,
        root,
        transfer.clone(),
        Spender {
            txid: spend_id.into(),
            input: source_output["spentIndex"].as_u64(),
        },
        trail,
        search,
        updates,
    )
    .await
}

async fn continue_from_spender(
    configs: Vec<Config>,
    root: OutpointRef,
    transfer: Value,
    spender: Spender,
    mut trail: Trail,
    search: ImportSearch,
    updates: Option<route::Progress>,
) -> Trail {
    let Spender {
        txid: spend_id,
        input: spend_input,
    } = spender;
    let Some(config) = configs
        .iter()
        .find(|c| c.chain.system_id.get() == Some(&root.system_id))
    else {
        return stop(&mut trail, "Source configuration disappeared");
    };
    if !valid_txid(&spend_id) {
        return stop(&mut trail, "Spender transaction ID is invalid");
    }
    let Ok(client) = RpcClient::for_config(config) else {
        return stop(&mut trail, "Cannot construct source RPC client");
    };
    let (spent, export_source) = match client.get_raw_transaction(&spend_id).await {
        Ok(v) => (v.result, v.source_id),
        Err(error) => {
            return stop(
                &mut trail,
                format!("Cannot load spender {spend_id}: {error}"),
            );
        }
    };
    if spent["txid"].as_str() != Some(&spend_id)
        || spent["confirmations"]
            .as_i64()
            .is_some_and(|count| count <= 0)
    {
        return stop(
            &mut trail,
            "Spender transaction is mismatched, unconfirmed, or outside the active chain",
        );
    }
    let Some(inputs) = spent["vin"].as_array() else {
        return stop(&mut trail, "Spender has no decoded inputs");
    };
    let input = match spend_input.and_then(|v| usize::try_from(v).ok()) {
        Some(index)
            if inputs.get(index).is_some_and(|v| {
                v["txid"].as_str() == Some(&root.txid)
                    && v["vout"].as_u64() == Some(u64::from(root.vout))
            }) =>
        {
            index
        }
        _ => match inputs.iter().position(|v| {
            v["txid"].as_str() == Some(&root.txid)
                && v["vout"].as_u64() == Some(u64::from(root.vout))
        }) {
            Some(index) => index,
            None => {
                return stop(
                    &mut trail,
                    "Decoded spender does not consume the selected outpoint",
                );
            }
        },
    };
    let Some((export_vout, export)) = matched_export(&spent, input) else {
        trail.entries.push(entry(
            &OutpointRef {
                system_id: root.system_id.clone(),
                txid: spend_id,
                vout: 0,
            },
            "Spent outside an export",
            format!("Input {input} consumes the transfer; no export's input range includes it"),
        ));
        return stop(
            &mut trail,
            "This spender is not a verified export for the selected transfer",
        );
    };
    trail.legs[0].stage = Stage::Exported;
    let export_ref = OutpointRef {
        system_id: root.system_id.clone(),
        txid: spend_id.clone(),
        vout: export_vout,
    };
    trail.entries.push(entry(&export_ref, "Export", format!(
        "source input {input}; batch inputs {}..{}; destination system={} currency={} hashtransfers={}",
        export["firstinput"], export["firstinput"].as_u64().unwrap_or(0).saturating_add(export["numinputs"].as_u64().unwrap_or(0)),
        export["destinationsystemid"].as_str().unwrap_or("?"), export["destinationcurrencyid"].as_str().unwrap_or("?"), export["hashtransfers"].as_str().unwrap_or("?"),
    )));
    let Some(destination) = export["destinationsystemid"].as_str() else {
        return stop(&mut trail, "Export has no destination system ID");
    };
    let companion_exports: Vec<u32> = spent["vout"]
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
        .filter_map(|(index, output)| {
            let system_export = output["scriptPubKey"].get("crosschainexport")?;
            (index != export_vout as usize
                && system_export["destinationsystemid"].as_str() == Some(destination)
                && system_export["destinationcurrencyid"].as_str() == Some(destination))
            .then_some(index as u32)
        })
        .collect();
    for &vout in &companion_exports {
        trail.entries.push(entry(
            &OutpointRef {
                system_id: root.system_id.clone(),
                txid: spend_id.clone(),
                vout,
            },
            "Companion system export",
            "System-thread record for this export batch; not a second payment",
        ));
    }
    let Some(dest_config) = configs.iter().find(|c| {
        c.chain
            .system_id
            .get()
            .is_some_and(|id| id.as_ref() == destination)
    }) else {
        return stop(
            &mut trail,
            format!(
                "Export is on chain, but destination system {destination} has no configured source. Add it in F2, then refresh."
            ),
        );
    };
    let Some(currency) = export["destinationcurrencyid"].as_str() else {
        return stop(&mut trail, "Export has no destination currency ID");
    };
    let Ok(dest_client) = RpcClient::for_config(dest_config) else {
        return stop(&mut trail, "Cannot construct destination RPC client");
    };
    let tip = match dest_client.get_block_count().await {
        Ok(v) => v.result,
        Err(error) => return stop(&mut trail, format!("Cannot read destination tip: {error}")),
    };
    let default_end = if destination == root.system_id.as_ref() {
        let Some(height) = spent["height"].as_u64() else {
            return stop(
                &mut trail,
                "Export is not confirmed; import cannot yet be bounded",
            );
        };
        let ordinary_end = height.saturating_add(IMPORT_WINDOW - 1).min(tip);
        if matches!(search, ImportSearch::Initial)
            && transfer["flags"].as_u64() == Some(7)
            && let Ok(definition) = dest_client
                .call_on(&export_source, "getcurrency", &[json!(currency)])
                .await
            && definition["currencyid"].as_str() == Some(currency)
            && definition["launchsystemid"].as_str() == Some(destination)
            && let Some(launch) = definition["startblock"].as_u64()
            && launch > ordinary_end
            && launch <= tip
        {
            let end = launch.saturating_add(IMPORT_WINDOW / 2).min(tip);
            trail.notes.push(format!("Initial import window is centered near the currency's scheduled launch at {launch}. Use [ or s to check earlier windows."));
            end
        } else {
            ordinary_end
        }
    } else {
        tip
    };
    let mut end = match search {
        ImportSearch::Initial => default_end,
        ImportSearch::WindowEndingAt(height) | ImportSearch::BackwardFrom(height) => {
            height.min(tip)
        }
    };
    let limit = if matches!(search, ImportSearch::BackwardFrom(_)) {
        MAX_SCAN_WINDOWS
    } else {
        1
    };
    let mut found: Option<Value> = None;
    let mut import_source = None;
    for window in 1..=limit {
        let start = end.saturating_sub(IMPORT_WINDOW - 1);
        let (imports, window_source) = match dest_client
            .call_nullable_with_raw::<Value>(
                "getimports",
                &[json!(currency), json!(start), json!(end)],
            )
            .await
        {
            Ok(v) => (v.result.unwrap_or(Value::Null), v.source_id),
            Err(error) => {
                return stop(
                    &mut trail,
                    format!("Destination getimports failed at {start}..{end}: {error}"),
                );
            }
        };
        if !imports.is_null() && !imports.is_array() {
            return stop(
                &mut trail,
                format!("Destination getimports returned an unexpected result at {start}..{end}"),
            );
        }
        let first = trail
            .search
            .as_ref()
            .map_or(start, |prior| prior.first.min(start));
        let last = trail
            .search
            .as_ref()
            .map_or(end, |prior| prior.last.max(end));
        trail.search_root = Some(root.clone());
        trail.search = Some(SearchProgress {
            system: destination.into(),
            first,
            last,
            tip,
            windows: window,
            more_older: start > 0,
        });
        found = imports
            .as_array()
            .into_iter()
            .flatten()
            .find(|item| {
                matches_import(
                    &item["import"],
                    root.system_id.as_ref(),
                    &spend_id,
                    export_vout,
                )
            })
            .cloned();
        if found.is_some() {
            import_source = Some(window_source);
        }
        if let Some(sender) = &updates {
            let mut progress = trail.clone();
            progress.searching = true;
            sender.send(progress);
        }
        if found.is_some() || start == 0 {
            break;
        }
        end = start - 1;
    }
    let progress = trail.search.clone().expect("searched at least one window");
    trail.notes.push(format!("Destination {} imports for {currency}: searched heights {}..{} in {} window(s). Source and destination heights are independent.", dest_config.chain.name, progress.first, progress.last, progress.windows));
    let Some(found) = found.as_ref() else {
        return stop(
            &mut trail,
            if progress.more_older {
                "Matching import not in these windows. Press s to scan older heights, [ for one older window, or g to jump to a known destination height. This is not proof of non-import."
            } else {
                "Matching import not found through destination genesis; check source availability, import currency, and chain membership."
            },
        );
    };
    if let Some(height) = found["importheight"].as_u64() {
        trail.import_heights.push((root.clone(), height));
    }
    let matching_hash = export["hashtransfers"]
        .as_str()
        .filter(|hash| valid_txid(hash))
        .is_some_and(|hash| found["import"]["hashtransfers"].as_str() == Some(hash));
    if !matching_hash {
        trail.notes.push("Import and export transfer hashes are missing or differ; importer-added arbitrage transfers can cause a difference. Inspect the ordered transfer arrays before attributing results.".into());
    }
    let batch_offset = input - export["firstinput"].as_u64().unwrap_or(0) as usize;
    let Some(batch) = found["transfers"].as_array() else {
        return stop(
            &mut trail,
            "Import has no ordered transfer array; individual attribution is unresolved",
        );
    };
    let export_count = export["numinputs"].as_u64().unwrap_or(0);
    if batch.len() < usize::try_from(export_count).unwrap_or(usize::MAX) {
        return stop(
            &mut trail,
            "Import transfer array is shorter than the matched export input range; individual attribution is unresolved",
        );
    }
    if !batch
        .get(batch_offset)
        .is_some_and(|item| transfer_matches(&transfer, item))
    {
        return stop(
            &mut trail,
            format!(
                "Import batch member {batch_offset} does not match the selected source transfer; individual attribution is unresolved"
            ),
        );
    }
    trail.notes.push(format!(
        "Selected source input is import batch member {batch_offset} of {}.",
        batch.len()
    ));
    let Some(import_id) = found["importtxid"].as_str().filter(|s| valid_txid(s)) else {
        return stop(
            &mut trail,
            "getimports match has no valid import transaction ID",
        );
    };
    let Some(import_vout) = found["importvout"]
        .as_u64()
        .and_then(|v| u32::try_from(v).ok())
    else {
        return stop(&mut trail, "getimports match has no valid output index");
    };
    let import_ref = OutpointRef {
        system_id: dest_config.chain.system_id.get().unwrap().clone(),
        txid: import_id.into(),
        vout: import_vout,
    };
    trail.entries.push(entry(
        &import_ref,
        "Import",
        format!(
            "height={}  source export={}:{}  hashtransfers={}  batch transfers={}",
            found["importheight"],
            spend_id,
            export_vout,
            found["import"]["hashtransfers"],
            found["transfers"].as_array().map_or(0, Vec::len)
        ),
    ));
    let import_tx = match dest_client.get_raw_transaction(import_id).await {
        Ok(v) => {
            if import_source.as_deref() != Some(&v.source_id) {
                import_source = None;
            }
            v.result
        }
        Err(error) => return stop(&mut trail, format!("Cannot load import outputs: {error}")),
    };
    if import_tx["txid"].as_str() != Some(import_id)
        || import_tx["confirmations"]
            .as_i64()
            .is_some_and(|count| count <= 0)
    {
        return stop(
            &mut trail,
            "Import transaction is mismatched, unconfirmed, or outside the active chain",
        );
    }
    let Some((region_start, region)) = generated_region(&import_tx, &found["import"], import_vout)
    else {
        return stop(
            &mut trail,
            "Cannot verify the import's generated-output region; individual attribution is unresolved",
        );
    };
    trail.legs[0].imported(import_ref.clone(), found);
    trail.legs[0]
        .execution
        .as_mut()
        .expect("recorded importing state")
        .source = import_source.clone();
    trail.notes.push(format!("Import declares {} generated output(s), beginning at vout {region_start}; this region can include final fee/reward outputs, while reserve-maintenance outputs after it are excluded.", region.len()));
    if issuer_instruction(&transfer, currency, root.system_id.as_ref()).is_some() {
        let verified = if matching_hash
            && destination == root.system_id.as_ref()
            && import_source.as_ref() == Some(&export_source)
        {
            let evidence = supply_burn::Evidence {
                source: &export_source,
                system: destination,
                issuer: currency,
                export_tx: &spent,
                export_vout,
                import_tx: &import_tx,
                import_vout,
                record: found,
                selected: batch_offset,
                region,
            };
            tokio::time::timeout(
                std::time::Duration::from_secs(12),
                evidence.verify(&dest_client.noninteractive()),
            )
            .await
            .ok()
            .flatten()
        } else {
            None
        };
        let Some(burn) = verified else {
            trail.legs[0].stage = Stage::BurnUnverified;
            return stop(
                &mut trail,
                "Issuer burn instruction found. Its supply reduction could not be verified from the ordered export and linked currency states.",
            );
        };
        let leg = &mut trail.legs[0];
        leg.stage = Stage::Burned;
        leg.supply_burn = Some(burn);
        let previous = leg
            .execution
            .as_ref()
            .and_then(|execution| execution.previous.as_ref());
        if let Some(previous) = previous {
            trail.entries.push(entry(
                previous,
                "Prior currency state",
                "Consumed notarization with the supply before the burn batch",
            ));
        }
        let mut latest = import_ref;
        latest.vout += 1;
        trail.entries.push(entry(
            &latest,
            "Burn execution state",
            "Accepted notarization records the verified supply reduction for the ordered batch",
        ));
        return trail;
    }
    if launch_funding::applicable(found) && transfer["flags"].as_u64() == Some(7) {
        if import_source.as_deref() == Some(export_source.as_str())
            && destination == root.system_id.as_ref()
            && let Some(verified) = (launch_funding::Evidence {
                source: &export_source,
                root: &import_ref,
                record: found,
                import_tx: &import_tx,
                export_tx: &spent,
                export_vout,
            })
            .verify(&dest_client)
            .await
            && let Some(receipt) = verified
                .receipts
                .get(batch_offset)
                .filter(|receipt| receipt.funding == root)
        {
            trail.legs[0].replayed(
                replay::ReplayProof {
                    offset: receipt.offset,
                    proceeds: None,
                    satoshis: None,
                    kind: replay::ReplayKind::Preconversion,
                },
                receipt.output.clone(),
                &region[receipt.offset],
            );
            trail.edges = lifecycle_edges(&root, &trail);
            verified.links(&mut trail, &import_ref);
            verified.add_receipt(&mut trail, receipt);
            if let Some(rewards) = &verified.rewards {
                rewards.append(&mut trail, &import_ref);
            }
            trail.notes.push("Verified all ordered launch receipts against their export inputs, primary identity scripts, and exact amounts. Customer totals exclude later generated outputs and reserve maintenance.".into());
            return trail;
        }
        trail.notes.push("The confirmed launch receipt could not be verified from the available definition, export inputs, and linked launch phases.".into());
    }
    if import_rewards::applicable(found) {
        if let Some(observing_source) = import_source.as_deref()
            && let Some(rewards) = (import_rewards::Evidence {
                source: &client,
                source_id: &export_source,
                destination: &dest_client,
                destination_id: observing_source,
                root: &import_ref,
                record: found,
                import_tx: &import_tx,
                export_tx: &spent,
            })
            .verify()
            .await
            && rewards.funding == root
        {
            trail.legs[0].replayed(
                replay::ReplayProof {
                    offset: 0,
                    proceeds: None,
                    satoshis: None,
                    kind: replay::ReplayKind::ReservePlain,
                },
                rewards.customer.clone(),
                &region[0],
            );
            trail.edges = lifecycle_edges(&root, &trail);
            rewards.append(&mut trail, &import_ref, false);
            return trail;
        }
        trail.notes.push("Submission rewards remain unattributed for this import. Their calculation requires the original export and definition, the producer block, and the active entropy header.".into());
    }
    // Definitions are immutable; request one only for the narrow prelaunch
    // branch and require the same enabled source that supplied the import.
    let prelaunch = found["importnotarization"]["prelaunch"].as_bool() == Some(true);
    let has_gateway = batch.iter().take(batch_offset + 1).any(|transfer| {
        transfer["flags"].as_u64() == Some(1) && transfer["destination"]["gateway"].is_string()
    });
    let currency_definition = if prelaunch || has_gateway {
        match dest_client
            .clone()
            .noninteractive()
            .get_currency(currency)
            .await
        {
            Ok(response) if Some(&response.source_id) == import_source.as_ref() => {
                Some(response.result)
            }
            _ => None,
        }
    } else {
        None
    };
    let mut gateway_definitions = std::collections::BTreeMap::new();
    if !prelaunch
        && currency_definition.is_some()
        && found["importnotarization"]["currencystate"]["flags"].as_u64() == Some(49)
    {
        let gateways: std::collections::BTreeSet<_> = batch
            .iter()
            .take(batch_offset + 1)
            .filter(|transfer| transfer["flags"].as_u64() == Some(1))
            .filter_map(|transfer| transfer["destination"]["gateway"].as_str())
            .collect();
        if gateways.len() <= 8 {
            for gateway in gateways {
                if let Ok(response) = dest_client
                    .clone()
                    .noninteractive()
                    .get_currency(gateway)
                    .await
                    && Some(&response.source_id) == import_source.as_ref()
                {
                    gateway_definitions.insert(gateway.to_owned(), response.result);
                }
            }
        }
    }
    let replayed_prefix = (matching_hash && export_count == batch.len() as u64)
        .then(|| {
            if prelaunch && let Some(definition) = &currency_definition {
                replay::replay_with_launch(
                    batch,
                    batch_offset,
                    &found["import"],
                    &found["importnotarization"],
                    destination,
                    region,
                    Some((definition, export)),
                )
            } else if !gateway_definitions.is_empty()
                && let Some(definition) = &currency_definition
            {
                replay::replay_with_evidence(
                    batch,
                    batch_offset,
                    &found["import"],
                    &found["importnotarization"],
                    destination,
                    region,
                    replay::Evidence::Gateways(&gateway_definitions, definition),
                )
            } else {
                replay::replay_supported_prefix(
                    batch,
                    batch_offset,
                    &found["import"],
                    &found["importnotarization"],
                    destination,
                    region,
                )
            }
        })
        .flatten();
    if let Some(proof) = replayed_prefix {
        trail.legs[0].replayed(
            proof,
            OutpointRef {
                system_id: import_ref.system_id.clone(),
                txid: import_id.into(),
                vout: u32::try_from(region_start + proof.offset)
                    .expect("generated region output index"),
            },
            &region[proof.offset],
        );
        if let Some(offset) = proof.proceeds {
            let output = OutpointRef {
                system_id: import_ref.system_id.clone(),
                txid: import_id.into(),
                vout: u32::try_from(region_start + offset).expect("generated proceeds index"),
            };
            trail.legs[0].launch_proceeds(output.clone(), &region[offset]);
            trail.entries.push(entry(&output, "Replayed launch proceeds",
                "Net preconversion reserves paid to the currency identity; separate from customer principal"));
        }
        trail.notes.push(if proof.kind == replay::ReplayKind::Preconversion {
            format!("Replayed batch members 0..{batch_offset}; preconversion principal and any preceding currency-identity proceeds match in order. This receipt does not establish eventual launch success.")
        } else if proof.kind == replay::ReplayKind::GatewayFallback {
            format!("Replayed batch members 0..{batch_offset}; the PBaaS next-leg budget is below its immutable import threshold. Local principal plus the unused forwarding budget, whole reserve vector and destination script match exactly; no continuation is inferred.")
        } else if proof.kind.is_forwarded() {
            format!("Replayed batch members 0..{}; forwarding principal, converted fee budget, destination and next system match exactly. This is another transfer instruction, not a final receipt.", batch_offset)
        } else if proof.kind.is_refund() {
            format!("Replayed batch members 0..{}; full original principal matched a local refund output and destination. No conversion receipt or conversion fee is inferred.", batch_offset)
        } else { match proof.satoshis {
            Some(satoshis) => format!("Replayed batch members 0..{} in order; native outputs matched exact satoshis and P2PKH script, and reserve outputs matched exact decoded amount vectors and destinations. Selected result is {satoshis} native satoshis.", batch_offset),
            None => format!("Replayed batch members 0..{} in order; reserve result matched the exact decoded currency amounts, destination, and zero native value.", batch_offset),
        }});
    }
    for (index, out) in import_tx["vout"]
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
    {
        let Some(vout) = u32::try_from(index).ok() else {
            continue;
        };
        if vout == import_vout {
            continue;
        }
        let script = &out["scriptPubKey"];
        let label = if let Some(system_import) = script.get("crosschainimport") {
            if system_import["exporttxid"].as_str() == Some(&spend_id)
                && system_import["sourcesystemid"].as_str() == Some(root.system_id.as_ref())
                && system_import["exporttxout"]
                    .as_u64()
                    .and_then(|n| u32::try_from(n).ok())
                    .is_some_and(|n| companion_exports.contains(&n))
            {
                Some("Companion system import")
            } else {
                None
            }
        } else if script.get("acceptednotarization").is_some()
            || script.get("earnednotarization").is_some()
        {
            Some("Import notarization")
        } else if script.get("notaryevidence").is_some() {
            Some("Import proof/evidence")
        } else if script["crosschainexport"]["issupplemental"].as_bool() == Some(true) {
            Some("Supplemental transfer evidence")
        } else {
            None
        };
        if let Some(label) = label {
            trail.entries.push(entry(
                &OutpointRef {
                    system_id: import_ref.system_id.clone(),
                    txid: import_id.into(),
                    vout,
                },
                label,
                "Batch/protocol context; not an additional recipient output",
            ));
        }
    }
    let destination_address = transfer["destination"]["address"].as_str();
    let mut count = 0;
    for (offset, out) in region.iter().enumerate() {
        if replayed_prefix.is_some_and(|proof| offset < proof.offset) {
            // The verified prefix assigned these outputs to earlier members,
            // even when they share the selected transfer's destination.
            continue;
        }
        let index = region_start + offset;
        let script = &out["scriptPubKey"];
        if script.get("crosschainimport").is_some()
            || script.get("crosschainexport").is_some()
            || script.get("acceptednotarization").is_some()
            || script.get("earnednotarization").is_some()
            || script.get("notaryevidence").is_some()
            || script.get("finalizeexport").is_some()
            || script.get("reservedeposit").is_some()
        {
            continue;
        }
        let direct = script["addresses"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|v| v.as_str() == destination_address);
        let nested =
            script["reservetransfer"]["destination"]["address"].as_str() == destination_address;
        if !direct && !nested && !replayed_prefix.is_some_and(|proof| proof.offset == offset) {
            continue;
        }
        let Some(vout) = u32::try_from(index).ok() else {
            continue;
        };
        count += 1;
        let result_ref = OutpointRef {
            system_id: import_ref.system_id.clone(),
            txid: import_id.into(),
            vout,
        };
        let settled = found["importnotarization"]["launchcomplete"].as_bool() == Some(true)
            && found["importnotarization"]["launchconfirmed"].as_bool() == Some(true);
        let direct = settled
            && export_count == 1
            && matching_hash
            && direct_result(&transfer, batch, region, offset);
        let ordered = export_count == batch.len() as u64
            && matching_hash
            && ordered_plain_result(
                batch,
                region,
                batch_offset,
                offset,
                &found["importnotarization"],
            );
        let replayed = replayed_prefix.is_some_and(|proof| proof.offset == offset);
        if replayed && replayed_prefix.is_some_and(|proof| proof.kind.is_forwarded()) {
            trail.next_leg = Some(Continuation {
                output: result_ref.clone(),
                transaction: import_tx.clone(),
            });
        }

        trail.entries.push(entry(
            &result_ref,
            if replayed && replayed_prefix.is_some_and(|proof| proof.kind.is_forwarded()) {
                "Replayed next-leg transfer"
            } else if replayed && replayed_prefix.is_some_and(|proof| proof.kind == replay::ReplayKind::ReservePlain) {
                "Replayed reserve result"
            } else if replayed && replayed_prefix.is_some_and(|proof| proof.kind == replay::ReplayKind::GatewayFallback) {
                "Replayed local gateway payout"
            } else if replayed && replayed_prefix.is_some_and(|proof| proof.kind == replay::ReplayKind::Preconversion) {
                "Replayed preconversion result"
            } else if replayed && replayed_prefix.is_some_and(|proof| proof.kind.is_refund()) {
                "Replayed refund result"
            } else if replayed && replayed_prefix.is_some_and(|proof| proof.kind.is_conversion()) {
                "Replayed conversion result"
            } else if replayed {
                "Replayed transfer result"
            } else if direct || ordered {
                "Direct transfer result"
            } else {
                "Destination-address candidate"
            },
            format!(
                "output value={} reserve={} address={}; {}",
                out["value"],
                script["reserve_balance"],
                if replayed {
                    script["reservetransfer"]["destination"]["address"]
                        .as_str()
                        .or_else(|| script["addresses"].as_array()?.first()?.as_str())
                        .unwrap_or("?")
                } else {
                    destination_address.unwrap_or("?")
                },
                if replayed {
                    "ordered prefix replay matched the result amount and destination"
                } else if direct {
                    "single direct transfer and sole generated output"
                } else if ordered {
                    "ordered plain-transfer result; every batch member has one matching generated output"
                } else {
                    "batch-level link only — inspect before attributing"
                }
            ),
        ));
    }
    if count == 0 {
        trail.notes.push("No import output matched the transfer destination address; inspect all import outputs manually.".into());
    } else {
        trail.notes.push(format!("{count} generated output(s) matched the transfer destination address or exact prefix replay. Only entries labeled replayed or direct are individually attributed; other matches remain candidates."));
    }
    if import_source.as_deref() == Some(export_source.as_str())
        && import_rewards::exporters::settled_applicable(found, root.system_id.as_ref())
        && let Some(verified) = (import_rewards::exporters::SettledEvidence {
            source: &export_source,
            root: &import_ref,
            record: found,
            import_tx: &import_tx,
            export_tx: &spent,
            export_vout,
        })
        .verify(&dest_client)
        .await
    {
        trail.edges = lifecycle_edges(&root, &trail);
        verified.rewards.append(&mut trail, &import_ref);
    }
    trail
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lifecycle_edges_preserve_candidate_resolution_and_skip_protocol_context() {
        let system = crate::world::SystemId::try_from("iSystem".to_string()).unwrap();
        let at = |txid: &str, vout| OutpointRef {
            system_id: system.clone(),
            txid: txid.into(),
            vout,
        };
        let root = at("source", 1);
        let trail = Trail {
            entries: vec![
                entry(&root, "Reserve transfer", "source"),
                entry(&at("export", 0), "Export", "input 2"),
                entry(&at("import", 0), "Import", "matched export"),
                entry(&at("import", 1), "Import proof/evidence", "context"),
                entry(
                    &at("import", 2),
                    "Destination-address candidate",
                    "same address",
                ),
            ],
            ..Trail::default()
        };
        let edges = lifecycle_edges(&root, &trail);
        assert_eq!(edges.len(), 3);
        assert_eq!(edges[0].relation, "spent-by-export");
        assert_eq!(edges[1].relation, "export-import");
        assert_eq!(edges[2].resolution, "candidate");
        assert_eq!(edges[2].target.vout, 2);
    }
    #[test]
    fn generated_region_excludes_protocol_and_maintenance_outputs() {
        let tx = json!({"vout": [
            {"scriptPubKey":{"crosschainimport":{}}},
            {"scriptPubKey":{"acceptednotarization":{}}},
            {"scriptPubKey":{"notaryevidence":{}}},
            {"scriptPubKey":{"crosschainexport":{"issupplemental":true}}},
            {"scriptPubKey":{"addresses":["recipient"],"spendableoutput":true}},
            {"scriptPubKey":{"reservedeposit":{}}}
        ]});
        let (start, region) = generated_region(&tx, &json!({"numoutputs":1}), 0).unwrap();
        assert_eq!(start, 4);
        assert_eq!(region.len(), 1);
        assert!(generated_region(&tx, &json!({"numoutputs":4}), 0).is_none());
    }
    #[test]
    fn direct_result_requires_sole_plain_transfer_and_output() {
        let transfer = json!({"flags":1,"destination":{"address":"recipient"}});
        let result = json!({"scriptPubKey":{"addresses":["recipient"],"spendableoutput":true}});
        assert!(direct_result(
            &transfer,
            std::slice::from_ref(&transfer),
            std::slice::from_ref(&result),
            0
        ));
        assert!(!direct_result(
            &transfer,
            &[transfer.clone(), transfer.clone()],
            std::slice::from_ref(&result),
            0
        ));
        assert!(!direct_result(
            &transfer,
            std::slice::from_ref(&transfer),
            &[result.clone(), result],
            0
        ));
        let conversion = json!({"flags":1027,"convert":true,"destination":{"address":"recipient"}});
        assert!(!direct_result(
            &conversion,
            std::slice::from_ref(&conversion),
            &[json!({"scriptPubKey":{"addresses":["recipient"],"spendableoutput":true}})],
            0
        ));
    }
    #[test]
    fn ordered_plain_batch_requires_one_matching_output_per_member() {
        let transfers = vec![
            json!({"flags":1,"destination":{"address":"alice"}}),
            json!({"flags":65,"crosssystem":true,"destination":{"address":"bob"}}),
        ];
        let outputs = vec![
            json!({"scriptPubKey":{"addresses":["alice"],"spendableoutput":true}}),
            json!({"scriptPubKey":{"reservetransfer":{"destination":{"address":"bob"}}}}),
        ];
        let settled = json!({"launchcomplete":true,"launchconfirmed":true});
        assert!(ordered_plain_result(&transfers, &outputs, 1, 1, &settled));
        assert!(!ordered_plain_result(&transfers, &outputs, 1, 0, &settled));
        assert!(!ordered_plain_result(
            &transfers,
            &outputs[..1],
            1,
            1,
            &settled
        ));
        let mut swapped = outputs.clone();
        swapped.swap(0, 1);
        assert!(!ordered_plain_result(&transfers, &swapped, 1, 1, &settled));
        let mut conversion = transfers.clone();
        conversion[0]["convert"] = json!(true);
        assert!(!ordered_plain_result(&conversion, &outputs, 1, 1, &settled));
    }
    #[test]
    fn live_pool_sell_binds_source_export_import_and_exact_receipt() {
        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/vrsc-pool-sell.json")).unwrap();
        let source = &fixture["source_transaction"];
        let exported = &fixture["export_transaction"];
        let record = &fixture["record"];
        let transfer = &source["vout"][0]["scriptPubKey"]["reservetransfer"];
        let input = source["vout"][0]["spentIndex"].as_u64().unwrap() as usize;
        assert_eq!(exported["vin"][input]["txid"], source["txid"]);
        assert_eq!(exported["vin"][input]["vout"], 0);
        let (export_vout, export) = matched_export(exported, input).unwrap();
        let system = record["import"]["sourcesystemid"].as_str().unwrap();
        assert!(matches_import(
            &record["import"],
            system,
            exported["txid"].as_str().unwrap(),
            export_vout
        ));
        assert_eq!(export["hashtransfers"], record["import"]["hashtransfers"]);
        assert!(transfer_matches(transfer, &record["transfers"][0]));
        assert_eq!(
            transfer_import_currency(transfer),
            record["import"]["importcurrencyid"].as_str()
        );
        let (start, outputs) = generated_region(
            &fixture["transaction"],
            &record["import"],
            record["importvout"].as_u64().unwrap() as u32,
        )
        .unwrap();
        let proof = replay::replay_supported_prefix(
            record["transfers"].as_array().unwrap(),
            0,
            &record["import"],
            &record["importnotarization"],
            system,
            outputs,
        )
        .unwrap();
        assert_eq!(start + proof.offset, 2);
        assert_eq!(proof.kind, replay::ReplayKind::FractionalToReserve);
        assert_eq!(proof.satoshis, Some(507_895_612_795));
        let at = |txid: &str, vout| OutpointRef {
            system_id: system.to_string().try_into().unwrap(),
            txid: txid.into(),
            vout,
        };
        let mut summary = LegSummary::new(at(source["txid"].as_str().unwrap(), 0), transfer);
        summary.imported(at(record["importtxid"].as_str().unwrap(), 0), record);
        summary.replayed(
            proof,
            at(record["importtxid"].as_str().unwrap(), 2),
            &outputs[0],
        );
        assert_eq!(summary.stage, Stage::Received);
        assert_eq!(summary.conversion_fee.as_ref().unwrap().satoshis, 8_186_472);
        assert_eq!(
            summary.transfer_fee_budget.as_ref().unwrap().satoshis,
            20_010
        );
        assert_eq!(
            summary.execution_rates()[0].output.satoshis,
            507_895_612_795
        );
    }

    #[test]
    fn pool_sell_import_currency_is_source_not_received_reserve() {
        for flags in [515, 579] {
            let transfer = json!({"flags":flags,"convert":true,"importtosource":true,
                "currencyvalues":{"pool":50},"destinationcurrencyid":"reserve"});
            assert_eq!(transfer_import_currency(&transfer), Some("pool"));
        }
    }

    #[test]
    fn import_currency_routes_buys_reserve_swaps_and_missing_source_conservatively() {
        assert_eq!(
            transfer_import_currency(&json!({"flags":3,
            "destinationcurrencyid":"pool"})),
            Some("pool")
        );
        assert_eq!(
            transfer_import_currency(&json!({"flags":1027,
            "destinationcurrencyid":"reserve","via":"pool"})),
            Some("pool")
        );
        assert_eq!(
            transfer_import_currency(&json!({"flags":515,
            "destinationcurrencyid":"reserve","currencyvalues":{}})),
            None
        );
        assert_eq!(
            transfer_import_currency(&json!({"flags":515,
            "destinationcurrencyid":"reserve","currencyvalues":{"one":1,"two":2}})),
            None
        );
    }

    #[test]
    fn export_membership_uses_input_range() {
        let tx =
            json!({"vout":[{"scriptPubKey":{"crosschainexport":{"firstinput":1,"numinputs":2}}}]});
        assert!(matched_export(&tx, 0).is_none());
        assert!(matched_export(&tx, 1).is_some());
        assert!(matched_export(&tx, 2).is_some());
        assert!(matched_export(&tx, 3).is_none());
    }
    #[test]
    fn import_match_requires_the_export_outpoint() {
        let value =
            json!({"sourcesystemid":"source","exporttxid":"a","exporttxout":1,"hashtransfers":"h"});
        assert!(matches_import(&value, "source", "a", 1));
        assert!(!matches_import(&value, "source", "a", 0));
        assert!(!matches_import(&value, "source", "b", 1));
        assert!(!matches_import(&value, "other", "a", 1));
    }
}
