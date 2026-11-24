//! Verified same-system fractional launch funding and receipt lineage.
use super::*;
use std::collections::{BTreeMap, HashMap};

const MAX_TRANSFERS: usize = 16;
const SATOSHI: i128 = 100_000_000;

pub(super) struct Receipt {
    pub funding: OutpointRef,
    pub output: OutpointRef,
    pub offset: usize,
    pub amount: i128,
    principal: i128,
    fee: i128,
    reserve: String,
}

pub(super) struct Funding {
    pub receipts: Vec<Receipt>,
    pub rewards: Option<import_rewards::exporters::NativePayouts>,
    currency: String,
    clear: OutpointRef,
    complete: OutpointRef,
    definition: OutpointRef,
}

pub(super) struct Evidence<'a> {
    pub source: &'a str,
    pub root: &'a OutpointRef,
    pub record: &'a Value,
    pub import_tx: &'a Value,
    pub export_tx: &'a Value,
    pub export_vout: u32,
}

pub(super) fn applicable(record: &Value) -> bool {
    let notarization = &record["importnotarization"];
    notarization["launchcleared"].as_bool() == Some(true)
        && notarization["launchconfirmed"].as_bool() == Some(true)
        && notarization["currencystate"]["flags"].as_u64() == Some(17)
        && record["import"]["flags"].as_u64() == Some(12)
}

async fn pinned_transaction(client: &RpcClient, source: &str, id: &str) -> Option<Value> {
    let value = client
        .call_on(source, "getrawtransaction", &[json!(id), json!(1)])
        .await
        .ok()?;
    confirmed(&value, id).then_some(value)
}

fn confirmed(tx: &Value, id: &str) -> bool {
    valid_txid(id)
        && tx["txid"].as_str() == Some(id)
        && tx["confirmations"].as_i64().is_some_and(|count| count > 0)
}

fn consumes(tx: &Value, id: &str, index: u64) -> bool {
    tx["vin"].as_array().is_some_and(|inputs| {
        inputs
            .iter()
            .filter(|input| {
                input["txid"].as_str() == Some(id) && input["vout"].as_u64() == Some(index)
            })
            .count()
            == 1
    })
}

fn shape<'a>(
    tx: &'a Value,
    record: &Value,
    currency: &str,
    system: &str,
    flag: u64,
) -> Option<&'a Value> {
    let txid = record["importtxid"].as_str()?;
    let import = &output(tx, 0)?["scriptPubKey"]["crosschainimport"];
    let state = &output(tx, 1)?["scriptPubKey"]["acceptednotarization"];
    let phase = &state["currencystate"];
    (confirmed(tx, txid)
        && record["importvout"].as_u64() == Some(0)
        && record["importheight"].as_u64().is_some()
        && tx["height"].as_u64() == record["importheight"].as_u64()
        && import == &record["import"]
        && import["version"].as_u64() == Some(1)
        && import["sourcesystemid"].as_str() == Some(system)
        && import["importcurrencyid"].as_str() == Some(currency)
        && state == &record["importnotarization"]
        && state["version"].as_u64() == Some(2)
        && state["currencyid"].as_str() == Some(currency)
        && state["launchcleared"].as_bool() == Some(true)
        && state["launchconfirmed"].as_bool() == Some(true)
        && state["launchcomplete"].as_bool().unwrap_or(false) == (flag == 49)
        && state["prelaunch"].as_bool() != Some(true)
        && state["refunding"].as_bool() != Some(true)
        && phase["version"].as_u64() == Some(1)
        && phase["flags"].as_u64() == Some(flag)
        && phase["currencyid"].as_str() == Some(currency))
    .then_some(phase)
}

fn outpoint(root: &OutpointRef, txid: &str, vout: u32) -> OutpointRef {
    OutpointRef {
        system_id: root.system_id.clone(),
        txid: txid.to_owned(),
        vout,
    }
}

impl<'a> Evidence<'a> {
    pub(super) async fn verify(&self, client: &RpcClient) -> Option<Funding> {
        let (mut funding, transactions) = tokio::time::timeout(
            std::time::Duration::from_secs(12),
            self.verify_inner(client),
        )
        .await
        .ok()
        .flatten()?;
        let inputs = funding
            .receipts
            .iter()
            .filter_map(|receipt| {
                Some((
                    transactions.get(receipt.funding.txid.as_str())?,
                    receipt.funding.vout,
                ))
            })
            .collect::<Vec<_>>();
        let evidence = import_rewards::exporters::BatchEvidence {
            source: self.source,
            root: self.root,
            import: &self.record["import"],
            notarization: &self.record["importnotarization"],
            import_tx: self.import_tx,
            export_tx: self.export_tx,
            export_vout: self.export_vout,
            batch: self.record["transfers"].as_array()?,
            funding: &inputs,
            reward_start: funding.receipts.last()?.output.vout.checked_add(1)?,
        };
        funding.rewards = evidence.verify(client, true).await;
        Some(funding)
    }

    async fn verify_inner(&self, client: &RpcClient) -> Option<(Funding, HashMap<&'a str, Value>)> {
        let system = self.root.system_id.as_ref();
        let import = &self.record["import"];
        let currency = import["importcurrencyid"].as_str()?;
        if self.root.vout != 0
            || self.import_tx["txid"].as_str() != Some(self.root.txid.as_str())
            || !applicable(self.record)
            || currency == system
        {
            return None;
        }
        let state = shape(self.import_tx, self.record, currency, system, 17)?;
        let (start, region) = generated_region(self.import_tx, import, 0)?;
        let batch = self.record["transfers"].as_array()?;
        if batch.is_empty() || batch.len() > MAX_TRANSFERS || start != 2 {
            return None;
        }
        let export_id = import["exporttxid"].as_str()?;
        let export = &output(self.export_tx, self.export_vout)?["scriptPubKey"]["crosschainexport"];
        if !confirmed(self.export_tx, export_id)
            || import["exporttxout"].as_u64() != Some(self.export_vout.into())
            || export["version"].as_u64() != Some(1)
            || !matches!(export["flags"].as_u64(), Some(1 | 5))
            || export["sourcesystemid"].as_str() != Some(system)
            || export["destinationsystemid"].as_str() != Some(system)
            || export["destinationcurrencyid"].as_str() != Some(currency)
            || export["numinputs"].as_u64() != Some(batch.len() as u64)
            || export["hashtransfers"] != import["hashtransfers"]
            || !export["hashtransfers"]
                .as_str()
                .is_some_and(|hash| valid_txid(hash) && !hash.bytes().all(|byte| byte == b'0'))
        {
            return None;
        }
        let definition = client
            .call_on(self.source, "getcurrency", &[json!(currency)])
            .await
            .ok()?;
        let definition_id = definition["definitiontxid"].as_str()?;
        let definition_vout = u32::try_from(definition["definitiontxout"].as_u64()?).ok()?;
        let definition_tx = pinned_transaction(client, self.source, definition_id).await?;
        let raw_definition =
            &output(&definition_tx, definition_vout)?["scriptPubKey"]["currencydefinition"];
        if !definition_matches(&definition, raw_definition, currency, system) {
            return None;
        }

        let height = self.record["importheight"].as_u64()?;
        if height < definition["startblock"].as_u64()? {
            return None;
        }
        let records = client
            .call_on(
                self.source,
                "getimports",
                &[json!(currency), json!(height), json!(height)],
            )
            .await
            .ok()?;
        let records = records.as_array()?;
        let clear_record = phase_candidate(records, 25)?;
        let complete_record = phase_candidate(records, 49)?;
        let clear_id = clear_record["importtxid"].as_str()?;
        let complete_id = complete_record["importtxid"].as_str()?;
        let clear = pinned_transaction(client, self.source, clear_id).await?;
        let complete = pinned_transaction(client, self.source, complete_id).await?;
        let clear_state = shape(&clear, clear_record, currency, system, 25)?;
        let complete_state = shape(&complete, complete_record, currency, system, 49)?;
        let block = self.import_tx["blockhash"]
            .as_str()
            .filter(|hash| valid_txid(hash))?;
        if clear["blockhash"].as_str() != Some(block)
            || complete["blockhash"].as_str() != Some(block)
            || !consumes(self.import_tx, clear_id, 0)
            || !consumes(self.import_tx, clear_id, 1)
            || !consumes(&complete, &self.root.txid, 0)
            || !consumes(&complete, &self.root.txid, 1)
            || clear_record["import"]["exporttxid"].as_str() != Some(definition_id)
            || clear_record["import"]["exporttxout"]
                .as_u64()
                .and_then(|index| u32::try_from(index).ok())
                .and_then(|index| output(&definition_tx, index))
                .is_none_or(|out| {
                    out["scriptPubKey"]["crosschainexport"]["destinationcurrencyid"].as_str()
                        != Some(currency)
                })
            || [clear_state, state, complete_state].iter().any(|phase| {
                phase["initialsupply"] != definition["initialsupply"]
                    || phase["supply"] != definition["initialsupply"]
                    || amount_sats(&phase["emitted"]) != Some(0)
            })
        {
            return None;
        }
        let first = usize::try_from(export["firstinput"].as_u64()?).ok()?;
        let inputs = self.export_tx["vin"]
            .as_array()?
            .get(first..first.checked_add(batch.len())?)?;
        let mut transactions = HashMap::new();
        for input in inputs {
            let id = input["txid"].as_str()?;
            if !transactions.contains_key(id) {
                transactions.insert(id, pinned_transaction(client, self.source, id).await?);
            }
        }
        let receipts = ordered_receipts(
            self.root,
            &definition,
            state,
            batch,
            inputs,
            &transactions,
            (start, region),
        )?;
        Some((
            Funding {
                receipts,
                rewards: None,
                currency: currency.to_owned(),
                clear: outpoint(self.root, clear_id, 0),
                complete: outpoint(self.root, complete_id, 0),
                definition: outpoint(self.root, definition_id, definition_vout),
            },
            transactions,
        ))
    }
}

fn phase_candidate(records: &[Value], flags: u64) -> Option<&Value> {
    let mut matches = records.iter().filter(|record| {
        record["importnotarization"]["currencystate"]["flags"].as_u64() == Some(flags)
            && record["import"]["numoutputs"].as_u64() == Some(0)
            && record["transfers"].as_array().is_some_and(Vec::is_empty)
    });
    let first = matches.next()?;
    matches.next().is_none().then_some(first)
}

fn definition_matches(definition: &Value, raw: &Value, currency: &str, system: &str) -> bool {
    definition["version"].as_u64() == Some(1)
        && matches!(definition["options"].as_u64(), Some(33 | 41))
        && definition["currencyid"].as_str() == Some(currency)
        && definition["systemid"].as_str() == Some(system)
        && definition["launchsystemid"].as_str() == Some(system)
        && [
            "version",
            "options",
            "currencyid",
            "systemid",
            "launchsystemid",
            "startblock",
            "currencies",
            "weights",
            "conversions",
            "initialsupply",
            "preallocations",
            "prelaunchcarveout",
            "prelaunchdiscount",
            "gatewayconverterissuance",
            "minpreconversion",
            "maxpreconversion",
        ]
        .into_iter()
        .all(|key| definition.get(key) == raw.get(key))
        && [
            "prelaunchcarveout",
            "prelaunchdiscount",
            "gatewayconverterissuance",
        ]
        .into_iter()
        .all(|key| {
            definition
                .get(key)
                .is_none_or(|value| amount_sats(value) == Some(0))
        })
        && ["preallocations", "minpreconversion", "maxpreconversion"]
            .into_iter()
            .all(|key| {
                definition
                    .get(key)
                    .is_none_or(|value| value.as_array().is_some_and(Vec::is_empty))
            })
}

fn varint(mut value: u64) -> Vec<u8> {
    let mut bytes = vec![(value & 0x7f) as u8];
    while value > 0x7f {
        value = (value >> 7) - 1;
        bytes.push((value & 0x7f) as u8 | 0x80);
    }
    bytes.reverse();
    bytes
}

fn identity_receipt_script(currency: &str, identity: &str, amount: i128) -> Option<String> {
    let (102, currency) = address_payload(currency)? else {
        return None;
    };
    let (102, identity) = address_payload(identity)? else {
        return None;
    };
    let amount = varint(u64::try_from(amount).ok()?);
    let mut script = vec![0x1b, 4, 3, 0, 1, 1, 0x15, 4];
    script.extend(&identity);
    script.extend([
        0xcc,
        u8::try_from(49 + amount.len()).ok()?,
        4,
        3,
        9,
        1,
        1,
        0x15,
        4,
    ]);
    script.extend(&identity);
    script.extend([u8::try_from(21 + amount.len()).ok()?, 1]);
    script.extend(currency);
    script.extend(amount);
    script.push(0x75);
    Some(script.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn ordered_receipts(
    root: &OutpointRef,
    definition: &Value,
    state: &Value,
    batch: &[Value],
    inputs: &[Value],
    transactions: &HashMap<&str, Value>,
    region: (usize, &[Value]),
) -> Option<Vec<Receipt>> {
    let (start, outputs) = region;
    let currency = definition["currencyid"].as_str()?;
    let system = root.system_id.as_ref();
    let mut receipts = Vec::with_capacity(batch.len());
    let mut total = 0i128;
    let mut fees = BTreeMap::<String, i128>::new();
    let mut seen = std::collections::HashSet::new();
    for (offset, (transfer, input)) in batch.iter().zip(inputs).enumerate() {
        let id = input["txid"].as_str()?;
        let vout = u32::try_from(input["vout"].as_u64()?).ok()?;
        let source = output(transactions.get(id)?, vout)?;
        let original = &source["scriptPubKey"]["reservetransfer"];
        let dest = &transfer["destination"];
        let values = transfer["currencyvalues"].as_object()?;
        let (reserve, principal) = values.iter().next()?;
        let principal = amount_sats(principal)?;
        let explicit = amount_sats(&transfer["fees"])?;
        if !seen.insert((id, vout))
            || transfer["version"].as_u64() != Some(1)
            || original["version"] != transfer["version"]
            || !transfer_matches(original, transfer)
            || transfer["flags"].as_u64() != Some(7)
            || transfer["destinationcurrencyid"].as_str() != Some(currency)
            || transfer["feecurrencyid"].as_str() != Some(system)
            || dest.get("gateway").is_some()
            || !identity_destination(dest)
            || values.len() != 1
            || reserve == currency
            || !definition["currencies"]
                .as_array()?
                .iter()
                .any(|item| item.as_str() == Some(reserve))
            || native(source)
                != Some(explicit.checked_add(if reserve == system { principal } else { 0 })?)
        {
            return None;
        }
        let fee = replay::conversion_fee(principal, 1)?;
        let price = amount_sats(&state["currencies"][reserve]["lastconversionprice"])?;
        let receipt = principal
            .checked_sub(fee)?
            .checked_mul(SATOSHI)?
            .checked_div(price)?;
        let observed = outputs.get(offset)?;
        let script = &observed["scriptPubKey"];
        let expected = identity_receipt_script(currency, dest["address"].as_str()?, receipt)?;
        if !(1..=i128::from(i64::MAX)).contains(&receipt)
            || native(observed) != Some(0)
            || script["hex"]
                .as_str()
                .is_none_or(|hex| !hex.eq_ignore_ascii_case(&expected))
            || script["reserveoutput"]["version"].as_u64() != Some(1)
            || script["reserveoutput"]["currencyvalues"].as_object()?.len() != 1
            || amount_sats(&script["reserveoutput"]["currencyvalues"][currency]) != Some(receipt)
        {
            return None;
        }
        let fee_total = fees.entry(reserve.clone()).or_default();
        *fee_total = fee_total.checked_add(fee)?;
        total = total.checked_add(receipt)?;
        receipts.push(Receipt {
            funding: outpoint(root, id, vout),
            output: outpoint(root, &root.txid, u32::try_from(start + offset).ok()?),
            offset,
            reserve: reserve.clone(),
            amount: receipt,
            principal,
            fee,
        });
    }
    (receipts.len() == batch.len()
        && total > 0
        && amount_sats(&state["preconvertedout"]) == Some(total)
        && amount_sats(&state["primarycurrencyout"]) == Some(total)
        && fees.iter().all(|(reserve, fee)| {
            amount_sats(&state["currencies"][reserve]["conversionfees"]) == Some(*fee)
        }))
    .then_some(receipts)
}

fn native(output: &Value) -> Option<i128> {
    match output.get("valueSat") {
        Some(sats) => sats.as_i64().filter(|value| *value >= 0).map(i128::from),
        None => amount_sats(&output["value"]),
    }
}

fn identity_destination(destination: &Value) -> bool {
    match destination["type"].as_u64() {
        Some(4) => destination.get("auxdests").is_none(),
        Some(68) => destination["auxdests"].as_array().is_some_and(|values| {
            !values.is_empty()
                && values.iter().all(|aux| {
                    aux["type"].as_u64() == Some(2)
                        && aux["address"]
                            .as_str()
                            .and_then(address_payload)
                            .is_some_and(|(prefix, _)| matches!(prefix, 0 | 60))
                })
        }),
        _ => false,
    }
}

impl Funding {
    pub(super) fn links(&self, trail: &mut Trail, root: &OutpointRef) {
        for (output, label, detail, relation) in [
            (
                &self.definition,
                "Launch definition",
                "Original currency definition committed on the launch system",
                "launch-definition",
            ),
            (
                &self.clear,
                "Launch clear",
                "Consumed import and state at the launch-clear phase",
                "launch-clear",
            ),
            (
                &self.complete,
                "Launch complete",
                "Later import consumes this receipt import and its state",
                "launch-complete",
            ),
        ] {
            trail.entries.push(entry(output, label, detail));
            trail.edges.push(LifecycleEdge {
                source: root.clone(),
                target: output.clone(),
                relation: relation.into(),
                resolution: "structural".into(),
                detail: detail.into(),
            });
        }
    }

    pub(super) fn add_receipt(&self, trail: &mut Trail, receipt: &Receipt) {
        let units = |sats| format!("{}.{:08}", sats / SATOSHI, sats % SATOSHI);
        let detail = format!(
            "Received {} {} from {} {}; conversion fee {} {}",
            units(receipt.amount),
            self.currency,
            units(receipt.principal),
            receipt.reserve,
            units(receipt.fee),
            receipt.reserve,
        );
        trail.entries.push(entry(
            &receipt.output,
            "Verified launch funding receipt",
            &detail,
        ));
        trail.edges.push(LifecycleEdge {
            source: receipt.funding.clone(),
            target: receipt.output.clone(),
            relation: "launch-funding-receipt".into(),
            resolution: "attributed".into(),
            detail,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::world::{Network, system_id};

    #[test]
    fn ordered_launch_receipts_require_exact_scripts_and_the_whole_batch() {
        let system = system_id("vDEX", Network::Mainnet).unwrap();
        let reserve = system.as_ref();
        let currency = system_id("VRSC", Network::Mainnet).unwrap().to_string();
        let root = OutpointRef {
            system_id: system.clone(),
            txid: "a".repeat(64),
            vout: 0,
        };
        let source = "b".repeat(64);
        let transfer = json!({
            "version":1,"flags":7,"destinationcurrencyid":currency,
            "currencyvalues":{(reserve):1},
            "feecurrencyid":reserve,"fees":0.0002,
            "destination":{"type":4,"address":currency}
        });
        let ordered = [transfer.clone(), transfer.clone()];
        let inputs = [
            json!({"txid":source,"vout":0}),
            json!({"txid":source,"vout":1}),
        ];
        let instruction = json!({"valueSat":100020000,"scriptPubKey":{"reservetransfer":transfer}});
        let original =
            HashMap::from([(source.as_str(), json!({"vout":[instruction, instruction]}))]);
        let definition = json!({"currencyid":currency,"currencies":[reserve]});
        let state = json!({
            "preconvertedout": 7.998, "primarycurrencyout": 7.998,
            "currencies":{(reserve):{"lastconversionprice":0.25,"conversionfees":0.0005}}
        });
        let receipt = json!({
            "valueSat":0,
            "scriptPubKey":{
                "hex":identity_receipt_script(&currency, &currency, 399900000).unwrap(),
                "reserveoutput":{"version":1,"currencyvalues":{(currency):3.999}}
            }
        });
        let check = |outputs: &[Value], inputs: &[Value]| {
            ordered_receipts(
                &root,
                &definition,
                &state,
                &ordered,
                inputs,
                &original,
                (2, outputs),
            )
        };
        let mut outputs = [receipt.clone(), receipt];
        let result = check(&outputs, &inputs).unwrap();
        assert_eq!(
            result
                .iter()
                .map(|receipt| receipt.output.vout)
                .collect::<Vec<_>>(),
            [2, 3]
        );
        assert_eq!(result[1].funding.vout, 1);
        assert!(check(&outputs, &[inputs[0].clone(), inputs[0].clone()]).is_none());
        outputs[0]["scriptPubKey"]["hex"] = json!("51");
        assert!(check(&outputs, &inputs).is_none());
    }
}
