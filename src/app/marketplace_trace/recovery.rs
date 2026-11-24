//! Release evidence for spends that do not match signed marketplace terms.
use super::super::{
    ObjectStore, ShieldedAccounting, case_insensitive_field, decode_hex_bytes, transaction_output,
};
use super::{currency_output, native_amount, no_private_inputs, orphan, reserves};
use crate::app::resolved_prevout;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
pub(in crate::app) struct Recovery {
    pub inputs: BTreeSet<usize>,
    pub outputs: BTreeMap<usize, Vec<usize>>,
    pub shielded_inputs: Vec<usize>,
    pub reserve_outputs: BTreeMap<usize, Vec<usize>>,
    pub reserve_inputs: BTreeSet<usize>,
}

fn sum_map(total: &mut BTreeMap<String, i128>, values: BTreeMap<String, i128>) -> Option<()> {
    for (currency, amount) in values {
        let sum = total.entry(currency).or_default();
        *sum = sum.checked_add(amount)?;
    }
    Some(())
}

fn identity_returns(offers: &[(usize, &Value)], outputs: &[Value]) -> Recovery {
    let mut recovery = Recovery::default();
    for &(index, locked) in offers {
        let identity = &locked["scriptPubKey"]["identityprimary"];
        if !identity.is_object() || identity["identityaddress"].as_str().is_none() {
            continue;
        }
        let mut matches = outputs
            .iter()
            .enumerate()
            .filter(|(_, output)| &output["scriptPubKey"]["identityprimary"] == identity);
        if let Some((output, _)) = matches.next()
            && matches.next().is_none()
        {
            recovery.outputs.entry(output).or_default().push(index);
        }
    }
    recovery.outputs.retain(|_, inputs| inputs.len() == 1);
    recovery
        .inputs
        .extend(recovery.outputs.values().flatten().copied());
    recovery
}

fn release_bundle(
    offers: &[(usize, &Value)],
    outputs: &[Value],
    accounting: ShieldedAccounting,
    recovery: &mut Recovery,
) -> Option<()> {
    let mut locked_reserves = BTreeMap::new();
    let mut native_in = 0i128;
    let mut currency_inputs = Vec::new();
    let mut native_sources = BTreeSet::new();
    let mut reserve_sources: BTreeMap<String, BTreeSet<usize>> = BTreeMap::new();
    for &(index, locked) in offers {
        let native = u64::try_from(native_amount(locked)?).ok()?;
        native_in = native_in.checked_add(i128::from(native))?;
        if native > 0u64 {
            native_sources.insert(index);
        }
        if locked["scriptPubKey"]["commitmenthash"].is_object() {
            currency_inputs.push(index);
            let values = reserves(&locked["scriptPubKey"], "commitmenthash")?;
            for currency in values.keys() {
                reserve_sources
                    .entry(currency.clone())
                    .or_default()
                    .insert(index);
            }
            sum_map(&mut locked_reserves, values)?;
        } else if !recovery.inputs.contains(&index) {
            return None;
        }
    }
    if currency_inputs.is_empty() {
        return None;
    }

    let shielded_native = accounting
        .sapling_value_balance_zat
        .checked_neg()
        .filter(|value| *value >= 0)?;
    let mut native_out = shielded_native;
    let mut returned_reserves = BTreeMap::new();
    let mut returns = Vec::new();
    for (index, output) in outputs.iter().enumerate() {
        let native = u64::try_from(native_amount(output)?).ok()?;
        native_out = native_out.checked_add(i128::from(native))?;
        if recovery.outputs.contains_key(&index) {
            continue;
        }
        let script = &output["scriptPubKey"];
        if script["type"].as_str() == Some("nulldata") && native == 0 {
            continue;
        }
        let reserves = currency_output(script)?;
        let mut sources = BTreeSet::new();
        if native > 0 {
            sources.extend(native_sources.iter().copied());
        }
        for currency in reserves.keys() {
            sources.extend(reserve_sources.get(currency)?.iter().copied());
        }
        if !sources.is_empty() {
            returns.push((index, sources.into_iter().collect()));
        }
        sum_map(&mut returned_reserves, reserves)?;
    }
    if returned_reserves != locked_reserves
        || native_out > native_in
        || (returns.is_empty() && shielded_native == 0)
    {
        return None;
    }

    // Never assign shared native funds or identical reserve units to
    // individual inputs. Disjoint reserve currencies can be linked exactly.
    recovery.outputs.extend(returns);
    if shielded_native > 0 {
        recovery.shielded_inputs = native_sources.into_iter().collect();
    }
    recovery.inputs.extend(currency_inputs);
    Some(())
}

fn release_reserves_with_other_inputs(
    offers: &[(usize, &Value)],
    inputs: &[Value],
    outputs: &[Value],
    store: &ObjectStore,
    recovery: &mut Recovery,
) -> Option<()> {
    let mut locked = BTreeMap::new();
    let mut asset_sources: BTreeMap<String, BTreeSet<usize>> = BTreeMap::new();
    for &(index, output) in offers {
        let assets = reserves(&output["scriptPubKey"], "commitmenthash")?;
        for currency in assets.keys() {
            asset_sources
                .entry(currency.clone())
                .or_default()
                .insert(index);
        }
        sum_map(&mut locked, assets)?;
    }
    if asset_sources.is_empty() {
        return None;
    }
    for (index, input) in inputs.iter().enumerate() {
        if offers.iter().any(|(offer_index, _)| *offer_index == index) {
            continue;
        }
        let prevout = resolved_prevout(input, store)?;
        let script = &prevout["scriptPubKey"];
        if !script["identityprimary"].is_object() {
            for currency in currency_output(script)?.keys() {
                asset_sources.remove(currency);
            }
        }
    }
    if asset_sources.is_empty() {
        return None;
    }
    let mut returned = BTreeMap::new();
    let mut rows = Vec::new();
    for (index, output) in outputs.iter().enumerate() {
        let script = &output["scriptPubKey"];
        if script["identityprimary"].is_object() || script["type"].as_str() == Some("nulldata") {
            continue;
        }
        let assets = currency_output(script)?;
        rows.push((index, assets.keys().cloned().collect::<Vec<_>>()));
        sum_map(&mut returned, assets)?;
    }
    for (index, assets) in rows {
        let sources: BTreeSet<_> = assets
            .iter()
            .filter(|currency| locked.get(*currency) == returned.get(*currency))
            .filter_map(|currency| asset_sources.get(currency))
            .flatten()
            .copied()
            .collect();
        if !sources.is_empty() {
            recovery.reserve_inputs.extend(sources.iter().copied());
            recovery
                .reserve_outputs
                .insert(index, sources.into_iter().collect());
        }
    }
    Some(())
}

pub(in crate::app) fn recover(raw: &Value, store: &ObjectStore) -> Option<Recovery> {
    if orphan(raw) {
        return None;
    }
    let inputs = raw["vin"].as_array()?;
    let outputs = raw["vout"].as_array()?;
    let offers: Vec<_> = inputs
        .iter()
        .enumerate()
        .filter_map(|(index, input)| {
            let funding = input["txid"].as_str()?;
            if input["vout"].as_u64() != Some(0) {
                return None;
            }
            let (_, offer) = store.marketplace_contract(funding)?;
            // The funding owner's signature differs from the posted settlement
            // signature. Missing bytes cannot establish this distinction.
            let signature = decode_hex_bytes(input.pointer("/scriptSig/hex")?.as_str()?)?;
            if signature.is_empty()
                || signature == offer.signature
                || outputs
                    .get(index)
                    .is_some_and(|output| offer.matches_transparent_payment(output))
            {
                return None;
            }
            let locked = transaction_output(&store.transaction(funding)?.raw_json, 0)?;
            Some((index, locked))
        })
        .collect();
    if offers.is_empty() {
        return None;
    }
    let mut recovery = identity_returns(&offers, outputs);
    if no_private_inputs(raw) {
        if offers.len() == inputs.len() {
            let accounting = ShieldedAccounting::from_raw(raw);
            let shielded_outputs = case_insensitive_field(raw, "vshieldedoutput")
                .and_then(Value::as_array)
                .is_some_and(|items| !items.is_empty());
            if accounting.complete && shielded_outputs == (accounting.sapling_value_balance_zat < 0)
            {
                release_bundle(&offers, outputs, accounting, &mut recovery);
            }
        } else {
            release_reserves_with_other_inputs(&offers, inputs, outputs, store, &mut recovery);
        }
    }
    (!recovery.inputs.is_empty() || !recovery.reserve_inputs.is_empty()).then_some(recovery)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::{marketplace_trace::test_publication, transaction_record};
    use serde_json::json;

    #[test]
    fn batch_closes_bind_disjoint_reserves_and_leave_native_shared() {
        let mut store = ObjectStore::default();
        for (digit, currency) in [(1, "synthetic_a"), (2, "synthetic_b")] {
            let funding = digit.to_string().repeat(64);
            let proof = (digit + 2).to_string().repeat(64);
            store.insert_transaction(transaction_record(
                &funding,
                json!({
                    "txid": funding, "vout":[{"n":0,"valueSat":80000,"scriptPubKey":{
                        "commitmenthash":{"currencyvalues":{(currency):1}}
                    }}]
                }),
            ));
            store.insert_transaction(transaction_record(&proof, json!({
                "txid": proof, "vin":[{"txid":funding,"vout":1}], "vout":[{"n":0,"scriptPubKey":{
                    "hex":test_publication(digit, 5000)
                }}]
            })));
        }
        let mut batch = json!({
            "vin":[{"txid":"1".repeat(64),"vout":0,"scriptSig":{"hex":"53"}},
                   {"txid":"2".repeat(64),"vout":0,"scriptSig":{"hex":"53"}}],
            "vout":[{"n":0,"valueSat":0,"scriptPubKey":{"reserveoutput":{"currencyvalues":{"synthetic_a":1}}}},
                    {"n":1,"valueSat":0,"scriptPubKey":{"reserveoutput":{"currencyvalues":{"synthetic_b":1}}}},
                    {"n":2,"valueSat":140000,"scriptPubKey":{"type":"pubkeyhash"}}]
        });
        let recovery = recover(&batch, &store).unwrap();
        assert_eq!(recovery.inputs, BTreeSet::from([0, 1]));
        assert_eq!(
            recovery.outputs,
            BTreeMap::from([(0, vec![0]), (1, vec![1]), (2, vec![0, 1])])
        );

        let fee_funding = "5".repeat(64);
        store.insert_transaction(transaction_record(&fee_funding, json!({
            "txid":fee_funding,"vout":[{"n":0,"valueSat":40000,"scriptPubKey":{"type":"pubkeyhash"}}]
        })));
        let mut externally_funded = batch.clone();
        externally_funded["vin"]
            .as_array_mut()
            .unwrap()
            .push(json!({"txid":fee_funding,"vout":0}));
        let recovery = recover(&externally_funded, &store).unwrap();
        assert!(recovery.inputs.is_empty());
        assert_eq!(recovery.reserve_inputs, BTreeSet::from([0, 1]));
        assert_eq!(
            recovery.reserve_outputs,
            BTreeMap::from([(0, vec![0]), (1, vec![1])])
        );
        let mut top_up = store.transaction(&fee_funding).unwrap().raw_json.clone();
        top_up["vout"][0]["scriptPubKey"]["reserveoutput"] =
            json!({"currencyvalues":{"synthetic_a":1}});
        store.replace_transaction(transaction_record(&fee_funding, top_up));
        let recovery = recover(&externally_funded, &store).unwrap();
        assert_eq!(recovery.reserve_outputs, BTreeMap::from([(1, vec![1])]));

        batch["vin"][1]["scriptSig"]["hex"] = json!("51");
        assert!(recover(&batch, &store).is_none());
    }

    #[test]
    fn custom_spend_can_prove_identity_restoration_with_other_inputs() {
        let mut store = ObjectStore::default();
        let funding = "1".repeat(64);
        let posting = "2".repeat(64);
        let identity = json!({"identityaddress":"synthetic_identity","flags":0,"version":3});
        store.insert_transaction(transaction_record(&funding, json!({
            "txid":funding,"vout":[{"n":0,"valueSat":10000,"scriptPubKey":{"identityprimary":identity}}]
        })));
        store.insert_transaction(transaction_record(
            &posting,
            json!({
                "txid":posting,"vin":[{"txid":funding,"vout":1}],
                "vout":[{"n":0,"scriptPubKey":{"hex":test_publication(1, 5000)}}]
            }),
        ));
        let mut custom = json!({
            "vin":[{"txid":"3".repeat(64),"vout":1},
                   {"txid":funding,"vout":0,"scriptSig":{"hex":"53"}}],
            "vout":[{"n":0,"valueSat":10000,"scriptPubKey":{"identityprimary":identity}},
                    {"n":1,"valueSat":5000,"scriptPubKey":{"type":"pubkeyhash"}}]
        });
        let recovery = recover(&custom, &store).unwrap();
        assert_eq!(recovery.inputs, BTreeSet::from([1]));
        assert_eq!(recovery.outputs, BTreeMap::from([(0, vec![1])]));
        custom["vout"][0]["scriptPubKey"]["identityprimary"]["flags"] = json!(1);
        assert!(recover(&custom, &store).is_none());
    }
}
