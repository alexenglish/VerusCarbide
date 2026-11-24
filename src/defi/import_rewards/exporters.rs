//! Native exporter payouts and source-block producer attribution.
use super::*;
use ripemd::Ripemd160;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub(in crate::defi) struct Payee {
    pub(super) script: Vec<u8>,
    primary_slot: bool,
    first_auxiliary: bool,
}

pub(in crate::defi) struct Payees {
    pub(super) primary: Option<Payee>,
    pub(super) auxiliary: Option<Payee>,
}

pub(super) struct Producer {
    pub(super) output: OutpointRef,
    script: Vec<u8>,
}

pub(in crate::defi) fn native_reward(fee: i128, count: usize) -> Option<(i128, i128)> {
    let count = i128::try_from(count.clamp(1, 50)).ok()?;
    let split = fee.checked_mul(50_000_000 + 500_000 * (count - 1))? / UNIT;
    (fee >= 0).then_some((
        split,
        (split / 10).max(2 * STANDARD_TRANSFER_FEE).min(split),
    ))
}

fn address_for(kind: u8, data: &[u8], address: &str) -> Option<()> {
    let destination = Destination::from_json(address)?;
    match kind {
        1 if matches!(data, [2 | 3, ..]) && data.len() == 33 => {
            let key = Ripemd160::digest(Sha256::digest(data));
            (destination.kind == 2 && destination.key == key[..]).then_some(())
        }
        2 | 4 => (destination.kind == kind && destination.key == data).then_some(()),
        _ => None,
    }
}

fn script(kind: u8, data: &[u8]) -> Option<Vec<u8>> {
    match kind {
        1 if matches!(data, [2 | 3, ..]) && data.len() == 33 => {
            let mut result = vec![33];
            result.extend(data);
            result.push(0xac);
            Some(result)
        }
        2 | 4 => Some(
            Destination {
                kind,
                key: data.try_into().ok()?,
            }
            .script(),
        ),
        _ => None,
    }
}

pub(super) fn payees(export: &Value, bytes: &[u8]) -> Option<Payees> {
    let mut raw = Wire(cc_payload(bytes, 12)?);
    let version = u16::from_le_bytes(raw.integer()?);
    let flags = u16::from_le_bytes(raw.integer()?);
    if version != 1
        || !matches!(flags, 1 | 5 | 128)
        || u64::from(flags) != export["flags"].as_u64()?
        || raw.bytes(20)? != ids(export, "sourcesystemid")?
        || display_hash(raw.bytes(32)?) != export["hashtransfers"].as_str()?
        || raw.bytes(20)? != ids(export, "destinationsystemid")?
        || raw.bytes(20)? != ids(export, "destinationcurrencyid")?
    {
        return None;
    }
    let kind = raw.integer::<1>()?[0];
    if kind & !0x4f != 0 {
        return None;
    }
    let data = raw.vector()?;
    let mut primary = script(kind & 15, data).map(|script| Payee {
        script,
        primary_slot: true,
        first_auxiliary: false,
    });
    if primary.is_some() {
        address_for(kind & 15, data, export["rewardaddress"].as_str()?)?;
    }
    let mut auxiliary = None;
    if kind & 64 != 0 {
        let count = raw.count()?;
        if count > 16 {
            return None;
        }
        for index in 0..count {
            let mut encoded = Wire(raw.vector()?);
            let aux_kind = encoded.integer::<1>()?[0];
            let data = encoded.vector()?;
            if auxiliary.is_none()
                && encoded.0.is_empty()
                && let Some(script) = script(aux_kind, data)
            {
                auxiliary = Some(Payee {
                    script,
                    primary_slot: false,
                    first_auxiliary: index == 0,
                });
            }
        }
    }
    if u64::from(u32::from_le_bytes(raw.integer()?)) != export["firstinput"].as_u64()?
        || u64::from(u32::from_le_bytes(raw.integer()?)) != export["numinputs"].as_u64()?
        || raw.varint()? != export["sourceheightstart"].as_u64()?
        || raw.varint()? != export["sourceheightend"].as_u64()?
    {
        return None;
    }
    if kind & 15 == 0 && data.is_empty() {
        primary = auxiliary.take();
    }
    Some(Payees { primary, auxiliary })
}

pub(super) fn is_producer(payee: &Payee, producer: Option<&Producer>) -> bool {
    (payee.primary_slot || payee.first_auxiliary)
        && producer.is_some_and(|found| producer_script_matches(&found.script, &payee.script))
}

fn producer_script_matches(coinbase: &[u8], payout: &[u8]) -> bool {
    if coinbase == payout {
        return true;
    }
    if let [33, public_key @ .., 0xac] = coinbase
        && public_key.len() == 33
        && matches!(public_key.first(), Some(2 | 3))
    {
        let key = Ripemd160::digest(Sha256::digest(public_key)).into();
        return Destination { kind: 2, key }.script() == payout;
    }
    false
}

pub(super) fn role(primary: bool, payee: &Payee, producer: Option<&Producer>) -> &'static str {
    if is_producer(payee, producer) {
        if primary {
            "Primary exporter and source block producer reward"
        } else {
            "Source block producer reward"
        }
    } else if primary {
        "Primary exporter reward"
    } else {
        "Auxiliary exporter reward"
    }
}

fn choose_height<'a>(
    blocks: &BTreeMap<u64, (&str, &'a Value)>,
    entropy: u64,
) -> Option<(u64, &'a Value)> {
    let index = usize::try_from(entropy % u64::try_from(blocks.len()).ok()?.max(1)).ok()?;
    blocks
        .iter()
        .nth(index)
        .map(|(&height, &(_, tx))| (height, tx))
}

pub(super) async fn source_producer(
    client: &RpcClient,
    source: &str,
    system: &crate::world::SystemId,
    currency: &str,
    export: &Value,
    funding: &[&Value],
) -> Option<Producer> {
    let first = export["sourceheightstart"].as_u64()?;
    let last = export["sourceheightend"].as_u64()?;
    let mut blocks = BTreeMap::new();
    for &fund in funding {
        let height = fund["height"].as_u64()?;
        let hash = fund["blockhash"].as_str().filter(|hash| valid_txid(hash))?;
        if !(first..=last).contains(&height)
            || blocks.get(&height).is_some_and(|(seen, _)| *seen != hash)
        {
            return None;
        }
        blocks.insert(height, (hash, fund));
    }
    let entropy = if blocks.len() > 1 {
        let id = request(client, source, "getblockhash", &[json!(last)]).await?;
        let hash = id.as_str().filter(|hash| valid_txid(hash))?;
        let header = request(
            client,
            source,
            "getblockheader",
            &[json!(hash), json!(true)],
        )
        .await?;
        if header["hash"].as_str() != Some(hash)
            || header["height"].as_u64() != Some(last)
            || header["confirmations"].as_i64()? <= 0
            || blocks
                .get(&last)
                .is_some_and(|(expected, _)| *expected != hash)
        {
            return None;
        }
        entropy_component(&header, currency)?
    } else {
        0
    };
    let (height, selected) = choose_height(&blocks, entropy)?;
    let hash = selected["blockhash"].as_str()?;
    let block = request(client, source, "getblock", &[json!(hash), json!(1)]).await?;
    let txids = block["tx"].as_array()?;
    if block["hash"].as_str() != Some(hash)
        || block["height"].as_u64() != Some(height)
        || block["confirmations"].as_i64()? <= 0
        || funding
            .iter()
            .any(|fund| fund["height"].as_u64() == Some(height) && !txids.contains(&fund["txid"]))
    {
        return None;
    }
    let coinbase = tx(client, source, txids.first()?.as_str()?).await?;
    if !coinbase["vin"][0]["coinbase"].is_string() || coinbase["blockhash"].as_str() != Some(hash) {
        return None;
    }
    Some(Producer {
        output: point(system, &coinbase, 0)?,
        script: bound_bytes(&coinbase, 0)?,
    })
}

pub(in crate::defi) struct NativePayouts {
    converted_fee: i128,
    payouts: Vec<(OutpointRef, &'static str, i128)>,
    producer: Option<OutpointRef>,
}

impl NativePayouts {
    pub(in crate::defi) fn append(&self, trail: &mut Trail, root: &OutpointRef) {
        for (output, label, amount) in &self.payouts {
            let detail = format!(
                "{}.{:08} {} ({amount} base units) · exact recipient script",
                amount / UNIT,
                amount % UNIT,
                root.system_id
            );
            trail.entries.push(entry(output, *label, &detail));
            trail.edges.push(LifecycleEdge {
                source: root.clone(),
                target: output.clone(),
                relation: "import-submission-reward".into(),
                resolution: "attributed".into(),
                detail: format!("{label} · {detail}"),
            });
        }
        if let Some(producer) = &self.producer {
            trail.entries.push(entry(
                producer,
                "Source producer coinbase",
                "Selected source block's payout script matches the proven exporter recipient",
            ));
        }
        if self.producer.is_none() {
            trail.notes.push("The exporter payouts are verified. The source block producer remains unattributed.".into());
        }
        let paid: i128 = self.payouts.iter().map(|(_, _, sats)| sats).sum();
        trail.notes.push(format!("Native fees after conversion: {} base units. Remainder after these exporter payouts: {} base units, with no payout role established.",
            self.converted_fee,self.converted_fee-paid));
    }
}

pub(in crate::defi) struct BatchEvidence<'a> {
    pub source: &'a str,
    pub root: &'a OutpointRef,
    pub import: &'a Value,
    pub notarization: &'a Value,
    pub import_tx: &'a Value,
    pub export_tx: &'a Value,
    pub export_vout: u32,
    pub batch: &'a [Value],
    pub funding: &'a [(&'a Value, u32)],
    pub reward_start: u32,
}

fn instruction(body: &[u8], transfer: &Value, launch: bool) -> Option<()> {
    let mut raw = Wire(body);
    let (reserve, value) = transfer["currencyvalues"].as_object()?.iter().next()?;
    if raw.varint()? != 1
        || raw.bytes(20)? != raw_id(reserve)?
        || i128::from(raw.varint()?) != amount_sats(value)?
        || raw.varint()? != if launch { 7 } else { 1027 }
        || raw.bytes(20)? != ids(transfer, "feecurrencyid")?
        || i128::from(raw.varint()?) != amount_sats(&transfer["fees"])?
    {
        return None;
    }
    let destination = &transfer["destination"];
    let kind = raw.integer::<1>()?[0];
    if (if launch {
        !matches!(kind, 4 | 68)
    } else {
        !matches!(kind, 2 | 66)
    }) || u64::from(kind) != destination["type"].as_u64()?
        || Some(Destination::with_kind(kind & 15, &mut raw)?)
            != destination["address"]
                .as_str()
                .and_then(Destination::from_json)
    {
        return None;
    }
    if kind & 64 != 0 {
        let aux = destination["auxdests"].as_array()?;
        if raw.count()? != aux.len() {
            return None;
        }
        for entry in aux {
            let mut encoded = Wire(raw.vector()?);
            let decoded = Destination::decode(&mut encoded)?;
            if !encoded.0.is_empty()
                || decoded.kind != 2
                || decoded != Destination::from_json(entry["address"].as_str()?)?
            {
                return None;
            }
        }
    }
    if !launch && raw.bytes(20)? != ids(transfer, "via")? {
        return None;
    }
    (raw.bytes(20)? == ids(transfer, "destinationcurrencyid")? && raw.0.is_empty()).then_some(())
}

fn native_fee(state: &Value, system: &str, batch: &[Value], launch: bool) -> Option<i128> {
    let currencies = state["currencies"].as_object()?;
    let explicit = batch.iter().try_fold(0i128, |sum, transfer| {
        sum.checked_add(amount_sats(&transfer["fees"])?)
    })?;
    let mut expected = BTreeMap::<&str, i128>::new();
    for transfer in batch {
        let (id, amount) = transfer["currencyvalues"].as_object()?.iter().next()?;
        let conversion = replay::conversion_fee(amount_sats(amount)?, if launch { 1 } else { 2 })?;
        let sum = expected.entry(id).or_default();
        *sum = sum.checked_add(conversion)?;
    }
    let native_price = amount_sats(&currencies.get(system)?["viaconversionprice"])?;
    let mut fee = 0i128;
    for (id, row) in currencies {
        let conversion = amount_sats(&row["conversionfees"])?;
        let reported = amount_sats(&row["fees"])?;
        if conversion != expected.remove(id.as_str()).unwrap_or(0)
            || reported != conversion.checked_add(if id == system { explicit } else { 0 })?
        {
            return None;
        }
        let available = reported.checked_sub(conversion / 2)?;
        let native = if id == system || available == 0 {
            available
        } else {
            available
                .checked_mul(UNIT)?
                .checked_div(amount_sats(
                    &row[if launch {
                        "viaconversionprice"
                    } else {
                        "lastconversionprice"
                    }],
                )?)?
                .checked_mul(native_price)?
                .checked_div(UNIT)?
        };
        fee = fee.checked_add(native)?;
    }
    let reported_native = if launch {
        fee
    } else {
        fee.checked_sub(explicit)?
    };
    (expected.is_empty()
        && amount_sats(&state["primarycurrencyfees"]) == Some(0)
        && amount_sats(&state["primarycurrencyconversionfees"]) == Some(0)
        && amount_sats(&currencies.get(system)?["reserveout"]) == Some(reported_native))
    .then_some(fee)
}

impl BatchEvidence<'_> {
    pub(in crate::defi) async fn verify(
        &self,
        client: &RpcClient,
        launch: bool,
    ) -> Option<NativePayouts> {
        let system = self.root.system_id.as_ref();
        let currency = self.import["importcurrencyid"].as_str()?;
        checked_transaction(self.import_tx, &self.root.txid)?;
        checked_transaction(self.export_tx, self.import["exporttxid"].as_str()?)?;
        let export = &output(self.export_tx, self.export_vout)?["scriptPubKey"]["crosschainexport"];
        if export["sourcesystemid"].as_str() != Some(system)
            || export["destinationsystemid"].as_str() != Some(system)
            || export["destinationcurrencyid"].as_str() != Some(currency)
            || export["numinputs"].as_u64() != Some(self.batch.len() as u64)
            || export["hashtransfers"] != self.import["hashtransfers"]
        {
            return None;
        }
        let payees = payees(export, &bound_bytes(self.export_tx, self.export_vout)?)?;
        let mut serialized = Vec::new();
        for ((fund, vout), transfer) in self.funding.iter().zip(self.batch) {
            checked_transaction(fund, fund["txid"].as_str()?)?;
            let script = bound_bytes(fund, *vout)?;
            let body = cc_payload(&script, 8)?;
            instruction(body, transfer, launch)?;
            serialized.extend_from_slice(body);
        }
        if self.funding.len() != self.batch.len()
            || display_hash(&hash::verus_hash(&serialized)?)
                != self.import["hashtransfers"].as_str()?
        {
            return None;
        }
        state_bytes(
            &bound_bytes(self.import_tx, 1)?,
            self.notarization,
            if launch { 140 } else { 396 },
            if launch { 17 } else { 49 },
        )?;
        let native_fee = native_fee(
            &self.notarization["currencystate"],
            system,
            self.batch,
            launch,
        )?;
        let (_, reward) = native_reward(native_fee, self.batch.len())?;
        let primary = payees.primary.as_ref()?;
        let auxiliary = payees
            .auxiliary
            .as_ref()
            .filter(|_| reward > STANDARD_TRANSFER_FEE);
        let payouts = if let Some(auxiliary) = auxiliary {
            vec![
                (false, auxiliary, reward / 2),
                (true, primary, reward - reward / 2),
            ]
        } else {
            vec![(true, primary, reward)]
        };
        let (begin, region) = generated_region(self.import_tx, self.import, self.root.vout)?;
        if usize::try_from(self.reward_start)
            .ok()?
            .checked_add(payouts.len())?
            != begin + region.len()
        {
            return None;
        }
        let mut verified = Vec::new();
        for (offset, (primary, payee, amount)) in payouts.into_iter().enumerate() {
            let vout = self.reward_start.checked_add(u32::try_from(offset).ok()?)?;
            if native(output(self.import_tx, vout)?) != Some(amount)
                || bound_bytes(self.import_tx, vout)? != payee.script
            {
                return None;
            }
            verified.push((
                point(&self.root.system_id, self.import_tx, vout)?,
                primary,
                payee,
                amount,
            ));
        }
        let funding: Vec<_> = self.funding.iter().map(|(tx, _)| *tx).collect();
        let producer = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            source_producer(
                client,
                self.source,
                &self.root.system_id,
                currency,
                export,
                &funding,
            ),
        )
        .await
        .ok()
        .flatten();
        let matched = producer.as_ref().filter(|found| {
            verified
                .iter()
                .any(|(_, _, payee, _)| is_producer(payee, Some(found)))
        });
        Some(NativePayouts {
            converted_fee: native_fee,
            payouts: verified
                .into_iter()
                .map(|(output, primary, payee, amount)| {
                    (output, role(primary, payee, matched), amount)
                })
                .collect(),
            producer: matched.map(|value| value.output.clone()),
        })
    }
}

pub(in crate::defi) fn settled_applicable(record: &Value, system: &str) -> bool {
    record["import"]["sourcesystemid"].as_str() == Some(system)
        && record["import"]["flags"].as_u64() == Some(12)
        && record["importnotarization"]["currencystate"]["flags"].as_u64() == Some(49)
        && record["transfers"].as_array().is_some_and(|items| {
            (1..=16).contains(&items.len())
                && items.iter().all(|t| t["flags"].as_u64() == Some(1027))
        })
}

pub(in crate::defi) struct SettledEvidence<'a> {
    pub source: &'a str,
    pub root: &'a OutpointRef,
    pub record: &'a Value,
    pub import_tx: &'a Value,
    pub export_tx: &'a Value,
    pub export_vout: u32,
}

pub(in crate::defi) struct Settled {
    pub rewards: NativePayouts,
    pub funding: Vec<(OutpointRef, OutpointRef)>,
}

impl SettledEvidence<'_> {
    pub(in crate::defi) async fn verify(&self, client: &RpcClient) -> Option<Settled> {
        tokio::time::timeout(
            std::time::Duration::from_secs(12),
            self.verify_inner(client),
        )
        .await
        .ok()
        .flatten()
    }

    async fn verify_inner(&self, client: &RpcClient) -> Option<Settled> {
        let system = self.root.system_id.as_ref();
        let import = &self.record["import"];
        let notarization = &self.record["importnotarization"];
        let batch = self.record["transfers"].as_array()?;
        let export = &output(self.export_tx, self.export_vout)?["scriptPubKey"]["crosschainexport"];
        if !settled_applicable(self.record, system)
            || self.root.vout != 0
            || self.record["importtxid"].as_str() != Some(self.root.txid.as_str())
            || self.record["importvout"].as_u64() != Some(0)
            || self.record["importheight"].as_u64()? != self.import_tx["height"].as_u64()?
            || import != &output(self.import_tx, 0)?["scriptPubKey"]["crosschainimport"]
            || notarization != &output(self.import_tx, 1)?["scriptPubKey"]["acceptednotarization"]
            || import["exporttxout"].as_u64() != Some(self.export_vout.into())
            || import["exporttxid"] != self.export_tx["txid"]
            || export["flags"].as_u64() != Some(128)
        {
            return None;
        }
        let (start, region) = generated_region(self.import_tx, import, 0)?;
        if start != 2 {
            return None;
        }
        let first = usize::try_from(export["firstinput"].as_u64()?).ok()?;
        let inputs = self.export_tx["vin"]
            .as_array()?
            .get(first..first.checked_add(batch.len())?)?;
        let mut sources = std::collections::HashMap::new();
        let mut seen = std::collections::HashSet::new();
        let mut links = Vec::new();
        for (index, (input, transfer)) in inputs.iter().zip(batch).enumerate() {
            let id = input["txid"].as_str()?;
            let n = u32::try_from(input["vout"].as_u64()?).ok()?;
            if !seen.insert((id, n)) {
                return None;
            }
            if let std::collections::hash_map::Entry::Vacant(slot) = sources.entry(id) {
                slot.insert(tx(client, self.source, id).await?);
            }
            let fund = sources.get(id)?;
            let prevout = output(fund, n)?;
            let reserve = transfer["currencyvalues"].as_object()?.keys().next()?;
            let target = transfer["destinationcurrencyid"].as_str()?;
            let currency = import["importcurrencyid"].as_str()?;
            if !transfer_matches(&prevout["scriptPubKey"]["reservetransfer"], transfer)
                || transfer["currencyvalues"].as_object()?.len() != 1
                || reserve == system
                || reserve == currency
                || [system, currency].contains(&target)
                || transfer["feecurrencyid"].as_str() != Some(system)
                || transfer["via"].as_str() != Some(currency)
                || native(prevout) != amount_sats(&transfer["fees"])
            {
                return None;
            }
            let found = replay::replay_supported_prefix(
                batch,
                index,
                import,
                notarization,
                system,
                region,
            )?;
            if found.offset != index || !found.kind.is_conversion() {
                return None;
            }
            let observed = &region[index];
            let amount =
                amount_sats(&observed["scriptPubKey"]["reserveoutput"]["currencyvalues"][target])?;
            let recipient = Destination::from_json(transfer["destination"]["address"].as_str()?)?;
            let out = u32::try_from(start + index).ok()?;
            if recipient.kind != 2
                || bound_bytes(self.import_tx, out)?
                    != pkh_receipt(recipient.key, raw_id(target)?, u64::try_from(amount).ok()?)
            {
                return None;
            }
            links.push((
                point(&self.root.system_id, fund, n)?,
                point(&self.root.system_id, self.import_tx, out)?,
            ));
        }
        let funding = inputs
            .iter()
            .map(|input| {
                Some((
                    sources.get(input["txid"].as_str()?)?,
                    u32::try_from(input["vout"].as_u64()?).ok()?,
                ))
            })
            .collect::<Option<Vec<_>>>()?;
        let rewards = BatchEvidence {
            source: self.source,
            root: self.root,
            import,
            notarization,
            import_tx: self.import_tx,
            export_tx: self.export_tx,
            export_vout: self.export_vout,
            batch,
            funding: &funding,
            reward_start: u32::try_from(start + batch.len()).ok()?,
        }
        .verify(client, false)
        .await?;
        Some(Settled {
            rewards,
            funding: links,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn caps_processed_transfers_and_selects_ascending_distinct_heights() {
        assert_eq!(native_reward(13_332_504, 4), Some((6_866_239, 686_623)));
        assert_eq!(native_reward(2_000_000, 50), native_reward(2_000_000, 51));
        assert_eq!(native_reward(1_020_000, 51), Some((759_900, 75_990)));
        let source = Value::Null;
        let blocks = BTreeMap::from([
            (73, ("a", &source)),
            (14, ("b", &source)),
            (29, ("c", &source)),
        ]);
        assert_eq!(choose_height(&blocks, 4).unwrap().0, 29);
        assert_eq!(choose_height(&blocks, 6).unwrap().0, 14);
    }
}
