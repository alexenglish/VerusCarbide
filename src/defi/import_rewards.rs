//! Exact attribution for an ordinary, single-transfer PBaaS import batch.
use super::*;
use wire::{Destination, Wire, cc_payload, checked_transaction, display_hash, hex, native, raw_id};

pub(super) mod exporters;
mod hash;
mod wire;

const UNIT: i128 = 100_000_000;
const STANDARD_TRANSFER_FEE: i128 = 20_000;
const ENTROPY_KEY: [u8; 20] = [
    0x43, 0xdb, 0x1c, 0x72, 0x87, 0x6f, 0xfe, 0xec, 0x25, 0x14, 0x81, 0x2b, 0xa6, 0x0f, 0x40, 0xf9,
    0x9f, 0xaf, 0xd1, 0xfa,
];

pub(super) struct Evidence<'a> {
    pub source: &'a RpcClient,
    pub source_id: &'a str,
    pub destination: &'a RpcClient,
    pub destination_id: &'a str,
    pub root: &'a OutpointRef,
    pub record: &'a Value,
    pub import_tx: &'a Value,
    pub export_tx: &'a Value,
}

pub(super) struct Rewards {
    pub(super) funding: OutpointRef,
    pub(super) customer: OutpointRef,
    principal: i128,
    payments: [(OutpointRef, &'static str, i128); 4],
    producer: Option<OutpointRef>,
    definition: OutpointRef,
    converted_fee: i128,
    residual: i128,
}

pub(super) fn applicable(record: &Value) -> bool {
    let import = &record["import"];
    let state = &record["importnotarization"]["currencystate"];
    import["flags"].as_u64() == Some(4)
        && state["flags"].as_u64() == Some(49)
        && record["transfers"]
            .as_array()
            .is_some_and(|batch| batch.len() == 1 && batch[0]["flags"].as_u64() == Some(65))
}

fn payout_amounts(native_fee: i128) -> Option<[i128; 4]> {
    let (export_fee, exporter) = exporters::native_reward(native_fee, 1)?;
    let remaining = export_fee.checked_sub(exporter)?;
    let notary = (remaining / 2).min(exporter);
    (remaining > exporter && notary >= STANDARD_TRANSFER_FEE).then_some([
        notary,
        notary,
        exporter / 2,
        exporter - exporter / 2,
    ])
}

fn ids(value: &Value, name: &str) -> Option<[u8; 20]> {
    raw_id(value[name].as_str()?)
}

fn point(system: &crate::world::SystemId, tx: &Value, vout: u32) -> Option<OutpointRef> {
    Some(OutpointRef {
        system_id: system.clone(),
        txid: tx["txid"].as_str()?.into(),
        vout,
    })
}

fn bound_bytes(tx: &Value, n: u32) -> Option<Vec<u8>> {
    hex(output(tx, n)?.pointer("/scriptPubKey/hex")?.as_str()?)
}

fn singleton_map(value: &Value, currency: &str, amount: i128) -> bool {
    value.as_object().is_some_and(|entries| entries.len() == 1)
        && amount_sats(&value[currency]) == Some(amount)
}

fn pkh_receipt(key: [u8; 20], currency: [u8; 20], amount: u64) -> Vec<u8> {
    let mut encoded = vec![(amount & 127) as u8];
    let mut rest = amount;
    while rest > 127 {
        rest = (rest >> 7) - 1;
        encoded.push((rest & 127) as u8 | 128);
    }
    encoded.reverse();
    let mut script = vec![26, 4, 3, 0, 1, 1, 20];
    script.extend(key);
    script.extend([0xcc, (48 + encoded.len()) as u8, 4, 3, 9, 1, 1, 20]);
    script.extend(key);
    script.extend([(21 + encoded.len()) as u8, 1]);
    script.extend(currency);
    script.extend(encoded);
    script.push(0x75);
    script
}

fn transfer_body(script: &[u8], transfer: &Value) -> Option<(Destination, i128, i128)> {
    let mut raw = Wire(script);
    let values = transfer["currencyvalues"].as_object()?;
    let (currency, principal) = values.iter().next()?;
    let principal = amount_sats(principal)?;
    let explicit = amount_sats(&transfer["fees"])?;
    if values.len() != 1
        || raw.varint()? != 1
        || raw.bytes(20)? != raw_id(currency)?
        || i128::from(raw.varint()?) != principal
        || raw.varint()? != 65
        || raw.bytes(20)? != ids(transfer, "feecurrencyid")?
        || i128::from(raw.varint()?) != explicit
    {
        return None;
    }
    let customer = Destination::decode(&mut raw)?;
    if customer.kind != 2
        || transfer["destination"]["type"].as_u64() != Some(2)
        || customer != Destination::from_json(transfer["destination"]["address"].as_str()?)?
        || raw.bytes(20)? != ids(transfer, "destinationcurrencyid")?
        || raw.bytes(20)? != ids(transfer, "exportto")?
        || !raw.0.is_empty()
    {
        return None;
    }
    Some((customer, principal, explicit))
}

fn state_bytes(
    script: &[u8],
    notarization: &Value,
    flags: u64,
    phase: u8,
) -> Option<(Option<Destination>, u64)> {
    let mut raw = Wire(cc_payload(script, 5)?);
    if raw.varint()? != 2 || raw.varint()? != flags {
        return None;
    }
    let kind = raw.integer::<1>()?[0];
    let proposer = if kind == 0 {
        if !raw.vector()?.is_empty() {
            return None;
        }
        None
    } else {
        Some(Destination::with_kind(kind, &mut raw)?)
    };
    let description = &notarization["proposer"];
    let currency = ids(notarization, "currencyid")?;
    let state = &notarization["currencystate"];
    if u64::from(kind) != description["type"].as_u64()?
        || proposer
            != description["address"]
                .as_str()
                .and_then(Destination::from_json)
        || raw.bytes(20)? != currency
        || raw.bytes(4)? != [1, 0, phase, 0]
        || raw.bytes(20)? != currency
        || ids(state, "currencyid")? != currency
    {
        return None;
    }
    let reserve_ids = raw.array(20)?;
    let declared = state["reservecurrencies"].as_array()?;
    if declared.len() > 32
        || reserve_ids.len() != declared.len() * 20
        || !reserve_ids
            .as_chunks::<20>()
            .0
            .iter()
            .zip(declared)
            .all(|(id, item)| ids(item, "currencyid").is_some_and(|bytes| &bytes == id))
    {
        return None;
    }
    raw.array(4)?;
    raw.array(8)?;
    for _ in 0..3 {
        raw.varint()?;
    }
    raw.bytes(32)?;
    for (field, width) in [
        ("reservein", 8),
        ("primarycurrencyin", 8),
        ("reserveout", 8),
        ("lastconversionprice", 8),
        ("viaconversionprice", 8),
        ("fees", 8),
        ("priorweights", 4),
        ("conversionfees", 8),
    ] {
        let encoded = raw.array(width)?;
        if encoded.len() != declared.len() * width {
            return None;
        }
        if field == "priorweights" {
            continue;
        }
        for (entry, bytes) in declared.iter().zip(encoded.as_chunks::<8>().0) {
            let id = entry["currencyid"].as_str()?;
            let sat = i128::from(i64::from_le_bytes(*bytes));
            if replay::signed_amount_sats(&state["currencies"][id][field]) != Some(sat) {
                return None;
            }
        }
    }
    let height = u64::from(u32::from_le_bytes(raw.integer()?));
    (notarization["notarizationheight"].as_u64() == Some(height)).then_some((proposer, height))
}

fn configured_notaries(
    definition: &Value,
    payload: &[u8],
    parent: &str,
    system: &str,
) -> Option<Vec<[u8; 20]>> {
    let mut raw = Wire(payload);
    if raw.bytes(4)? != [1, 0, 0, 0]
        || u32::from_le_bytes(raw.integer()?) & 256 == 0
        || raw.bytes(20)? != raw_id(parent)?
    {
        return None;
    }
    raw.vector()?; // Name.
    if raw.bytes(20)? != raw_id(parent)? || raw.bytes(20)? != raw_id(system)? {
        return None;
    }
    raw.bytes(8)?; // Notarization and proof protocols.
    if raw.bytes(1)? != [0] || !raw.vector()?.is_empty() {
        return None;
    }
    raw.bytes(20)?; // Gateway.
    raw.varint()?; // Start.
    raw.varint()?; // End.
    raw.bytes(8)?; // Initial supply.
    raw.array(28)?; // Preallocations.
    raw.bytes(8)?; // Gateway issuance.
    raw.array(20)?; // Currencies.
    raw.array(4)?; // Weights.
    for _ in 0..5 {
        raw.array(8)?;
    }
    raw.varint()?; // Prelaunch discount.
    raw.bytes(4)?; // Prelaunch carve-out.
    let encoded = raw.array(20)?;
    let declared = definition["notaries"].as_array()?;
    if !(1..=64).contains(&declared.len()) || encoded.len() != declared.len() * 20 {
        return None;
    }
    encoded
        .as_chunks::<20>()
        .0
        .iter()
        .zip(declared)
        .map(|(bytes, value)| {
            let id = raw_id(value.as_str()?)?;
            (bytes == &id).then_some(id)
        })
        .collect()
}

async fn request(
    client: &RpcClient,
    source: &str,
    method: &str,
    params: &[Value],
) -> Option<Value> {
    client.call_on(source, method, params).await.ok()
}

async fn tx(client: &RpcClient, source: &str, id: &str) -> Option<Value> {
    let tx = request(client, source, "getrawtransaction", &[json!(id), json!(1)]).await?;
    checked_transaction(&tx, id)?;
    Some(tx)
}

fn import_binding(
    raw: &[u8],
    decoded: &Value,
    currency_value: &[u8; 20],
    value: i128,
) -> Option<()> {
    let mut wire = Wire(cc_payload(raw, 13)?);
    let version = u16::from_le_bytes(wire.integer()?);
    let flags = u16::from_le_bytes(wire.integer()?);
    if version != 1 || u64::from(flags) != decoded["flags"].as_u64()?
        || wire.bytes(20)? != ids(decoded, "sourcesystemid")?
        || u64::from(u32::from_le_bytes(wire.integer()?)) != decoded["sourceheight"].as_u64()?
        || wire.bytes(20)? != ids(decoded,"importcurrencyid")?
        || wire.count()? != 1 || wire.bytes(20)? != currency_value
        || i128::from(i64::from_le_bytes(wire.integer()?)) != value
        || wire.count()? != 0 // No burned or imported-token adjustment.
        || wire.bytes(4)? != [5,0,0,0]
        || display_hash(wire.bytes(32)?) != decoded["hashtransfers"].as_str()?
        || display_hash(wire.bytes(32)?) != decoded["exporttxid"].as_str()?
        || u64::from(u32::from_le_bytes(wire.integer()?)) != decoded["exporttxout"].as_u64()?
        || !wire.0.is_empty()
    {
        return None;
    }
    Some(())
}

impl Evidence<'_> {
    pub(super) async fn verify(&self) -> Option<Rewards> {
        tokio::time::timeout(std::time::Duration::from_secs(12), self.verify_inner())
            .await
            .ok()
            .flatten()
    }

    async fn verify_inner(&self) -> Option<Rewards> {
        let main = &self.record["import"];
        let companion = &self.record["sysimport"];
        let notarization = &self.record["importnotarization"];
        let state = &notarization["currencystate"];
        let currencies = &state["currencies"];
        let source = main["sourcesystemid"].as_str()?;
        let source_system = source.to_owned().try_into().ok()?;
        let source_raw = raw_id(source)?;
        let system = self.root.system_id.as_ref();
        let currency = main["importcurrencyid"].as_str()?;
        let height = self.record["importheight"].as_u64()?;
        let source_height = main["sourceheight"].as_u64()?;
        let export_id = main["exporttxid"].as_str()?;
        let export_number = u32::try_from(main["exporttxout"].as_u64()?).ok()?;
        let export = &output(self.export_tx, export_number)?["scriptPubKey"]["crosschainexport"];
        let companion_number = u32::try_from(companion["exporttxout"].as_u64()?).ok()?;
        let companion_export =
            &output(self.export_tx, companion_number)?["scriptPubKey"]["crosschainexport"];
        let transfer = self.record["transfers"].get(0)?;

        if !applicable(self.record)
            || source == system
            || self.root.vout != 0
            || self.record["importtxid"].as_str() != Some(self.root.txid.as_str())
            || self.record["importvout"].as_u64() != Some(0)
            || self.import_tx["height"].as_u64() != Some(height)
            || output(self.import_tx, 0)?["scriptPubKey"]["crosschainimport"] != *main
            || output(self.import_tx, 1)?["scriptPubKey"]["crosschainimport"] != *companion
            || output(self.import_tx, 2)?["scriptPubKey"]["acceptednotarization"] != *notarization
            || !output(self.import_tx, 3)?["scriptPubKey"]["notaryevidence"].is_object()
            || notarization["currencyid"].as_str() != Some(currency)
            || companion["flags"].as_u64() != Some(68)
            || companion["importcurrencyid"].as_str() != Some(source)
            || companion["sourcesystemid"].as_str() != Some(source)
            || companion["sourceheight"] != main["sourceheight"]
            || companion["exporttxid"] != main["exporttxid"]
            || companion["hashtransfers"] != main["hashtransfers"]
            || export_number == companion_number
            || export["flags"].as_u64() != Some(128)
            || export["numinputs"].as_u64() != Some(1)
            || export["sourcesystemid"].as_str() != Some(source)
            || export["destinationsystemid"].as_str() != Some(system)
            || export["destinationcurrencyid"].as_str() != Some(currency)
            || companion_export["sourcesystemid"].as_str() != Some(source)
            || companion_export["destinationsystemid"].as_str() != Some(system)
            || companion_export["destinationcurrencyid"].as_str() != Some(system)
            || export["hashtransfers"] != main["hashtransfers"]
            || export["sourceheightend"].as_u64() != Some(source_height)
            || transfer["destinationcurrencyid"].as_str() != Some(currency)
            || transfer["exportto"].as_str() != Some(system)
            || transfer["feecurrencyid"].as_str() != Some(source)
            || transfer["version"].as_u64() != Some(1)
        {
            return None;
        }
        checked_transaction(self.export_tx, export_id)?;
        checked_transaction(self.import_tx, &self.root.txid)?;
        let (start, outputs) = generated_region(self.import_tx, main, 0)?;
        if start != 5 || outputs.len() != 5 {
            return None;
        }

        let payees = exporters::payees(export, &bound_bytes(self.export_tx, export_number)?)?;
        let primary = payees.primary.as_ref()?;
        let auxiliary = payees.auxiliary.as_ref()?;
        let first = usize::try_from(export["firstinput"].as_u64()?).ok()?;
        let input = self.export_tx["vin"].get(first)?;
        let funding_id = input["txid"].as_str()?;
        let funding_vout = u32::try_from(input["vout"].as_u64()?).ok()?;
        let funding = tx(self.source, self.source_id, funding_id).await?;
        let instruction = output(&funding, funding_vout)?;
        let raw_instruction = bound_bytes(&funding, funding_vout)?;
        let body = cc_payload(&raw_instruction, 8)?;
        let (customer, principal, explicit) = transfer_body(body, transfer)?;
        let total = principal.checked_add(explicit)?;
        let supplemental = &output(self.import_tx, 4)?["scriptPubKey"]["crosschainexport"];
        let raw_supplemental = bound_bytes(self.import_tx, 4)?;
        let mut raw = Wire(cc_payload(&raw_supplemental, 12)?);
        if funding["height"].as_u64() != Some(source_height)
            || !transfer_matches(&instruction["scriptPubKey"]["reservetransfer"], transfer)
            || raw.bytes(4)? != [1, 0, 24, 0]
            || raw.bytes(20)? != source_raw
            || raw.count()? != 1
            || raw.0 != body
            || supplemental["transfers"] != self.record["transfers"]
            || display_hash(&hash::verus_hash(body)?) != main["hashtransfers"].as_str()?
            || native(instruction) != Some(total)
            || !singleton_map(&main["valuein"], source, total)
            || !singleton_map(&companion["valuein"], source, total)
            || !singleton_map(&export["totalamounts"], source, total)
            || !singleton_map(&export["totalfees"], source, explicit)
            || !singleton_map(&transfer["currencyvalues"], source, principal)
        {
            return None;
        }
        import_binding(&bound_bytes(self.import_tx, 0)?, main, &source_raw, total)?;
        import_binding(
            &bound_bytes(self.import_tx, 1)?,
            companion,
            &source_raw,
            total,
        )?;

        let (proposer, entropy_height) =
            state_bytes(&bound_bytes(self.import_tx, 2)?, notarization, 396, 49)?;
        let converted = explicit
            .checked_mul(UNIT)?
            .checked_div(amount_sats(&currencies[source]["lastconversionprice"])?)?
            .checked_mul(amount_sats(&currencies[system]["viaconversionprice"])?)?
            .checked_div(UNIT)?;
        let amounts = payout_amounts(converted)?;
        if amount_sats(&currencies[system]["reserveout"]) != Some(converted)
            || amount_sats(&currencies[source]["fees"]) != Some(explicit)
            || currencies.as_object()?.iter().any(|(id, values)| {
                amount_sats(&values["conversionfees"]) != Some(0)
                    || (id != source && amount_sats(&values["fees"]) != Some(0))
            })
            || native(&outputs[0]) != Some(0)
            || bound_bytes(self.import_tx, 5)?
                != pkh_receipt(customer.key, source_raw, u64::try_from(principal).ok()?)
            || !singleton_map(
                &outputs[0]["scriptPubKey"]["reserveoutput"]["currencyvalues"],
                source,
                principal,
            )
        {
            return None;
        }

        let funding_batch = [&funding];
        let (definition, notary, producer) = tokio::join!(
            self.notary_definition(&source_system, source, system),
            self.entropy(height, entropy_height, currency),
            async {
                tokio::time::timeout(
                    std::time::Duration::from_secs(3),
                    exporters::source_producer(
                        self.source,
                        self.source_id,
                        &source_system,
                        currency,
                        export,
                        &funding_batch,
                    ),
                )
                .await
                .ok()
                .flatten()
            },
        );
        let (definition_ref, notaries) = definition?;
        let key = notaries[usize::try_from(notary? % notaries.len() as u64).ok()?];
        let recipients = [
            proposer?.script(),
            Destination { kind: 4, key }.script(),
            auxiliary.script.clone(),
            primary.script.clone(),
        ];
        let labels = [
            "Import proposer reward",
            "Configured notary reward",
            exporters::role(false, auxiliary, producer.as_ref()),
            exporters::role(true, primary, producer.as_ref()),
        ];
        let producer = producer
            .filter(|found| {
                exporters::is_producer(primary, Some(found))
                    || exporters::is_producer(auxiliary, Some(found))
            })
            .map(|found| found.output);
        let points = [6, 7, 8, 9].map(|vout| OutpointRef {
            system_id: self.root.system_id.clone(),
            txid: self.root.txid.clone(),
            vout,
        });
        for (index, recipient) in recipients.iter().enumerate() {
            let out = outputs.get(index + 1)?;
            if native(out) != Some(amounts[index])
                || &bound_bytes(self.import_tx, points[index].vout)? != recipient
                || out["scriptPubKey"].get("reserveoutput").is_some()
            {
                return None;
            }
        }
        Some(Rewards {
            funding: point(&source_system, &funding, funding_vout)?,
            customer: point(&self.root.system_id, self.import_tx, 5)?,
            principal,
            payments: std::array::from_fn(|i| (points[i].clone(), labels[i], amounts[i])),
            producer,
            definition: definition_ref,
            converted_fee: converted,
            residual: converted.checked_sub(amounts.iter().sum())?,
        })
    }

    async fn notary_definition(
        &self,
        source_system: &crate::world::SystemId,
        source: &str,
        system: &str,
    ) -> Option<(OutpointRef, Vec<[u8; 20]>)> {
        let locator = request(self.source, self.source_id, "getcurrency", &[json!(system)]).await?;
        if locator["currencyid"].as_str() != Some(system) {
            return None;
        }
        let id = locator["definitiontxid"].as_str()?;
        let index = u32::try_from(locator["definitiontxout"].as_u64()?).ok()?;
        let definition = tx(self.source, self.source_id, id).await?;
        let decoded = &output(&definition, index)?["scriptPubKey"]["currencydefinition"];
        if decoded["currencyid"].as_str() != Some(system)
            || decoded["parent"].as_str() != Some(source)
            || definition["height"].as_u64()? > self.export_tx["height"].as_u64()?
        {
            return None;
        }
        let script = bound_bytes(&definition, index)?;
        Some((
            point(source_system, &definition, index)?,
            configured_notaries(decoded, cc_payload(&script, 2)?, source, system)?,
        ))
    }

    async fn entropy(&self, import_height: u64, height: u64, currency: &str) -> Option<u64> {
        if height.checked_add(1)? != import_height {
            return None;
        }
        let hash = self.import_tx["blockhash"]
            .as_str()
            .filter(|hash| valid_txid(hash))?;
        let import = request(
            self.destination,
            self.destination_id,
            "getblockheader",
            &[json!(hash), json!(true)],
        )
        .await?;
        let parent = import["previousblockhash"]
            .as_str()
            .filter(|value| valid_txid(value))?;
        if import["height"].as_u64() != Some(import_height)
            || import["hash"].as_str() != Some(hash)
            || import["confirmations"].as_i64()? <= 0
        {
            return None;
        }
        let block = request(
            self.destination,
            self.destination_id,
            "getblockheader",
            &[json!(parent), json!(true)],
        )
        .await?;
        if block["height"].as_u64() != Some(height)
            || block["hash"].as_str() != Some(parent)
            || block["confirmations"].as_i64()? <= 0
        {
            return None;
        }
        entropy_component(&block, currency)
    }
}

fn entropy_component(block: &Value, currency: &str) -> Option<u64> {
    let component = match block["validationtype"].as_str()? {
        "work" => block["hash"].as_str()?,
        "stake" => block["rawposhashbh"].as_str()?,
        _ => return None,
    };
    let mut raw: [u8; 32] = hex(component)?.try_into().ok()?;
    if raw == [0; 32] {
        return None;
    }
    raw.reverse();
    let mut preimage = Vec::with_capacity(72);
    preimage.extend(ENTROPY_KEY);
    preimage.extend(raw);
    preimage.extend(raw_id(currency)?);
    let digest = hash::verus_hash(&preimage)?;
    Some(u64::from_le_bytes(digest[..8].try_into().ok()?))
}

impl Rewards {
    pub(super) fn append(self, trail: &mut Trail, root: &OutpointRef, include_funding: bool) {
        if include_funding {
            trail.entries.push(entry(
                &self.funding,
                "Import funding",
                "Only customer transfer in this export batch",
            ));
        }
        let detail = format!(
            "{} base units of {} match the exact source instruction and result script",
            self.principal, self.funding.system_id
        );
        trail
            .entries
            .push(entry(&self.customer, "Verified customer receipt", &detail));
        trail.edges.push(LifecycleEdge {
            source: self.funding,
            target: self.customer,
            relation: "import-customer-receipt".into(),
            resolution: "attributed".into(),
            detail,
        });
        for (point, label, amount) in self.payments {
            let detail = format!(
                "{amount} native base units of {} · exact recipient script",
                root.system_id
            );
            trail.entries.push(entry(&point, label, &detail));
            trail.edges.push(LifecycleEdge {
                source: root.clone(),
                target: point,
                relation: "import-submission-reward".into(),
                resolution: "attributed".into(),
                detail: format!("{label} · {detail}"),
            });
        }
        if let Some(producer) = self.producer {
            trail.entries.push(entry(
                &producer,
                "Source producer coinbase",
                "Coinbase payout script matches the proven exporter recipient",
            ));
        } else {
            trail.notes.push("The exporter payouts are verified. The source block producer remains unattributed.".into());
        }
        trail.entries.push(entry(&self.definition,"Notary definition",
            "Original chain definition supplies the ordered notary list used with the import's entropy header"));
        trail.notes.push(format!("Converted native fee: {} base units. Residual after the verified service payments: {} base units, with no payout role established.", self.converted_fee,self.residual));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn import_submission_split_preserves_thresholds_and_odd_exporter_remainder() {
        assert_eq!(
            payout_amounts(230_741),
            Some([37_685, 37_685, 20_000, 20_000])
        );
        assert!(payout_amounts(159_999).is_none());
        assert_eq!(
            payout_amounts(1_000_020),
            Some([50_001, 50_001, 25_000, 25_001])
        );
    }
}
