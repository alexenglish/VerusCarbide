//! Historical marketplace publication and settlement evidence.

pub(super) mod recovery;
pub(super) mod search;
use super::{
    ObjectStore, ShieldedAccounting, case_insensitive_field, decode_hex_bytes,
    marketplace_funding_shape, native_value_zat, transaction_output,
};
use crate::defi::amount_sats;
use crate::protocol::{Link, References, Target};
use crate::world::OutpointRef;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::Write;

pub(super) const OFFER_FEE: i128 = 10_000;
const MAX_PROOF_BYTES: usize = 65_535;
const SAPLING_OUTPUT_BYTES: usize = 948;

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Offer {
    pub funding: String,
    pub proof_output: usize,
    pub expiry: u32,
    pub spends_publication_funds: bool,
    header: u32,
    signature: Vec<u8>,
    sequence: u32,
    lock_time: u32,
    payment: i64,
    payment_script: Vec<u8>,
    shielded_payment: Option<ShieldedPayment>,
    contract_hash: [u8; 32],
}

#[derive(Debug, PartialEq, Eq)]
struct ShieldedPayment {
    balance: i64,
    outputs: [u8; 32],
    binding_signature: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Spend {
    Taken,
    Closed { transparent: bool, shielded: bool },
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, count: usize) -> Option<&'a [u8]> {
        let (head, tail) = self.0.split_at_checked(count)?;
        self.0 = tail;
        Some(head)
    }

    fn integer<const N: usize>(&mut self) -> Option<[u8; N]> {
        self.take(N)?.try_into().ok()
    }

    fn compact(&mut self) -> Option<usize> {
        let value = match self.integer::<1>()?[0] {
            253 => {
                let n = u64::from(u16::from_le_bytes(self.integer()?));
                if n < 253 {
                    return None;
                }
                n
            }
            254 => {
                let n = u64::from(u32::from_le_bytes(self.integer()?));
                if n <= u64::from(u16::MAX) {
                    return None;
                }
                n
            }
            255 => {
                let n = u64::from_le_bytes(self.integer()?);
                if n <= u64::from(u32::MAX) {
                    return None;
                }
                n
            }
            n => u64::from(n),
        };
        usize::try_from(value).ok()
    }

    fn vector(&mut self) -> Option<&'a [u8]> {
        let size = self.compact()?;
        self.take(size)
    }

    fn expect(&mut self, bytes: &[u8]) -> Option<()> {
        self.0 = self.0.strip_prefix(bytes)?;
        Some(())
    }
}

pub(super) fn publication(raw: &Value) -> Option<Offer> {
    let outputs = raw["vout"].as_array()?;
    let proof_output = outputs.len().checked_sub(1)?;
    let output = &outputs[proof_output];
    let hex = output.pointer("/scriptPubKey/hex")?.as_str()?;
    if hex.len() > (MAX_PROOF_BYTES + 4) * 2 || !hex.starts_with("6a") && !hex.starts_with("6A") {
        return None;
    }
    let script = decode_hex_bytes(hex)?;
    let mut outer = Reader(&script);
    outer.expect(&[0x6a])?;
    let length = match outer.integer::<1>()?[0] {
        n @ 1..=75 => usize::from(n),
        0x4c => usize::from(outer.integer::<1>()?[0]),
        0x4d => usize::from(u16::from_le_bytes(outer.integer()?)),
        _ => return None,
    };
    let mut proof = Reader(outer.take(length)?);
    if !outer.0.is_empty() {
        return None;
    }

    // OPRETTYPE_OBJECTARR, one CHAINOBJ_TRANSACTION_PROOF of type TYPE_FULLTX,
    // empty transaction MMR, one TX_FULL component at index zero.
    proof.expect(&[5, 0, 0, 0, 3, 0])?;
    if !matches!(proof.integer::<1>()?[0], 1 | 2) {
        return None;
    }
    proof.expect(&[1, 0, 0, 0, 0, 1, 0, 0, 0, 0])?;
    let template = proof.vector()?;
    let mut tx = Reader(template);
    // The full transaction component also has an empty MMR proof.
    if proof.0 != [0, 0, 0, 0] {
        return None;
    }

    let header = u32::from_le_bytes(tx.integer()?);
    match header {
        0x8000_0004 => tx.expect(&0x892f_2085u32.to_le_bytes())?,
        0x8000_0003 => tx.expect(&0x03c4_8270u32.to_le_bytes())?,
        _ => return None,
    }
    tx.expect(&[1])?;
    let hash = tx.take(32)?;
    if hash.iter().all(|&byte| byte == 0) {
        return None;
    }
    let mut funding = String::with_capacity(64);
    for byte in hash.iter().rev() {
        write!(&mut funding, "{byte:02x}").ok()?;
    }
    // makeoffer locks the asset at output zero and funds publication at one.
    tx.expect(&[0; 4])?;
    let signature = tx.vector()?.to_vec();
    if signature.is_empty() {
        return None;
    }
    let sequence = u32::from_le_bytes(tx.integer()?);
    tx.expect(&[1])?;
    let payment = i64::from_le_bytes(tx.integer()?);
    let payment_script = tx.vector()?.to_vec();
    if payment < 0 || payment_script.is_empty() {
        return None;
    }
    let lock_time = u32::from_le_bytes(tx.integer()?);
    let expiry = u32::from_le_bytes(tx.integer()?);
    let private = if header == 0x8000_0004 {
        let balance = i64::from_le_bytes(tx.integer()?);
        tx.expect(&[0])?; // No shielded spends in a posted template.
        let count = tx.compact()?;
        let outputs = tx.take(count.checked_mul(SAPLING_OUTPUT_BYTES)?)?;
        match (count, balance) {
            (0, 0) => None,
            (_, balance) if count > 0 && balance < 0 => Some((balance, outputs)),
            _ => return None,
        }
    } else {
        None
    };
    tx.expect(&[0])?; // No JoinSplits.
    let shielded_payment = match private {
        Some((balance, outputs)) => Some(ShieldedPayment {
            balance,
            outputs: Sha256::digest(outputs).into(),
            binding_signature: tx.take(64)?.to_vec(),
        }),
        None => None,
    };
    if !tx.0.is_empty() {
        return None;
    }

    // In addition to a separate publication spending funding:1, the daemon
    // recognizes a posting-shaped transaction carrying its own last-output proof.
    // The decoded template names the funding transaction in both cases.
    let carrier = raw["vin"]
        .as_array()?
        .iter()
        .any(|input| spends(input, &funding, 1));
    let embedded = outputs.len() > 1 && marketplace_funding_shape(raw);
    if !carrier && !embedded {
        return None;
    }
    Some(Offer {
        funding,
        proof_output,
        expiry,
        spends_publication_funds: carrier,
        header,
        signature,
        sequence,
        lock_time,
        payment,
        payment_script,
        shielded_payment,
        contract_hash: Sha256::digest(template).into(),
    })
}

pub(super) fn spends(input: &Value, txid: &str, vout: u64) -> bool {
    input["txid"].as_str() == Some(txid) && input["vout"].as_u64() == Some(vout)
}

pub(super) fn native_amount(output: &Value) -> Option<i128> {
    native_value_zat(output).or_else(|| amount_sats(&output["value"]))
}

fn reserves(script: &Value, kind: &str) -> Option<BTreeMap<String, i128>> {
    let Some(values) = script[kind].get("currencyvalues") else {
        return Some(BTreeMap::new());
    };
    values
        .as_object()?
        .iter()
        .try_fold(BTreeMap::new(), |mut out, (id, amount)| {
            let amount = amount_sats(amount)?;
            if amount != 0 {
                out.insert(id.clone(), amount);
            }
            Some(out)
        })
}

fn currency_output(script: &Value) -> Option<BTreeMap<String, i128>> {
    if script["reserveoutput"].is_object() {
        return reserves(script, "reserveoutput");
    }
    (matches!(
        script["type"].as_str()?,
        "pubkey" | "pubkeyhash" | "scripthash"
    ) || (script["evalnone"].as_str() == Some("valid")
        && script["spendableoutput"].as_bool() == Some(true)))
    .then(BTreeMap::new)
}

fn delivery(funding: &Value, output: &Value) -> Option<()> {
    let from = &funding["scriptPubKey"];
    let to = &output["scriptPubKey"];
    if let Some(id) = from["identityprimary"]["identityaddress"].as_str() {
        return (to["identityprimary"]["identityaddress"].as_str() == Some(id)).then_some(());
    }
    let delivered = currency_output(to)?;
    let locked = reserves(from, "commitmenthash")?;
    let native = native_amount(output)?;
    (native >= 0
        && native <= native_amount(funding)?
        && (native > 0 || !delivered.is_empty())
        && delivered
            .iter()
            .all(|(id, amount)| locked.get(id).is_some_and(|limit| amount <= limit)))
    .then_some(())
}

fn no_private_inputs(raw: &Value) -> bool {
    ["vshieldedspend", "vjoinsplit"].iter().all(|key| {
        case_insensitive_field(raw, key)
            .is_none_or(|value| value.as_array().is_some_and(Vec::is_empty))
    })
}

pub(super) fn orphan(raw: &Value) -> bool {
    raw["confirmations"].as_i64().is_some_and(|value| value < 0)
}

fn shielded_outputs_hash(outputs: &[Value]) -> Option<[u8; 32]> {
    let mut hash = Sha256::new();
    for output in outputs {
        // The RPC prints uint256s in display byte order and byte arrays in wire order.
        for (key, size, reverse) in [
            ("cv", 32, true),
            ("cmu", 32, true),
            ("ephemeralKey", 32, true),
            ("encCiphertext", 580, false),
            ("outCiphertext", 80, false),
            ("proof", 192, false),
        ] {
            let hex = output[key].as_str()?;
            if hex.len() != size * 2 {
                return None;
            }
            let mut bytes = decode_hex_bytes(hex)?;
            if reverse {
                bytes.reverse();
            }
            hash.update(bytes);
        }
    }
    Some(hash.finalize().into())
}

fn matches_shielded_payment(offer: &Offer, raw: &Value, accounting: ShieldedAccounting) -> bool {
    if !accounting.complete {
        return false;
    }
    let outputs = case_insensitive_field(raw, "vshieldedoutput")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    match &offer.shielded_payment {
        Some(payment) => {
            accounting.sapling_value_balance_zat == i128::from(payment.balance)
                && shielded_outputs_hash(outputs) == Some(payment.outputs)
                && raw["bindingSig"]
                    .as_str()
                    .and_then(decode_hex_bytes)
                    .as_deref()
                    == Some(payment.binding_signature.as_slice())
        }
        None => outputs.is_empty() && accounting.sapling_value_balance_zat == 0,
    }
}

pub(super) fn classify(offer: &Offer, funding: &Value, raw: &Value) -> Option<Spend> {
    let inputs = raw["vin"].as_array()?;
    let input = inputs.first()?;
    if !spends(input, &offer.funding, 0) || !no_private_inputs(raw) || orphan(raw) {
        return None;
    }
    let locked = transaction_output(funding, 0)?;
    let outputs = raw["vout"].as_array()?;
    let height = raw["height"].as_u64();
    let private_outputs = case_insensitive_field(raw, "vshieldedoutput")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    let accounting = ShieldedAccounting::from_raw(raw);

    // The signed outpoint, input bytes and consideration survive takeoffer.
    // The first appended output delivers the offered identity or currency.
    if inputs.len() > 1
        && height.is_some_and(|h| h > 0 && (offer.expiry == 0 || h <= u64::from(offer.expiry)))
        && raw["overwintered"].as_bool() == Some(true)
        && raw["version"].as_u64() == Some(u64::from(offer.header & 0x7fff_ffff))
        && raw["expiryheight"].as_u64() == Some(offer.expiry.into())
        && raw["locktime"].as_u64() == Some(offer.lock_time.into())
        && input["sequence"].as_u64() == Some(offer.sequence.into())
        && decode_hex_bytes(input.pointer("/scriptSig/hex")?.as_str()?)? == offer.signature
        && matches_shielded_payment(offer, raw, accounting)
        && offer.matches_transparent_payment(outputs.first()?)
        && delivery(locked, outputs.get(1)?).is_some()
    {
        return Some(Spend::Taken);
    }

    // A standalone wallet close returns the locked assets, net of its fee.
    // Batch closes and unrelated/funding inputs do not prove this allocation.
    if inputs.len() != 1 || outputs.len() > 1 || !accounting.complete || private_outputs > 1 {
        return None;
    }
    let locked_script = &locked["scriptPubKey"];
    let native = native_amount(locked)?;
    let transparent = !outputs.is_empty();
    let shielded = private_outputs == 1;
    let public_amount = if let Some(output) = outputs.first() {
        native_amount(output)?
    } else {
        0
    };
    let private_amount = -accounting.sapling_value_balance_zat;
    if public_amount < 0 || (shielded && private_amount <= 0) || (!shielded && private_amount != 0)
    {
        return None;
    }
    if locked_script["identityprimary"].is_object() {
        let returned = &outputs.first()?["scriptPubKey"]["identityprimary"];
        if shielded
            || returned != &locked_script["identityprimary"]
            || public_amount != native.saturating_sub(2 * OFFER_FEE).max(0)
        {
            return None;
        }
    } else {
        let locked_reserves = reserves(locked_script, "commitmenthash")?;
        let returned = if let Some(output) = outputs.first() {
            currency_output(&output["scriptPubKey"])?
        } else {
            BTreeMap::new()
        };
        if public_amount.checked_add(private_amount)? != native.checked_sub(OFFER_FEE)?
            || returned != locked_reserves
            || !transparent && !shielded
        {
            return None;
        }
    }
    Some(Spend::Closed {
        transparent,
        shielded,
    })
}

impl Offer {
    fn matches_transparent_payment(&self, output: &Value) -> bool {
        native_amount(output) == Some(i128::from(self.payment))
            && output
                .pointer("/scriptPubKey/hex")
                .and_then(Value::as_str)
                .and_then(decode_hex_bytes)
                .as_deref()
                == Some(self.payment_script.as_slice())
    }

    pub(super) fn same_contract(&self, other: &Self) -> bool {
        self.contract_hash == other.contract_hash
    }
}

pub(super) struct FlowEvidence<'a> {
    store: &'a ObjectStore,
    raw: &'a Value,
    publication: Option<&'a Offer>,
    funded: Option<(&'a str, &'a Offer)>,
    consumed: Vec<(usize, &'a str, &'a Offer)>,
    spend: Option<Spend>,
    recovery: Option<recovery::Recovery>,
}

impl<'a> FlowEvidence<'a> {
    pub(super) fn new(raw: &'a Value, store: &'a ObjectStore) -> Self {
        let txid = raw["txid"].as_str().unwrap_or_default();
        let publication = store.marketplace_publication(txid).filter(|offer| {
            store
                .transaction(&offer.funding)
                .is_some_and(|tx| marketplace_funding_shape(&tx.raw_json))
        });
        let funded = store.marketplace_contract(txid);
        let consumed = raw["vin"]
            .as_array()
            .into_iter()
            .flatten()
            .enumerate()
            .filter_map(|(index, input)| {
                let funding = input["txid"].as_str()?;
                if input["vout"].as_u64() != Some(0) {
                    return None;
                }
                let (proof, offer) = store.marketplace_contract(funding)?;
                Some((index, proof, offer))
            })
            .collect();
        let spend = store.marketplace_execution(raw);
        let recovery = spend
            .is_none()
            .then(|| recovery::recover(raw, store))
            .flatten();
        Self {
            store,
            raw,
            publication,
            funded,
            consumed,
            spend,
            recovery,
        }
    }

    fn link(&self, references: &mut References, label: &str, txid: &str, vout: usize) {
        if let Some(system_id) = self.store.workspace_system.clone()
            && let Ok(vout) = u32::try_from(vout)
            && super::is_hash_like(txid)
        {
            references.links.push(Link {
                label: label.into(),
                target: Target::Outpoint(OutpointRef {
                    system_id,
                    txid: txid.into(),
                    vout,
                }),
            });
        }
    }

    fn consumed(&self, index: usize) -> Option<(&str, &Offer)> {
        self.consumed
            .iter()
            .find(|(input, _, _)| *input == index)
            .map(|(_, proof, offer)| (*proof, *offer))
    }

    pub(super) fn input_links(&self, index: usize) -> References {
        let mut refs = References::default();
        if let Some((proof, offer)) = self.consumed(index) {
            self.link(
                &mut refs,
                "Signed offer publication",
                proof,
                offer.proof_output,
            );
        }
        refs
    }

    pub(super) fn input_annotation(&self, index: usize) -> Option<&'static str> {
        self.consumed(index)?;
        Some(match (self.spend, index) {
            (Some(Spend::Taken), 0) => "Marketplace Offer Settlement",
            (Some(Spend::Closed { .. }), 0) => "Marketplace Offer Cancellation",
            _ if self
                .recovery
                .as_ref()
                .is_some_and(|result| result.inputs.contains(&index)) =>
            {
                "Marketplace Funding Released"
            }
            _ if self
                .recovery
                .as_ref()
                .is_some_and(|result| result.reserve_inputs.contains(&index)) =>
            {
                "Marketplace Reserve Assets Released"
            }
            _ => "Marketplace Funding Spend · outcome unverified",
        })
    }

    pub(super) fn is_publication_output(&self, index: usize) -> bool {
        self.publication
            .is_some_and(|offer| offer.proof_output == index)
    }

    pub(super) fn shielded_annotation(&self) -> Option<&'static str> {
        match self.spend {
            Some(Spend::Taken)
                if self
                    .consumed(0)
                    .is_some_and(|(_, offer)| offer.shielded_payment.is_some()) =>
            {
                Some("Marketplace Seller Consideration")
            }
            Some(Spend::Closed { shielded: true, .. }) => Some("Marketplace Offer Refund"),
            _ if self
                .recovery
                .as_ref()
                .is_some_and(|result| !result.shielded_inputs.is_empty()) =>
            {
                Some("Marketplace Release · shielded total only")
            }
            _ => None,
        }
    }

    pub(super) fn output_evidence(
        &self,
        index: usize,
        references: &mut References,
        annotations: &mut Vec<String>,
    ) {
        if let Some(offer) = self.publication.filter(|offer| offer.proof_output == index) {
            self.link(references, "Marketplace funding output", &offer.funding, 0);
            annotations.push(if offer.expiry == 0 {
                "Signed offer · no transaction expiry height".into()
            } else {
                format!("Signed offer · expires at height {}", offer.expiry)
            });
        }
        if index == 0
            && let Some((proof, offer)) = self.funded
        {
            self.link(
                references,
                "Signed offer publication",
                proof,
                offer.proof_output,
            );
            if let Some(spender) = self.raw["vout"][0]["spentTxId"].as_str() {
                self.link(references, "Marketplace funding spender", spender, 0);
            }
        }
        if let Some(result) = &self.recovery {
            for (inputs, label, shared) in [
                (
                    result.outputs.get(&index),
                    "Marketplace Returned Asset",
                    "Marketplace Shared Return",
                ),
                (
                    result.reserve_outputs.get(&index),
                    "Marketplace Reserve Return",
                    "Marketplace Shared Reserve Return",
                ),
            ] {
                if let Some(inputs) = inputs {
                    annotations.push(if inputs.len() == 1 { label } else { shared }.into());
                    for input in inputs
                        .iter()
                        .take(crate::protocol::MAX_LINKS.saturating_sub(references.links.len()))
                    {
                        if let Some((_, offer)) = self.consumed(*input) {
                            self.link(references, "Marketplace funding output", &offer.funding, 0);
                        }
                    }
                }
            }
        }
        let label = match (self.spend, index) {
            (Some(Spend::Taken), 0) => "Marketplace Seller Consideration",
            (Some(Spend::Taken), 1) => "Marketplace Taker Delivery",
            (
                Some(Spend::Closed {
                    transparent: true, ..
                }),
                0,
            ) => "Marketplace Offer Refund",
            _ => return,
        };
        annotations.push(label.into());
        if let Some((proof, offer)) = self.consumed(0) {
            self.link(references, "Marketplace funding output", &offer.funding, 0);
            self.link(
                references,
                "Signed offer publication",
                proof,
                offer.proof_output,
            );
        }
    }
}

#[cfg(test)]
pub(super) fn test_publication(funding_digit: u8, payment: i64) -> String {
    let mut template = 0x8000_0004u32.to_le_bytes().to_vec();
    template.extend(0x892f_2085u32.to_le_bytes());
    template.push(1);
    template.extend([funding_digit * 17; 32]);
    template.extend([0, 0, 0, 0, 1, 0x51]);
    template.extend(u32::MAX.to_le_bytes());
    template.push(1);
    template.extend(payment.to_le_bytes());
    template.extend([1, 0x52]);
    template.extend(0u32.to_le_bytes());
    template.extend(100u32.to_le_bytes());
    template.extend([0; 11]);
    let mut payload = vec![5, 0, 0, 0, 3, 0, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0];
    payload.push(u8::try_from(template.len()).unwrap());
    payload.extend(template);
    payload.extend([0; 4]);
    let mut script = vec![0x6a, 0x4c, u8::try_from(payload.len()).unwrap()];
    script.extend(payload);
    let mut hex = String::new();
    for byte in script {
        write!(&mut hex, "{byte:02x}").unwrap();
    }
    hex
}
