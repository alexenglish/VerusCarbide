use ripemd::{Digest, Ripemd160};
use serde::de;
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use sha2::Sha256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OutputTagKind {
    EmissionTimelock,
    EarnedNotarization,
    AcceptedNotarization,
    NotarizationFinalization(FinalizationTag),
    ExportFinalization(FinalizationTag),
    LegacyNotarization,
    NotaryEvidence,
    CurrencyDefinition,
    ReserveTransfer,
    Burn,
    AutoArbitrage,
    Conversion,
    CrossChainTransfer,
    Export,
    Import,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FinalizationTag {
    Pending,
    Confirms,
    ConfirmsEarlier,
    Rejects,
    Unknown,
}

impl OutputTagKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::EmissionTimelock => "Emission Timelock",
            Self::EarnedNotarization => "Earned Notarization",
            Self::AcceptedNotarization => "Accepted Notarization",
            Self::NotarizationFinalization(status) => match status {
                FinalizationTag::Pending => "Pending Notarization Finalization",
                FinalizationTag::Confirms => "Confirms Referenced Notarization",
                FinalizationTag::ConfirmsEarlier => "Confirms Earlier Notarization",
                FinalizationTag::Rejects => "Rejects Referenced Notarization",
                FinalizationTag::Unknown => "Notarization Finalization",
            },
            Self::ExportFinalization(status) => match status {
                FinalizationTag::Pending => "Pending Export Finalization",
                FinalizationTag::Confirms | FinalizationTag::ConfirmsEarlier => {
                    "Confirms Referenced Export"
                }
                FinalizationTag::Rejects => "Rejects Referenced Export",
                FinalizationTag::Unknown => "Export Finalization",
            },
            Self::LegacyNotarization => "Legacy Notarization",
            Self::NotaryEvidence => "Notary Evidence",
            Self::CurrencyDefinition => "Currency Definition",
            Self::ReserveTransfer => "Reserve Transfer",
            Self::Burn => "Burn",
            Self::AutoArbitrage => "Auto-Arbitrage Transfer",
            Self::Conversion => "Conversion",
            Self::CrossChainTransfer => "Cross-Chain Transfer",
            Self::Export => "Export",
            Self::Import => "Import",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OutputTag {
    pub kind: OutputTagKind,
    pub currency_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MarketplaceTag {
    Offer,
    OfferClosed,
    OfferProof,
    OfferReleased,
    OfferTaken,
}

impl MarketplaceTag {
    pub fn label(self) -> &'static str {
        match self {
            Self::Offer => "Marketplace Offer",
            Self::OfferClosed => "Marketplace Offer Closed",
            Self::OfferProof => "Marketplace Offer Proof",
            Self::OfferReleased => "Marketplace Funding Released",
            Self::OfferTaken => "Marketplace Offer Taken",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionTag {
    Coinbase,
    Stake,
    PrivateInput,
    PrivateOutput,
    NameCommitment,
    IdentityRegistration,
    IdentityUpdate,
    Output(OutputTag),
    ConsumptionBurn,
    StakeGuardEnforcement,
    AutoArbitrageImport,
    Marketplace(MarketplaceTag),
}

impl TransactionTag {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Coinbase => "Coinbase",
            Self::Stake => "Stake",
            Self::PrivateInput => "Private Input",
            Self::PrivateOutput => "Private Output",
            Self::NameCommitment => "Name Commitment",
            Self::IdentityRegistration => "Identity Registration",
            Self::IdentityUpdate => "Identity Update",
            Self::Output(tag) => tag.kind.label(),
            Self::ConsumptionBurn => "Consumption Burn",
            Self::StakeGuardEnforcement => "StakeGuard Enforcement",
            Self::AutoArbitrageImport => "Auto-Arbitrage Import",
            Self::Marketplace(tag) => tag.label(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmissionTimelock {
    pub locked_output_index: usize,
    pub metadata_output_index: usize,
    pub unlock_height: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectRoute {
    Tip,
    BlockHeight(u64),
    BlockTime(String),
    Estimate(String),
    Hash(String),
    Address(String),
    Identity(String),
    Unknown(String),
}

#[derive(Debug, Clone, Deserialize)]
pub struct BlockSummary {
    pub hash: String,
    pub height: u64,
    pub time: u64,
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(default)]
    pub flags: Option<String>,
    #[serde(default)]
    pub blocktype: Option<String>,
    #[serde(default)]
    pub previousblockhash: Option<String>,
    #[serde(default)]
    pub nextblockhash: Option<String>,
    #[serde(default)]
    pub tx: Vec<TransactionSummary>,
}

impl BlockSummary {
    pub fn production_label(&self) -> String {
        self.blocktype
            .as_deref()
            .map(str::trim)
            .filter(|blocktype| !blocktype.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| match self.production_kind() {
                BlockProductionKind::Mined => "mined".to_string(),
                BlockProductionKind::Staked => "staked".to_string(),
            })
    }

    pub fn production_kind(&self) -> BlockProductionKind {
        if let Some(blocktype) = self.blocktype.as_deref() {
            let blocktype = blocktype.to_ascii_lowercase();
            if blocktype.contains("stake") {
                return BlockProductionKind::Staked;
            }
            if blocktype.contains("mine") {
                return BlockProductionKind::Mined;
            }
        }

        match self.flags.as_deref() {
            Some(flags) if flags.to_ascii_lowercase().contains("proof-of-stake") => {
                BlockProductionKind::Staked
            }
            _ => BlockProductionKind::Mined,
        }
    }

    pub fn tx_count(&self) -> usize {
        self.tx.len()
    }

    pub fn short_hash(&self) -> String {
        if self.hash.len() <= 12 {
            self.hash.clone()
        } else {
            format!(
                "{}...{}",
                &self.hash[..6],
                &self.hash[self.hash.len() - 6..]
            )
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockProductionKind {
    Mined,
    Staked,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TransactionSummary {
    pub txid: String,
    pub raw: Option<Value>,
}

impl TransactionSummary {
    pub fn tags(&self) -> Vec<TransactionTag> {
        let Some(raw) = &self.raw else {
            return Vec::new();
        };
        let mut tags = Vec::new();
        let text = raw.to_string().to_ascii_lowercase();

        if raw
            .get("vin")
            .and_then(Value::as_array)
            .is_some_and(|inputs| inputs.iter().any(|input| input.get("coinbase").is_some()))
        {
            tags.push(TransactionTag::Coinbase);
        }
        if is_stake_transaction(raw) {
            tags.push(TransactionTag::Stake);
        }
        if private_input_count(raw) > 0 {
            tags.push(TransactionTag::PrivateInput);
        }
        if private_output_count(raw) > 0 {
            tags.push(TransactionTag::PrivateOutput);
        }
        if has_name_commitment_output(raw) {
            tags.push(TransactionTag::NameCommitment);
        }
        if is_identity_registration(raw)
            || text.contains("identityregistration")
            || text.contains("identity registration")
        {
            tags.push(TransactionTag::IdentityRegistration);
        } else if text.contains("identity") {
            tags.push(TransactionTag::IdentityUpdate);
        }
        tags.extend(
            transaction_output_tags(raw)
                .into_iter()
                .map(TransactionTag::Output),
        );

        tags
    }
}

fn is_identity_registration(raw: &Value) -> bool {
    let mut primary = false;
    let mut reservation = false;
    for script in raw
        .get("vout")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|output| output.get("scriptPubKey"))
        .filter_map(Value::as_object)
    {
        primary |= script.contains_key("identityprimary");
        reservation |= script.contains_key("identityreservation")
            || script.contains_key("identityadvancedreservation");
    }
    primary && reservation
}

pub fn transaction_output_tags(raw: &Value) -> Vec<OutputTag> {
    let mut tags = Vec::new();
    for output in raw
        .get("vout")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        for tag in output_tags(output) {
            if !tags.contains(&tag) {
                tags.push(tag);
            }
        }
    }
    if emission_timelock(raw).is_some() {
        let tag = OutputTag {
            kind: OutputTagKind::EmissionTimelock,
            currency_id: None,
        };
        if !tags.contains(&tag) {
            tags.push(tag);
        }
    }
    tags
}

pub fn emission_timelock(raw: &Value) -> Option<EmissionTimelock> {
    let coinbase = raw
        .get("vin")?
        .as_array()?
        .iter()
        .any(|input| input.get("coinbase").is_some());
    if !coinbase {
        return None;
    }

    let outputs = raw.get("vout")?.as_array()?;
    let locked_output = outputs.first()?;
    let metadata_output_index = outputs.len().checked_sub(1)?;
    if metadata_output_index == 0 {
        return None;
    }

    let p2sh = locked_output
        .pointer("/scriptPubKey/hex")
        .and_then(Value::as_str)
        .and_then(decode_hex)
        .and_then(|script| p2sh_script_hash(&script))?;
    let payload = outputs[metadata_output_index]
        .pointer("/scriptPubKey/hex")
        .and_then(Value::as_str)
        .and_then(decode_hex)
        .and_then(|script| unpack_op_return(&script))?;
    let (&kind, redeem_script) = payload.split_first()?;
    if kind != 1 || redeem_script.is_empty() {
        return None;
    }

    let redeem_hash = Ripemd160::digest(Sha256::digest(redeem_script));
    if redeem_hash.as_slice() != p2sh {
        return None;
    }

    Some(EmissionTimelock {
        locked_output_index: 0,
        metadata_output_index,
        unlock_height: check_lock_time_verify_height(redeem_script)?,
    })
}

fn p2sh_script_hash(script: &[u8]) -> Option<[u8; 20]> {
    (script.len() == 23 && script[0] == 0xa9 && script[1] == 0x14 && script[22] == 0x87)
        .then(|| script[2..22].try_into().expect("checked P2SH hash length"))
}

fn check_lock_time_verify_height(script: &[u8]) -> Option<u64> {
    let (encoded_height, next_opcode) = first_script_push(script)?;
    if script.get(next_opcode) != Some(&0xb1) || encoded_height.len() >= 6 {
        return None;
    }
    let mut bytes = [0_u8; 8];
    bytes[..encoded_height.len()].copy_from_slice(&encoded_height);
    Some(u64::from_le_bytes(bytes))
}

fn first_script_push(script: &[u8]) -> Option<(Vec<u8>, usize)> {
    let opcode = *script.first()?;
    let mut cursor = 1;
    let value = match opcode {
        0x00 => Vec::new(),
        0x01..=0x4b => take_script_bytes(script, &mut cursor, usize::from(opcode))?,
        0x4c => {
            let size = usize::from(*script.get(cursor)?);
            cursor += 1;
            take_script_bytes(script, &mut cursor, size)?
        }
        0x4d => {
            let size = read_script_size(script, &mut cursor, 2)?;
            take_script_bytes(script, &mut cursor, size)?
        }
        0x4e => {
            let size = read_script_size(script, &mut cursor, 4)?;
            take_script_bytes(script, &mut cursor, size)?
        }
        0x51..=0x60 => vec![opcode - 0x50],
        _ => return None,
    };
    Some((value, cursor))
}

fn burn_instruction(transfer: &serde_json::Map<String, Value>) -> bool {
    boolean_field(transfer, "burnchangeprice")
        || boolean_field(transfer, "burnchangeweight")
        || transfer
            .get("flags")
            .and_then(Value::as_u64)
            .is_some_and(|flags| flags & 0x180 != 0)
}

// These objects are emitted by the daemon without checking IsValid.
pub(crate) fn finalization_tag(value: &Value, base_type: u64) -> Option<FinalizationTag> {
    let kind = value["finalizationtype"].as_u64()?;
    let currency = value["currencyid"].as_str()?;
    if kind > 255
        || kind & !0xe0 != base_type
        || currency.is_empty()
        || !value["output"].as_object().is_some_and(|o| !o.is_empty())
    {
        return None;
    }
    // A null hash refers to an output in the carrier transaction.
    let earlier = value["output"]["txid"].as_str().is_some_and(|hash| {
        hash.len() == 64
            && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
            && hash.bytes().any(|byte| byte != b'0')
    });
    Some(match kind & 0xc0 {
        0 => FinalizationTag::Pending,
        0x40 => FinalizationTag::Rejects,
        0x80 if earlier => FinalizationTag::ConfirmsEarlier,
        0x80 => FinalizationTag::Confirms,
        _ => FinalizationTag::Unknown,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LegacyNotarization {
    pub block_hash: String,
    pub height: u32,
    pub destination_txid: String,
    pub merkle_root: String,
    pub depth: u16,
}

// The captured Komodo-era VRSC format is a single 109-byte push with two
// uint256 values around a height, then the NUL-terminated chain symbol,
// a Merkle-of-Merkle-roots commitment and packed depth. This decodes the
// commitment and does not validate the historical notaries or destination.
pub(crate) fn legacy_notarization(output: &Value) -> Option<LegacyNotarization> {
    let hex = output.pointer("/scriptPubKey/hex")?.as_str()?;
    if !hex.starts_with("6a") && !hex.starts_with("6A") {
        return None;
    }
    let script = decode_hex(hex)?;
    let data = unpack_op_return(&script)?;
    if data.len() != 109 || &data[68..73] != b"VRSC\0" {
        return None;
    }
    let height = u32::try_from(i32::from_le_bytes(data[32..36].try_into().ok()?)).ok()?;
    let depth = u16::from_le_bytes(data[105..107].try_into().ok()?);
    if depth == 0 || u32::from(depth) > height {
        return None;
    }
    let hash = |bytes: &[u8]| {
        bytes.iter().any(|&byte| byte != 0).then(|| {
            bytes
                .iter()
                .rev()
                .map(|byte| format!("{byte:02x}"))
                .collect()
        })
    };
    Some(LegacyNotarization {
        block_hash: hash(&data[..32])?,
        height,
        destination_txid: hash(&data[36..68])?,
        merkle_root: hash(&data[73..105])?,
        depth,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OpReturnData {
    pub bytes: std::sync::Arc<[u8]>,
    pub script_body: bool,
}

pub(crate) fn arbitrary_op_return(output: &Value) -> Option<OpReturnData> {
    let hex = output.pointer("/scriptPubKey/hex")?.as_str()?;
    if !hex.starts_with("6a") && !hex.starts_with("6A") {
        return None;
    }
    let script = decode_hex(hex)?;
    let body = script.strip_prefix(&[0x6a])?;
    let mut pushes = body;
    let mut bytes = Vec::new();
    while let Some(&opcode) = pushes.first() {
        if opcode == 0x4f {
            bytes.push(0x81);
            pushes = &pushes[1..];
        } else if let Some((data, used)) = first_script_push(pushes) {
            bytes.extend(data);
            pushes = &pushes[used..];
        } else {
            return Some(OpReturnData {
                bytes: body.into(),
                script_body: true,
            });
        }
    }
    Some(OpReturnData {
        bytes: bytes.into(),
        script_body: false,
    })
}

pub fn output_tags(output: &Value) -> Vec<OutputTag> {
    let Some(script) = output.get("scriptPubKey").and_then(Value::as_object) else {
        return Vec::new();
    };
    let mut tags = Vec::new();
    // core_write.cpp emits these objects only after the daemon successfully
    // decodes and validates the corresponding CC payload; invalid data is a string.
    for (key, kind) in [
        ("earnednotarization", OutputTagKind::EarnedNotarization),
        ("acceptednotarization", OutputTagKind::AcceptedNotarization),
        ("notaryevidence", OutputTagKind::NotaryEvidence),
    ] {
        if let Some(payload) = script.get(key).and_then(Value::as_object)
            && !payload.is_empty()
        {
            tags.push(OutputTag {
                kind,
                currency_id: string_field(payload, "currencyid"),
            });
        }
    }
    // Finalization records describe the action on their referenced output.
    for (key, kind, tag) in [
        (
            "finalizenotarization",
            1,
            OutputTagKind::NotarizationFinalization as fn(FinalizationTag) -> OutputTagKind,
        ),
        ("finalizeexport", 2, OutputTagKind::ExportFinalization),
    ] {
        let finalization = &output["scriptPubKey"][key];
        if let Some(status) = finalization_tag(finalization, kind) {
            tags.push(OutputTag {
                kind: tag(status),
                currency_id: finalization["currencyid"].as_str().map(str::to_owned),
            });
        }
    }
    if legacy_notarization(output).is_some() {
        tags.push(OutputTag {
            kind: OutputTagKind::LegacyNotarization,
            currency_id: None,
        });
    }

    if let Some(definition) = script.get("currencydefinition").and_then(Value::as_object) {
        tags.push(OutputTag {
            kind: OutputTagKind::CurrencyDefinition,
            currency_id: string_field(definition, "currencyid"),
        });
    }
    if let Some(transfer) = script.get("reservetransfer").and_then(Value::as_object) {
        tags.push(OutputTag {
            kind: OutputTagKind::ReserveTransfer,
            currency_id: None,
        });
        // CReserveTransfer::IsBurn, decoded by core_write.cpp. These are burn
        // instructions, including cross-system burns executed on import.
        if burn_instruction(transfer) {
            tags.push(OutputTag {
                kind: OutputTagKind::Burn,
                currency_id: None,
            });
        }
        if boolean_field(transfer, "arbitrageonly") {
            tags.push(OutputTag {
                kind: OutputTagKind::AutoArbitrage,
                currency_id: string_field(transfer, "via")
                    .or_else(|| string_field(transfer, "destinationcurrencyid")),
            });
        }
        if boolean_field(transfer, "convert") {
            tags.push(OutputTag {
                kind: OutputTagKind::Conversion,
                currency_id: string_field(transfer, "via")
                    .or_else(|| string_field(transfer, "destinationcurrencyid")),
            });
        }
        if boolean_field(transfer, "crosssystem") {
            tags.push(OutputTag {
                kind: OutputTagKind::CrossChainTransfer,
                currency_id: string_field(transfer, "exportto"),
            });
        }
    }
    if let Some(export) = script.get("crosschainexport").and_then(Value::as_object) {
        // Imported external burn instructions are retained in typed supplemental
        // transfer evidence, rather than a local reserve-transfer UTXO.
        if boolean_field(export, "issupplemental")
            && export
                .get("transfers")
                .and_then(Value::as_array)
                .is_some_and(|transfers| {
                    transfers.iter().any(|transfer| {
                        transfer.as_object().is_some_and(|transfer| {
                            transfer.get("version").and_then(Value::as_u64) == Some(1)
                                && transfer
                                    .get("flags")
                                    .and_then(Value::as_u64)
                                    .is_some_and(|flags| flags & 1 != 0 && flags & 0x180 != 0)
                                && transfer
                                    .get("currencyvalues")
                                    .and_then(Value::as_object)
                                    .is_some_and(|values| !values.is_empty())
                        })
                    })
                })
            && !tags.iter().any(|tag| tag.kind == OutputTagKind::Burn)
        {
            tags.push(OutputTag {
                kind: OutputTagKind::Burn,
                currency_id: None,
            });
        }
        tags.push(OutputTag {
            kind: OutputTagKind::Export,
            currency_id: string_field(export, "destinationcurrencyid"),
        });
    }
    if let Some(import) = script.get("crosschainimport").and_then(Value::as_object) {
        tags.push(OutputTag {
            kind: OutputTagKind::Import,
            currency_id: string_field(import, "importcurrencyid"),
        });
    }
    tags
}

/// Currency references in transactions, hydrated prevouts, balances and state.
pub fn defi_currency_ids(raw: &Value) -> Vec<String> {
    let mut ids = Vec::new();
    collect_currency_ids(raw, &mut ids);
    ids
}

fn collect_currency_ids(value: &Value, ids: &mut Vec<String>) {
    match value {
        Value::Object(fields) => {
            for (key, value) in fields {
                let key_lower = key.to_ascii_lowercase();
                if key_lower.ends_with("currencyid")
                    || key_lower.ends_with("systemid")
                    || matches!(key_lower.as_str(), "via" | "exportto")
                {
                    if let Some(id) = value.as_str()
                        && id.starts_with('i')
                        && !ids.iter().any(|known| known == id)
                    {
                        ids.push(id.to_string());
                    }
                } else if matches!(
                    key_lower.as_str(),
                    "currencyvalues"
                        | "reservevalues"
                        | "reserveamounts"
                        | "currencybalance"
                        | "currencyreceived"
                        | "currencynames"
                        | "totalamounts"
                        | "totalfees"
                        | "totalburned"
                        | "valuein"
                        | "tokensout"
                ) {
                    if let Some(amounts) = value.as_object() {
                        for id in amounts.keys().filter(|id| id.starts_with('i')) {
                            if !ids.iter().any(|known| known == id) {
                                ids.push(id.clone());
                            }
                        }
                    }
                } else if key_lower == "currencies" && value.is_object() {
                    for id in value
                        .as_object()
                        .unwrap()
                        .keys()
                        .filter(|id| id.starts_with('i'))
                    {
                        if !ids.contains(id) {
                            ids.push(id.clone());
                        }
                    }
                } else if key_lower == "currencies"
                    && let Some(currencies) = value.as_array()
                {
                    for id in currencies
                        .iter()
                        .filter_map(Value::as_str)
                        .filter(|id| id.starts_with('i'))
                    {
                        if !ids.iter().any(|known| known == id) {
                            ids.push(id.to_string());
                        }
                    }
                }
                collect_currency_ids(value, ids);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_currency_ids(value, ids);
            }
        }
        _ => {}
    }
}

fn string_field(fields: &serde_json::Map<String, Value>, key: &str) -> Option<String> {
    fields.get(key).and_then(Value::as_str).map(str::to_string)
}

fn boolean_field(fields: &serde_json::Map<String, Value>, key: &str) -> bool {
    fields.get(key).and_then(Value::as_bool).unwrap_or(false)
}

fn has_name_commitment_output(raw: &Value) -> bool {
    raw.get("vout")
        .and_then(Value::as_array)
        .is_some_and(|outputs| {
            outputs.iter().any(|output| {
                output
                    .get("scriptPubKey")
                    .and_then(Value::as_object)
                    .and_then(|script| script.get("commitmenthash"))
                    .is_some_and(|commitment| match commitment {
                        Value::Object(fields) => fields
                            .get("hash")
                            .and_then(Value::as_str)
                            .is_none_or(|hash| hash.chars().any(|character| character != '0')),
                        Value::String(hash) => {
                            !hash.is_empty() && hash.chars().any(|character| character != '0')
                        }
                        _ => false,
                    })
            })
        })
}

pub fn private_input_count(raw: &Value) -> usize {
    case_insensitive_array(raw, "vshieldedspend").map_or(0, Vec::len)
        + joinsplit_component_count(raw, "nullifiers")
}

pub fn private_output_count(raw: &Value) -> usize {
    case_insensitive_array(raw, "vshieldedoutput").map_or(0, Vec::len)
        + joinsplit_component_count(raw, "commitments")
}

fn joinsplit_component_count(raw: &Value, component: &str) -> usize {
    case_insensitive_array(raw, "vjoinsplit").map_or(0, |joinsplits| {
        joinsplits
            .iter()
            .map(|joinsplit| {
                case_insensitive_array(joinsplit, component)
                    .map(Vec::len)
                    .unwrap_or(1)
            })
            .sum()
    })
}

fn case_insensitive_array<'a>(value: &'a Value, key: &str) -> Option<&'a Vec<Value>> {
    value.as_object()?.iter().find_map(|(candidate, value)| {
        candidate
            .eq_ignore_ascii_case(key)
            .then(|| value.as_array())
            .flatten()
    })
}

pub(crate) fn is_stake_transaction(raw: &Value) -> bool {
    let has_coinbase_input = raw
        .get("vin")
        .and_then(Value::as_array)
        .is_some_and(|inputs| inputs.iter().any(|input| input.get("coinbase").is_some()));
    if has_coinbase_input {
        return false;
    }

    let Some(inputs) = raw.get("vin").and_then(Value::as_array) else {
        return false;
    };
    let Some(outputs) = raw.get("vout").and_then(Value::as_array) else {
        return false;
    };
    if inputs.len() != 1 || outputs.len() != 2 || !has_positive_native_value(&outputs[0]) {
        return false;
    }

    outputs[1]
        .pointer("/scriptPubKey/hex")
        .and_then(Value::as_str)
        .and_then(decode_hex)
        .and_then(|script| unpack_op_return(&script))
        .is_some_and(|inner_script| {
            parse_script_data(&inner_script).is_some_and(|params| valid_stake_params(&params))
        })
}

fn has_positive_native_value(output: &Value) -> bool {
    ["valueZat", "valueSat", "satoshis"]
        .iter()
        .find_map(|key| output.get(key))
        .is_some_and(|value| {
            value.as_i64().is_some_and(|value| value > 0)
                || value.as_u64().is_some_and(|value| value > 0)
        })
        || output
            .get("value")
            .and_then(Value::as_f64)
            .is_some_and(|value| value > 0.0)
}

pub(crate) fn decode_hex(value: &str) -> Option<Vec<u8>> {
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

fn unpack_op_return(script: &[u8]) -> Option<Vec<u8>> {
    let (&0x6a, remainder) = script.split_first()? else {
        return None;
    };
    let pushes = parse_script_data(remainder)?;
    (pushes.len() == 1)
        .then(|| pushes.into_iter().next())
        .flatten()
}

fn parse_script_data(script: &[u8]) -> Option<Vec<Vec<u8>>> {
    let mut cursor = 0;
    let mut values = Vec::new();
    while cursor < script.len() {
        let opcode = *script.get(cursor)?;
        cursor += 1;
        let value = match opcode {
            0x00 => vec![0],
            0x01..=0x4b => take_script_bytes(script, &mut cursor, usize::from(opcode))?,
            0x4c => {
                let size = usize::from(*script.get(cursor)?);
                cursor += 1;
                take_script_bytes(script, &mut cursor, size)?
            }
            0x4d => {
                let size = read_script_size(script, &mut cursor, 2)?;
                take_script_bytes(script, &mut cursor, size)?
            }
            0x4e => {
                let size = read_script_size(script, &mut cursor, 4)?;
                take_script_bytes(script, &mut cursor, size)?
            }
            0x51..=0x60 => vec![opcode - 0x50],
            _ => return None,
        };
        values.push(value);
    }
    (!values.is_empty()).then_some(values)
}

fn read_script_size(script: &[u8], cursor: &mut usize, bytes: usize) -> Option<usize> {
    let value = script.get(*cursor..cursor.checked_add(bytes)?)?;
    *cursor += bytes;
    let mut padded = [0_u8; 8];
    padded[..bytes].copy_from_slice(value);
    usize::try_from(u64::from_le_bytes(padded)).ok()
}

fn take_script_bytes(script: &[u8], cursor: &mut usize, size: usize) -> Option<Vec<u8>> {
    let end = cursor.checked_add(size)?;
    let value = script.get(*cursor..end)?.to_vec();
    *cursor = end;
    Some(value)
}

fn valid_stake_params(params: &[Vec<u8>]) -> bool {
    match params {
        [kind, serialized] if kind.as_slice() == [6] => valid_extended_stake_params(serialized),
        [kind, source_height, target_height, previous_hash]
            if kind.as_slice() == [2]
                && source_height.len() <= 4
                && target_height.len() <= 4
                && previous_hash.len() == 32 =>
        {
            little_endian_u32(source_height).is_some_and(|height| height != 0)
        }
        [
            kind,
            source_height,
            target_height,
            previous_hash,
            public_key,
        ] if kind.as_slice() == [2]
            && source_height.len() <= 4
            && target_height.len() <= 4
            && previous_hash.len() == 32
            && public_key.len() == 33
            && matches!(public_key.first(), Some(2 | 3)) =>
        {
            little_endian_u32(source_height).is_some_and(|height| height != 0)
        }
        _ => false,
    }
}

fn valid_extended_stake_params(serialized: &[u8]) -> bool {
    let Some(version) = serialized.get(0..4).and_then(little_endian_u32) else {
        return false;
    };
    let Some(source_height) = serialized.get(4..8).and_then(little_endian_u32) else {
        return false;
    };
    if !(1..=2).contains(&version) || source_height == 0 || serialized.len() < 44 {
        return false;
    }

    deserialize_transfer_destination(&serialized[44..]).is_some()
}

fn little_endian_u32(value: &[u8]) -> Option<u32> {
    if value.len() > 4 {
        return None;
    }
    let mut bytes = [0_u8; 4];
    bytes[..value.len()].copy_from_slice(value);
    Some(u32::from_le_bytes(bytes))
}

fn deserialize_transfer_destination(value: &[u8]) -> Option<()> {
    let (&destination_type, mut remainder) = value.split_first()?;
    let destination_size = read_compact_size(&mut remainder)?;
    remainder = remainder.get(destination_size..)?;
    if destination_type & 0x80 != 0 {
        remainder = remainder.get(48..)?;
    }
    if destination_type & 0x40 != 0 {
        let count = read_compact_size(&mut remainder)?;
        for _ in 0..count {
            let size = read_compact_size(&mut remainder)?;
            remainder = remainder.get(size..)?;
        }
    }
    Some(())
}

fn read_compact_size(value: &mut &[u8]) -> Option<usize> {
    let (&prefix, remainder) = value.split_first()?;
    *value = remainder;
    let (size, encoded_bytes, minimum) = match prefix {
        0..=252 => return Some(usize::from(prefix)),
        253 => (read_little_endian(value, 2)?, 2, 253),
        254 => (read_little_endian(value, 4)?, 4, 65_536),
        255 => (read_little_endian(value, 8)?, 8, 4_294_967_296),
    };
    if size < minimum {
        return None;
    }
    if size > 0x0200_0000 {
        return None;
    }
    *value = value.get(encoded_bytes..)?;
    usize::try_from(size).ok()
}

fn read_little_endian(value: &[u8], bytes: usize) -> Option<u64> {
    let value = value.get(..bytes)?;
    let mut padded = [0_u8; 8];
    padded[..bytes].copy_from_slice(value);
    Some(u64::from_le_bytes(padded))
}

impl<'de> Deserialize<'de> for TransactionSummary {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        match value {
            Value::String(txid) => Ok(Self { txid, raw: None }),
            Value::Object(map) => {
                let txid = map
                    .get("txid")
                    .and_then(Value::as_str)
                    .ok_or_else(|| de::Error::missing_field("txid"))?
                    .to_string();
                Ok(Self {
                    txid,
                    raw: Some(Value::Object(map)),
                })
            }
            _ => Err(de::Error::custom(
                "transaction must be a txid string or object",
            )),
        }
    }
}

impl ObjectRoute {
    pub fn label(&self) -> String {
        match self {
            Self::Tip => "chain tip".to_string(),
            Self::BlockHeight(height) => format!("block height {height}"),
            Self::BlockTime(query) => format!("block near {query}"),
            Self::Hash(hash) => format!("hash {hash}"),
            Self::Address(address) => format!("address {address}"),
            Self::Identity(identity) => format!("identity {identity}"),
            Self::Unknown(value) => format!("unresolved object {value}"),
            Self::Estimate(value) => format!("future estimate {value}"),
        }
    }
}

#[cfg(test)]
mod defi_tests {
    use super::{
        OutputTagKind, TransactionSummary, defi_currency_ids, emission_timelock, output_tags,
        transaction_output_tags,
    };
    use serde_json::json;

    #[test]
    fn captured_legacy_notarization_and_export_finalization_have_distinct_output_tags() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../tests/fixtures/output-presentation.json"))
                .unwrap();
        let legacy = &fixture["legacy"];
        let decoded = super::legacy_notarization(&legacy["vout"][1]).unwrap();
        assert_eq!(
            decoded.height as u64,
            legacy["notarized_height"].as_u64().unwrap()
        );
        assert_eq!(decoded.block_hash, legacy["notarized_hash"]);
        assert_eq!(decoded.destination_txid, legacy["destination_txid"]);
        assert_eq!(
            output_tags(&legacy["vout"][1])[0].kind,
            OutputTagKind::LegacyNotarization
        );

        let mut invalid = legacy["vout"][1].clone();
        let script = invalid["scriptPubKey"]["hex"].as_str().unwrap().to_string() + "00";
        invalid["scriptPubKey"]["hex"] = json!(script);
        assert!(super::legacy_notarization(&invalid).is_none());

        let export = &fixture["export"];
        let tags = TransactionSummary {
            txid: export["txid"].as_str().unwrap().into(),
            raw: Some(export.clone()),
        }
        .tags()
        .iter()
        .map(super::TransactionTag::label)
        .collect::<Vec<_>>();
        assert!(tags.contains(&"Pending Export Finalization"), "{tags:?}");
        let observing = crate::world::system_id("VRSC", crate::world::Network::Mainnet).unwrap();
        let links = crate::relationships::transaction_links(&observing, export);
        let link = links
            .iter()
            .find(|link| link.field == "finalizeexport.output")
            .unwrap();
        assert_eq!(link.carrier_vout, 2);
        assert_eq!(link.target.vout, 1);
        assert_eq!(link.target.system_id, observing);
        assert_eq!(link.target.txid, export["txid"]);
        assert_eq!(
            crate::relationships::target_status(link, &observing, Some(export)),
            "Both sides resolved"
        );
    }

    #[test]
    fn arbitrary_op_return_preserves_binary_data_and_falls_back_to_exact_script_bytes() {
        for (hex, expected, script_body) in [
            ("6a0241ff4c010a", &[0x41, 0xff, 0x0a][..], false),
            ("6a", &[][..], false),
            ("6a00", &[][..], false),
            ("6a0241", &[0x02, 0x41][..], true),
            ("6a6c", &[0x6c][..], true),
        ] {
            let output = json!({"scriptPubKey":{"hex":hex}});
            let data = super::arbitrary_op_return(&output).unwrap();
            assert_eq!(data.bytes.as_ref(), expected);
            assert_eq!(data.script_body, script_body);
        }
        assert!(super::arbitrary_op_return(&json!({"scriptPubKey":{"hex":"51"}})).is_none());
    }

    #[test]
    fn notarization_tags_come_from_outputs_and_compose_without_duplicates() {
        let raw = json!({"vout":[
            {"scriptPubKey":{"earnednotarization":{"version":1,"currencyid":"iChain","notarizationheight":100}}},
            {"scriptPubKey":{"acceptednotarization":{"version":1,"currencyid":"iChain","notarizationheight":99}}},
            {"scriptPubKey":{"notaryevidence":{"version":1,"type":1}}},
            {"scriptPubKey":{"finalizenotarization":{"finalizationtype":129,"currencyid":"iChain","output":{"txid":"abc","voutnum":0}}}},
            {"scriptPubKey":{"earnednotarization":{"version":1,"currencyid":"iChain"}}}
        ]});
        let tags = TransactionSummary {
            txid: "test".into(),
            raw: Some(raw.clone()),
        }
        .tags()
        .iter()
        .map(super::TransactionTag::label)
        .collect::<Vec<_>>();
        assert_eq!(
            tags,
            vec![
                "Earned Notarization",
                "Accepted Notarization",
                "Notary Evidence",
                "Confirms Referenced Notarization"
            ]
        );
        assert_eq!(
            output_tags(&raw["vout"][0])[0].kind,
            OutputTagKind::EarnedNotarization
        );
        assert_eq!(
            output_tags(&raw["vout"][1])[0].kind,
            OutputTagKind::AcceptedNotarization
        );
    }

    #[test]
    fn pending_notarization_and_confirmation_of_earlier_output_are_distinct() {
        let txid = "a".repeat(64);
        let raw = json!({"txid": txid, "vout":[
            {"scriptPubKey":{"acceptednotarization":{"version":1}}},
            {"scriptPubKey":{"finalizenotarization":{"finalizationtype":1,"currencyid":"i5w5MuNik5NtLcYmNzcvaoixooEebB6MGV","output":{"txid":"0".repeat(64),"voutnum":0}}}},
            {"scriptPubKey":{"finalizenotarization":{"finalizationtype":129,"currencyid":"i5w5MuNik5NtLcYmNzcvaoixooEebB6MGV","output":{"txid":"b".repeat(64),"voutnum":0}}}}
        ]});
        let tags = TransactionSummary {
            txid,
            raw: Some(raw),
        }
        .tags()
        .iter()
        .map(super::TransactionTag::label)
        .collect::<Vec<_>>();
        assert!(
            tags.contains(&"Pending Notarization Finalization"),
            "{tags:?}"
        );
        assert!(tags.contains(&"Confirms Earlier Notarization"), "{tags:?}");
    }

    #[test]
    fn notarization_mentions_and_invalid_payloads_are_not_notarizations() {
        for raw in [
            json!({"memo":"earnednotarization", "vout":[{"scriptPubKey":{"currencydefinition":{"notarizationprotocol":1}}}]}),
            json!({"vout":[{"scriptPubKey":{"pbaasnotarization":"invalid","earnednotarization":"invalid","acceptednotarization":{},"notaryevidence":"invalid"}}]}),
            json!({"vout":[{"scriptPubKey":{"finalizenotarization":{"finalizationtype":130,"currencyid":"iChain","output":{"txid":"abc","voutnum":0}}}}]}),
            json!({"vout":[{"scriptPubKey":{"finalizenotarization":{"finalizationtype":0,"output":{}}}}]}),
        ] {
            let tags = TransactionSummary {
                txid: "test".into(),
                raw: Some(raw),
            }
            .tags()
            .iter()
            .map(super::TransactionTag::label)
            .collect::<Vec<_>>();
            assert!(
                !tags.iter().any(|tag| tag.to_lowercase().contains("notar")),
                "{tags:?}"
            );
        }
        for kind in [1, 33, 65, 97, 129, 161, 193, 225] {
            let output = json!({"scriptPubKey":{"finalizenotarization":{"finalizationtype":kind,"currencyid":"iChain","output":{"txid":"abc","voutnum":0}}}});
            assert!(matches!(
                output_tags(&output)[0].kind,
                OutputTagKind::NotarizationFinalization(_)
            ));
        }
        for kind in [2, 9, 17, 25] {
            let output = json!({"scriptPubKey":{"finalizenotarization":{"finalizationtype":kind,"currencyid":"iChain","output":{"txid":"abc","voutnum":0}}}});
            assert!(output_tags(&output).is_empty());
        }
    }

    #[test]
    fn reserve_transfer_tags_are_structural_and_composable() {
        let output = json!({
            "scriptPubKey": {
                "reservetransfer": {
                    "currencyvalues": { "iSource": 1.0 },
                    "convert": true,
                    "crosssystem": true,
                    "destinationcurrencyid": "iDestination",
                    "via": "iPool",
                    "exportto": "iRemote"
                }
            }
        });

        let tags = output_tags(&output);
        assert_eq!(tags.len(), 3);
        assert_eq!(tags[0].kind, OutputTagKind::ReserveTransfer);
        assert_eq!(tags[1].kind, OutputTagKind::Conversion);
        assert_eq!(tags[1].currency_id.as_deref(), Some("iPool"));
        assert_eq!(tags[2].kind, OutputTagKind::CrossChainTransfer);
    }

    #[test]
    fn arbitrage_only_reserve_transfer_has_a_distinct_tag() {
        let output = json!({
            "scriptPubKey": {
                "reservetransfer": {
                    "currencyvalues": { "iSource": 1.0 },
                    "convert": true,
                    "arbitrageonly": true,
                    "destinationcurrencyid": "iPool"
                }
            }
        });

        let tags = output_tags(&output);
        assert_eq!(tags[0].kind, OutputTagKind::ReserveTransfer);
        assert_eq!(tags[1].kind, OutputTagKind::AutoArbitrage);
        assert_eq!(tags[1].currency_id.as_deref(), Some("iPool"));
        assert_eq!(tags[2].kind, OutputTagKind::Conversion);
    }

    #[test]
    fn import_export_definition_tags_and_currency_references_are_detected() {
        let transaction = json!({
            "vout": [
                { "scriptPubKey": { "currencydefinition": {
                    "currencyid": "iPool", "systemid": "iSystem",
                    "currencies": ["iReserve"]
                }}},
                { "scriptPubKey": { "crosschainexport": {
                    "sourcesystemid": "iSystem", "destinationsystemid": "iRemote",
                    "destinationcurrencyid": "iPool"
                }}},
                { "scriptPubKey": { "crosschainimport": {
                    "sourcesystemid": "iRemote", "importcurrencyid": "iPool",
                    "valuein": { "iReserve": 2.0 }
                }}}
            ]
        });

        let tags = transaction_output_tags(&transaction);
        assert_eq!(tags[0].kind, OutputTagKind::CurrencyDefinition);
        assert_eq!(tags[1].kind, OutputTagKind::Export);
        assert_eq!(tags[2].kind, OutputTagKind::Import);
        let mut ids = defi_currency_ids(&transaction);
        ids.sort();
        assert_eq!(ids, vec!["iPool", "iRemote", "iReserve", "iSystem"]);
    }

    #[test]
    fn routine_stakeguard_protection_is_not_a_visible_output_tag() {
        let output = json!({
            "scriptPubKey": {
                "type": "cryptocondition",
                "stakeguard": "",
                "addresses": ["RStaker", "RCG8KwJNDVwpUBcdoa6AoHqHVJsA1uMYMR"]
            }
        });

        assert!(output_tags(&output).is_empty());
    }

    #[test]
    fn vrsc_emission_timelock_matches_redeem_script_and_decodes_height() {
        let transaction = json!({
            "txid": "9a43ee09493a99a9733166c1803a77185575feee0b575872ff54a0b8cd10c2ef",
            "vin": [{ "coinbase": "02c07b0101" }],
            "vout": [
                { "n": 0, "scriptPubKey": {
                    "type": "scripthash",
                    "hex": "a914c211e525380231b5dd364d4fed795af49281ec7c87"
                }},
                { "n": 1, "scriptPubKey": {
                    "type": "nulldata",
                    "hex": "6a200103cf610fb17576a914630bcfd6abe8125b6a9aeeb4fde7f8801c7a6de688ac"
                }}
            ]
        });

        let timelock = emission_timelock(&transaction).expect("valid emission timelock");
        assert_eq!(timelock.locked_output_index, 0);
        assert_eq!(timelock.metadata_output_index, 1);
        assert_eq!(timelock.unlock_height, 1_008_079);
        assert!(
            transaction_output_tags(&transaction)
                .iter()
                .any(|tag| tag.kind == OutputTagKind::EmissionTimelock)
        );
        let summary = TransactionSummary {
            txid: transaction["txid"].as_str().unwrap().to_string(),
            raw: Some(transaction),
        };
        assert!(
            summary
                .tags()
                .iter()
                .any(|tag| tag.label() == "Emission Timelock")
        );
    }

    #[test]
    fn emission_timelock_rejects_a_redeem_script_hash_mismatch() {
        let transaction = json!({
            "vin": [{ "coinbase": "00" }],
            "vout": [
                { "scriptPubKey": {
                    "hex": "a914000000000000000000000000000000000000000087"
                }},
                { "scriptPubKey": {
                    "hex": "6a200103cf610fb17576a914630bcfd6abe8125b6a9aeeb4fde7f8801c7a6de688ac"
                }}
            ]
        });

        assert_eq!(emission_timelock(&transaction), None);
    }

    #[test]
    fn identity_registration_and_marketplace_commitments_do_not_collide() {
        let registration = TransactionSummary {
            txid: "registration".to_string(),
            raw: Some(json!({
                "vout": [
                    { "scriptPubKey": { "identityprimary": {} } },
                    { "scriptPubKey": { "identityreservation": {} } }
                ]
            })),
        };
        let marketplace_funding = TransactionSummary {
            txid: "offer".to_string(),
            raw: Some(json!({
                "vout": [{ "scriptPubKey": { "commitmenthash": {
                    "hash": "0000000000000000000000000000000000000000000000000000000000000000"
                }}}]
            })),
        };

        assert!(
            registration
                .tags()
                .contains(&super::TransactionTag::IdentityRegistration)
        );
        assert!(
            !marketplace_funding
                .tags()
                .contains(&super::TransactionTag::NameCommitment)
        );
    }

    #[test]
    fn pre_march_2026_conversion_chain_fixtures_classify_each_step() {
        let conversion = json!({
            "txid": "a08e52324e8a1ab0825edc9759cf5c8ec2f3b18b297271162c67d3cd5561746a",
            "height": 3962160,
            "vout": [{ "scriptPubKey": { "reservetransfer": {
                "currencyvalues": { "i5w5MuNik5NtLcYmNzcvaoixooEebB6MGV": 10.0 },
                "convert": true,
                "reservetoreserve": true,
                "destinationcurrencyid": "i9nwxtKuVYX4MSbeULLiK2ttVi6rUEhh4X",
                "via": "iFrFn9b6ctse7XBzcWkRbpYMAHoKjbYKqG"
            }}}]
        });
        let export = json!({
            "txid": "eba1853c46250459b5a1e8f286eda3bd62fcda47c8a522d17048b4be9ac15d84",
            "height": 3962166,
            "vout": [{ "scriptPubKey": { "crosschainexport": {
                "destinationcurrencyid": "i3f7tSctFkiPpiedY8QR5Tep9p4qDVebDx"
            }}}]
        });
        let import_and_next_leg = json!({
            "txid": "24d6d1dc58aa4b1e12659853a2d5e01d990b923461bb5ffab28954ae5488d554",
            "height": 3962166,
            "vout": [
                { "scriptPubKey": { "crosschainimport": {
                    "importcurrencyid": "i3f7tSctFkiPpiedY8QR5Tep9p4qDVebDx",
                    "exporttxid": "eba1853c46250459b5a1e8f286eda3bd62fcda47c8a522d17048b4be9ac15d84",
                    "exporttxout": 1
                }}},
                { "scriptPubKey": { "reservetransfer": {
                    "crosssystem": true,
                    "exportto": "i9nwxtKuVYX4MSbeULLiK2ttVi6rUEhh4X",
                    "destinationcurrencyid": "i9nwxtKuVYX4MSbeULLiK2ttVi6rUEhh4X"
                }}}
            ]
        });

        assert_eq!(
            transaction_output_tags(&conversion)
                .iter()
                .map(|tag| tag.kind)
                .collect::<Vec<_>>(),
            vec![OutputTagKind::ReserveTransfer, OutputTagKind::Conversion]
        );
        assert_eq!(
            transaction_output_tags(&export)[0].kind,
            OutputTagKind::Export
        );
        assert_eq!(
            transaction_output_tags(&import_and_next_leg)
                .iter()
                .map(|tag| tag.kind)
                .collect::<Vec<_>>(),
            vec![
                OutputTagKind::Import,
                OutputTagKind::ReserveTransfer,
                OutputTagKind::CrossChainTransfer
            ]
        );
    }
}
