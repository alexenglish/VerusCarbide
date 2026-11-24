//! Limited readers for the confirmed transparent reward evidence.
use super::*;
use sha2::{Digest, Sha256};

pub(super) use crate::domain::decode_hex as hex;

pub(super) fn display_hash(bytes: &[u8]) -> String {
    bytes.iter().rev().map(|b| format!("{b:02x}")).collect()
}

#[derive(Clone, Copy)]
pub(super) struct Wire<'a>(pub &'a [u8]);

impl<'a> Wire<'a> {
    pub fn bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        let (head, tail) = self.0.split_at_checked(n)?;
        self.0 = tail;
        Some(head)
    }
    pub fn integer<const N: usize>(&mut self) -> Option<[u8; N]> {
        self.bytes(N)?.try_into().ok()
    }
    pub fn count(&mut self) -> Option<usize> {
        let lead = self.integer::<1>()?[0];
        let (value, min) = match lead {
            253 => (u64::from(u16::from_le_bytes(self.integer()?)), 253),
            254 => (u64::from(u32::from_le_bytes(self.integer()?)), 65536),
            255 => (u64::from_le_bytes(self.integer()?), 4_294_967_296),
            n => (u64::from(n), 0),
        };
        (value >= min)
            .then(|| usize::try_from(value).ok())
            .flatten()
    }
    pub fn vector(&mut self) -> Option<&'a [u8]> {
        let len = self.count()?;
        self.bytes(len)
    }
    pub fn varint(&mut self) -> Option<u64> {
        let mut number = 0u64;
        for _ in 0..10 {
            let byte = self.integer::<1>()?[0];
            number = number
                .checked_mul(128)?
                .checked_add(u64::from(byte & 127))?;
            if byte & 128 == 0 {
                return Some(number);
            }
            number = number.checked_add(1)?;
        }
        None
    }
    pub fn array(&mut self, width: usize) -> Option<&'a [u8]> {
        let count = self.count()?;
        self.bytes(count.checked_mul(width)?)
    }
    fn push(&mut self) -> Option<&'a [u8]> {
        let size = match self.integer::<1>()?[0] {
            n @ 1..=75 => n.into(),
            76 => self.integer::<1>()?[0].into(),
            77 => usize::from(u16::from_le_bytes(self.integer()?)),
            _ => return None,
        };
        self.bytes(size)
    }
}

pub(super) fn checked_transaction(tx: &Value, id: &str) -> Option<()> {
    if !valid_txid(id) || tx["txid"].as_str() != Some(id) || tx["confirmations"].as_i64()? <= 0 {
        return None;
    }
    let bytes = hex(tx["hex"].as_str()?)?;
    if display_hash(&Sha256::digest(Sha256::digest(&bytes))) != id {
        return None;
    }
    let mut raw = Wire(&bytes);
    if raw.bytes(8)? != [4, 0, 0, 128, 133, 32, 47, 137] {
        return None;
    }
    let inputs = tx["vin"].as_array()?;
    if raw.count()? != inputs.len() {
        return None;
    }
    for input in inputs {
        let hash = raw.bytes(32)?;
        let index = u32::from_le_bytes(raw.integer()?);
        let script = raw.vector()?;
        if let Some(coinbase) = input["coinbase"].as_str() {
            if hash != [0; 32] || index != u32::MAX || hex(coinbase)? != script {
                return None;
            }
        } else if input["txid"].as_str()? != display_hash(hash)
            || input["vout"].as_u64()? != u64::from(index)
        {
            return None;
        }
        raw.bytes(4)?;
    }
    let outputs = tx["vout"].as_array()?;
    if raw.count()? != outputs.len() {
        return None;
    }
    for (index, out) in outputs.iter().enumerate() {
        let value = i128::from(i64::from_le_bytes(raw.integer()?));
        if native(out) != Some(value)
            || out["n"].as_u64()? != index as u64
            || hex(out["scriptPubKey"]["hex"].as_str()?)? != raw.vector()?
        {
            return None;
        }
    }
    Some(())
}

pub(super) fn cc_payload(script: &[u8], eval: u8) -> Option<&[u8]> {
    let mut script = Wire(script);
    script.push()?;
    if script.bytes(1)? != [0xcc] {
        return None;
    }
    let mut params = Wire(script.push()?);
    if script.0 != [0x75] {
        return None;
    }
    let &[3, code, required, keys] = params.push()? else {
        return None;
    };
    if code != eval || required > keys || keys > 4 {
        return None;
    }
    for _ in 0..keys {
        params.push()?;
    }
    params.push()
}

pub(super) fn raw_id(id: &str) -> Option<[u8; 20]> {
    let (102, bytes) = address_payload(id)? else {
        return None;
    };
    bytes.try_into().ok()
}

#[derive(PartialEq, Eq)]
pub(super) struct Destination {
    pub kind: u8,
    pub key: [u8; 20],
}

impl Destination {
    pub fn decode(raw: &mut Wire<'_>) -> Option<Self> {
        let kind = raw.integer::<1>()?[0];
        Self::with_kind(kind, raw)
    }
    pub fn with_kind(kind: u8, raw: &mut Wire<'_>) -> Option<Self> {
        if !matches!(kind, 2 | 4) {
            return None;
        }
        Some(Self {
            kind,
            key: raw.vector()?.try_into().ok()?,
        })
    }
    pub fn from_json(address: &str) -> Option<Self> {
        let (prefix, key) = address_payload(address)?;
        let kind = match prefix {
            60 | 0 => 2,
            102 => 4,
            _ => return None,
        };
        Some(Self {
            kind,
            key: key.try_into().ok()?,
        })
    }
    pub fn script(&self) -> Vec<u8> {
        let mut script = if self.kind == 2 {
            vec![0x76, 0xa9, 0x14]
        } else {
            vec![0x05, 4, 3, 0, 0, 0, 0xcc, 0x1b, 4, 3, 0, 1, 1, 0x15, 4]
        };
        script.extend(self.key);
        script.extend(if self.kind == 2 {
            &[0x88, 0xac][..]
        } else {
            &[0x75]
        });
        script
    }
}

pub(super) fn native(out: &Value) -> Option<i128> {
    out.get("valueSat").map_or_else(
        || amount_sats(&out["value"]),
        |value| value.as_i64().map(i128::from),
    )
}
