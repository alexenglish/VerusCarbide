//! Verus network serialization. These projections are independent of RPC JSON.
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub(super) const MAX_BODY: usize = 2_000_000;

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, size: usize) -> Result<&'a [u8]> {
        let (value, rest) = self
            .0
            .split_at_checked(size)
            .context("truncated Verus payload")?;
        self.0 = rest;
        Ok(value)
    }

    fn number<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self.take(N)?.try_into()?)
    }

    fn compact(&mut self, minimum_size: usize) -> Result<usize> {
        let count = match self.take(1)?[0] {
            253 => {
                let value = u16::from_le_bytes(self.number()?);
                ensure!(value >= 253, "noncanonical CompactSize");
                u64::from(value)
            }
            254 => {
                let value = u32::from_le_bytes(self.number()?);
                ensure!(value > u32::from(u16::MAX), "noncanonical CompactSize");
                u64::from(value)
            }
            255 => {
                let value = u64::from_le_bytes(self.number()?);
                ensure!(value > u64::from(u32::MAX), "noncanonical CompactSize");
                value
            }
            value => value.into(),
        };
        ensure!(
            count <= (self.0.len() / minimum_size) as u64,
            "invalid vector length"
        );
        Ok(count as usize)
    }

    fn bytes(&mut self) -> Result<&'a [u8]> {
        let size = self.compact(1)?;
        self.take(size)
    }
}

pub(super) fn hex(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(bytes.len() * 2);
    const DIGITS: &[u8] = b"0123456789abcdef";
    for byte in bytes {
        result.push(DIGITS[usize::from(byte >> 4)] as char);
        result.push(DIGITS[usize::from(byte & 15)] as char);
    }
    result
}

pub(super) fn hash_hex(mut hash: [u8; 32]) -> String {
    hash.reverse();
    hex(&hash)
}

fn sha256d(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(Sha256::digest(bytes)).into()
}

fn coins(amount: i64) -> f64 {
    amount as f64 / 100_000_000.0
}

fn script_json(script: &[u8], testnet: bool) -> Value {
    let (kind, address) = match script {
        [0x76, 0xa9, 20, hash @ .., 0x88, 0xac] if hash.len() == 20 => (
            "pubkeyhash",
            Some(crate::world::base58_check(
                if testnet { 0 } else { 60 },
                hash,
            )),
        ),
        [0xa9, 20, hash @ .., 0x87] if hash.len() == 20 => (
            "scripthash",
            Some(crate::world::base58_check(
                if testnet { 5 } else { 85 },
                hash,
            )),
        ),
        [size @ (33 | 65), key @ .., 0xac] if usize::from(*size) == key.len() => {
            use ripemd::Ripemd160;
            let hash = Ripemd160::digest(Sha256::digest(key));
            (
                "pubkey",
                Some(crate::world::base58_check(
                    if testnet { 0 } else { 60 },
                    &hash,
                )),
            )
        }
        [0x6a, ..] => ("nulldata", None),
        _ => ("nonstandard", None),
    };
    let mut value = json!({"hex": hex(script), "type": kind});
    if let Some(address) = address {
        value["addresses"] = json!([address]);
        value["reqSigs"] = json!(1);
    }
    value
}

fn read_transaction(reader: &mut Reader<'_>, testnet: bool) -> Result<([u8; 32], Value)> {
    let start = reader.0;
    let word = u32::from_le_bytes(reader.number()?);
    let version = word & 0x7fff_ffff;
    let overwintered = word != version;
    let group = if overwintered {
        Some(u32::from_le_bytes(reader.number()?))
    } else {
        None
    };
    ensure!(
        matches!(
            (version, group),
            (1 | 2, None) | (3, Some(0x03c4_8270)) | (4, Some(0x892f_2085))
        ),
        "unsupported Verus transaction version/group"
    );
    let mut inputs = Vec::new();
    for _ in 0..reader.compact(41)? {
        let hash = reader.number::<32>()?;
        let index = u32::from_le_bytes(reader.number()?);
        let script = hex(reader.bytes()?);
        let sequence = u32::from_le_bytes(reader.number()?);
        inputs.push(if hash == [0; 32] && index == u32::MAX {
            json!({"coinbase": script, "sequence": sequence})
        } else {
            json!({"txid": hash_hex(hash), "vout": index, "scriptSig": {"hex": script}, "sequence": sequence})
        });
    }
    let mut outputs = Vec::new();
    for index in 0..reader.compact(9)? {
        let amount = i64::from_le_bytes(reader.number()?);
        outputs.push(
            json!({"n": index, "value": coins(amount), "valueZat": amount,
            "scriptPubKey": script_json(reader.bytes()?, testnet)}),
        );
    }
    let locktime = u32::from_le_bytes(reader.number()?);
    let mut raw = json!({"version": version, "overwintered": overwintered, "vin": inputs,
        "vout": outputs, "locktime": locktime, "decodedBy": "ZMQ"});
    if let Some(group) = group {
        raw["versiongroupid"] = json!(format!("{group:08x}"));
        raw["expiryheight"] = json!(u32::from_le_bytes(reader.number()?));
    }
    let mut binding_signature = false;
    if version == 4 {
        let balance = i64::from_le_bytes(reader.number()?);
        raw["valueBalanceZat"] = json!(balance);
        raw["valueBalance"] = json!(coins(balance));
        let mut spends = Vec::new();
        for _ in 0..reader.compact(384)? {
            let cv = hash_hex(reader.number()?);
            let anchor = hash_hex(reader.number()?);
            let nullifier = hash_hex(reader.number()?);
            let rk = hash_hex(reader.number()?);
            reader.take(192 + 64)?;
            spends.push(json!({"cv": cv, "anchor": anchor, "nullifier": nullifier, "rk": rk}));
        }
        let mut shielded_outputs = Vec::new();
        for _ in 0..reader.compact(948)? {
            let cv = hash_hex(reader.number()?);
            let cmu = hash_hex(reader.number()?);
            let ephemeral_key = hash_hex(reader.number()?);
            reader.take(580 + 80 + 192)?;
            shielded_outputs.push(json!({"cv": cv, "cmu": cmu, "ephemeralKey": ephemeral_key}));
        }
        binding_signature = !spends.is_empty() || !shielded_outputs.is_empty();
        raw["vShieldedSpend"] = json!(spends);
        raw["vShieldedOutput"] = json!(shielded_outputs);
    }
    let mut joinsplits = Vec::new();
    if version >= 2 {
        let proof_size = if version == 4 { 192 } else { 296 };
        // Two Sprout ciphertexts, each containing 585 plaintext bytes and a 16-byte tag.
        for _ in 0..reader.compact(304 + proof_size + 1202)? {
            let old = i64::from_le_bytes(reader.number()?);
            let new = i64::from_le_bytes(reader.number()?);
            let anchor = hash_hex(reader.number()?);
            let nullifiers = [hash_hex(reader.number()?), hash_hex(reader.number()?)];
            let commitments = [hash_hex(reader.number()?), hash_hex(reader.number()?)];
            reader.take(128 + proof_size + 1202)?;
            joinsplits.push(json!({"anchor": anchor, "nullifiers": nullifiers, "commitments": commitments,
                "vpub_old": coins(old), "vpub_oldZat": old, "vpub_new": coins(new), "vpub_newZat": new}));
        }
        if !joinsplits.is_empty() {
            reader.take(32 + 64)?;
        }
    }
    raw["vjoinsplit"] = json!(joinsplits);
    if binding_signature {
        reader.take(64)?;
    }
    let size = start.len() - reader.0.len();
    let hash = sha256d(&start[..size]);
    raw["txid"] = json!(hash_hex(hash));
    raw["size"] = json!(size);
    Ok((hash, raw))
}

pub(super) fn transaction(bytes: &[u8], testnet: bool) -> Result<(String, Value)> {
    ensure!(bytes.len() <= MAX_BODY, "oversized transaction");
    let mut reader = Reader(bytes);
    let (hash, value) = read_transaction(&mut reader, testnet)?;
    ensure!(reader.0.is_empty(), "trailing transaction bytes");
    Ok((hash_hex(hash), value))
}

fn merkle_root(mut hashes: Vec<[u8; 32]>) -> Result<[u8; 32]> {
    ensure!(!hashes.is_empty(), "empty block");
    while hashes.len() > 1 {
        let mut next = Vec::with_capacity(hashes.len().div_ceil(2));
        for pair in hashes.chunks(2) {
            let right = pair.get(1).unwrap_or(&pair[0]);
            ensure!(
                pair.len() != 2 || right != &pair[0],
                "duplicate Merkle branches"
            );
            let mut bytes = [0; 64];
            bytes[..32].copy_from_slice(&pair[0]);
            bytes[32..].copy_from_slice(right);
            next.push(sha256d(&bytes));
        }
        hashes = next;
    }
    Ok(hashes[0])
}

#[derive(Debug)]
pub(super) struct Block {
    pub header: Vec<u8>,
    pub parent: [u8; 32],
    pub version: i32,
    pub solution: String,
    pub transactions: Vec<Value>,
    pub size: usize,
}

impl Block {
    /// Match fields from the local daemon's qualified header instead of reimplementing
    /// the chain- and era-specific Verus header hash.
    pub fn matches_rpc(&self, header: &Value) -> bool {
        fn matches_hash(raw: &[u8], expected: &Value) -> bool {
            let bytes: [u8; 32] = raw.try_into().expect("fixed header field");
            expected.as_str() == Some(hash_hex(bytes).as_str())
        }
        let previous = if self.parent == [0; 32] {
            header.get("previousblockhash").is_none_or(Value::is_null)
        } else {
            matches_hash(&self.header[4..36], &header["previousblockhash"])
        };
        let time = u32::from_le_bytes(self.header[100..104].try_into().expect("fixed header time"));
        let bits = u32::from_le_bytes(self.header[104..108].try_into().expect("fixed header bits"));
        previous
            && header["version"].as_i64() == Some(i64::from(self.version))
            && matches_hash(&self.header[36..68], &header["merkleroot"])
            && matches_hash(&self.header[68..100], &header["finalsaplingroot"])
            && header["time"].as_u64() == Some(u64::from(time))
            && header["bits"].as_str() == Some(format!("{bits:08x}").as_str())
            && matches_hash(&self.header[108..140], &header["nonce"])
            && header["solution"].as_str() == Some(self.solution.as_str())
    }
}

pub(super) fn block(bytes: &[u8], testnet: bool) -> Result<Block> {
    ensure!(bytes.len() <= MAX_BODY, "oversized block");
    let mut reader = Reader(bytes);
    let version = i32::from_le_bytes(reader.number()?);
    let parent = reader.number()?;
    let merkle = reader.number::<32>()?;
    reader.take(32 + 4 + 4 + 32)?;
    let solution = hex(reader.bytes()?);
    let header = bytes[..bytes.len() - reader.0.len()].to_vec();
    let mut hashes = Vec::new();
    let mut transactions = Vec::new();
    for _ in 0..reader.compact(10)? {
        let (hash, tx) = read_transaction(&mut reader, testnet)?;
        hashes.push(hash);
        transactions.push(tx);
    }
    ensure!(reader.0.is_empty(), "trailing block bytes");
    ensure!(
        merkle_root(hashes)? == merkle,
        "raw block Merkle root mismatch"
    );
    Ok(Block {
        header,
        parent,
        version,
        solution,
        transactions,
        size: bytes.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn basic(version: u32, group: Option<u32>) -> Vec<u8> {
        let mut data = (version | if group.is_some() { 1 << 31 } else { 0 })
            .to_le_bytes()
            .to_vec();
        if let Some(group) = group {
            data.extend(group.to_le_bytes());
        }
        data.push(1); // one input
        data.extend([0; 32]);
        data.extend(u32::MAX.to_le_bytes());
        data.extend([1, 0]);
        data.extend(u32::MAX.to_le_bytes());
        data.push(1); // one output
        data.extend(300i64.to_le_bytes());
        data.extend([1, 0x6a]);
        data.extend(0u32.to_le_bytes());
        if group.is_some() {
            data.extend(123u32.to_le_bytes());
        }
        data
    }

    #[test]
    fn accepts_each_version_and_public_shielded_fields() {
        for (version, group) in [
            (1, None),
            (2, None),
            (3, Some(0x03c4_8270)),
            (4, Some(0x892f_2085)),
        ] {
            let mut bytes = basic(version, group);
            if version == 4 {
                bytes.extend((-47i64).to_le_bytes());
                bytes.push(1);
                bytes.extend([0x21; 384]);
                bytes.push(1);
                bytes.extend([0x32; 948]);
            }
            if version >= 2 {
                bytes.push(1);
                bytes.extend(201i64.to_le_bytes());
                bytes.extend(202i64.to_le_bytes());
                bytes.extend(vec![
                    0x43;
                    288 + if version == 4 { 192 } else { 296 } + 1202
                ]);
                bytes.extend([0x54; 96]);
            }
            if version == 4 {
                bytes.extend([0x65; 64]);
            }
            let (hash, tx) = transaction(&bytes, false).unwrap();
            assert_eq!(hash, hash_hex(sha256d(&bytes)));
            assert_eq!(tx["version"], version);
            assert_eq!(tx["vin"][0]["coinbase"], "00");
            assert_eq!(tx["vout"][0]["valueZat"], 300);
            assert_eq!(tx["size"], bytes.len());
            if version >= 2 {
                assert_eq!(tx["vjoinsplit"][0]["vpub_oldZat"], 201);
                assert_eq!(tx["vjoinsplit"][0]["vpub_newZat"], 202);
            }
            if version == 4 {
                assert_eq!(tx["valueBalanceZat"], -47);
                assert_eq!(tx["vShieldedSpend"][0]["nullifier"], "21".repeat(32));
                assert_eq!(tx["vShieldedOutput"][0]["cmu"], "32".repeat(32));
            }
            assert!(transaction(&bytes[..bytes.len() - 1], false).is_err());
            bytes.push(0);
            assert!(transaction(&bytes, false).is_err());
        }
    }

    #[test]
    fn txids_and_amounts_match_verus_decoded_transaction_fixtures() {
        fn check(value: &Value) -> usize {
            if let (Some(txid), Some(raw), Some(outputs)) = (
                value["txid"].as_str(),
                value["hex"].as_str(),
                value["vout"].as_array(),
            ) {
                let bytes: Vec<_> = raw
                    .as_bytes()
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                    .collect();
                let (id, native) = transaction(&bytes, false).unwrap();
                assert_eq!(id, txid);
                assert_eq!(
                    native["vin"].as_array().unwrap().len(),
                    value["vin"].as_array().unwrap().len()
                );
                assert_eq!(native["vout"].as_array().unwrap().len(), outputs.len());
                for (local, rpc) in native["vout"].as_array().unwrap().iter().zip(outputs) {
                    assert_eq!(local["scriptPubKey"]["hex"], rpc["scriptPubKey"]["hex"]);
                    assert!(
                        (local["value"].as_f64().unwrap() - rpc["value"].as_f64().unwrap()).abs()
                            < 0.00000001
                    );
                }
                return 1;
            }
            match value {
                Value::Array(values) => values.iter().map(check).sum(),
                Value::Object(values) => values.values().map(check).sum(),
                _ => 0,
            }
        }
        for fixture in [
            include_str!("../../tests/fixtures/defi-refunds/refund-bots.json"),
            include_str!("../../tests/fixtures/defi-refunds/burn-ethereum-dai.json"),
        ] {
            let value: Value = serde_json::from_str(fixture).unwrap();
            assert!(check(&value) >= 3);
        }
    }

    #[test]
    fn parses_a_block_with_an_extended_solution_and_rejects_bad_merkle_root() {
        let tx = basic(1, None);
        let mut raw = vec![0; 140];
        raw[36..68].copy_from_slice(&sha256d(&tx));
        raw.extend([253, 1, 1]); // canonical CompactSize for 257-byte solution
        raw.extend([0xab; 257]);
        raw.push(1);
        raw.extend(&tx);
        let parsed = block(&raw, false).unwrap();
        assert_eq!(parsed.header.len(), 400);
        assert_eq!(parsed.transactions[0]["txid"], hash_hex(sha256d(&tx)));
        assert_eq!(parsed.solution.len(), 514);
        raw[36] ^= 1;
        assert!(block(&raw, false).is_err());
        assert!(transaction(&[1, 0, 0, 0, 253, 0, 0], false).is_err());
    }
}
