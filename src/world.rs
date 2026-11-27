//! Network and protocol identities used by persistence and source routing.
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Network {
    Mainnet,
    Testnet,
}

impl Network {
    pub fn from_testnet(testnet: bool) -> Self {
        if testnet {
            Self::Testnet
        } else {
            Self::Mainnet
        }
    }

    pub fn database_name(self) -> &'static str {
        match self {
            Self::Mainnet => "mainnet.sqlite3",
            Self::Testnet => "testnet.sqlite3",
        }
    }
}

/// A daemon-reported protocol locator, never a chain display name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SystemId(String);

impl TryFrom<String> for SystemId {
    type Error = anyhow::Error;
    fn try_from(value: String) -> Result<Self> {
        ensure!(
            value.starts_with('i')
                && value.len() <= 64
                && value.len() > 1
                && value.bytes().all(|c| c.is_ascii_alphanumeric()),
            "invalid system ID"
        );
        Ok(Self(value))
    }
}
impl From<SystemId> for String {
    fn from(value: SystemId) -> Self {
        value.0
    }
}
impl AsRef<str> for SystemId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
impl fmt::Display for SystemId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectKind {
    Block,
    Transaction,
    Address,
    Identity,
    Currency,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectRef {
    pub system_id: SystemId,
    pub kind: ObjectKind,
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OutpointRef {
    pub system_id: SystemId,
    pub txid: String,
    pub vout: u32,
}

/// Filesystem spelling used by CanonicalChainFileName in the daemon.
/// Hashing names here resolves a locator; it is not blockchain validation.
pub fn chain_directory(chain: &str, network: Network) -> Result<String> {
    if chain.eq_ignore_ascii_case("vrsc") || chain.eq_ignore_ascii_case("vrsctest") {
        return Ok(if network == Network::Testnet {
            "vrsctest"
        } else {
            "VRSC"
        }
        .into());
    }
    if chain.len() == 40 && chain.bytes().all(|c| c.is_ascii_hexdigit()) {
        return Ok(chain.to_ascii_lowercase());
    }
    let bytes = if chain.starts_with('i') && chain.len() == 34 {
        decode_identity(chain)?
    } else {
        name_hash(chain, network)?
    };
    Ok(bytes.iter().rev().map(|b| format!("{b:02x}")).collect())
}

fn name_hash(name: &str, network: Network) -> Result<[u8; 20]> {
    use sha2::{Digest, Sha256};
    let name = name.trim_end_matches('@').to_lowercase();
    ensure!(!name.contains('@'), "chain name must use a terminal @ only");
    let rooted = name.ends_with('.');
    let mut parts: Vec<_> = name.trim_end_matches('.').split('.').collect();
    let root = if network == Network::Testnet {
        "vrsctest"
    } else {
        "vrsc"
    };
    if !rooted && parts.last().copied() != Some(root) {
        parts.push(root);
    }
    let mut parent: Option<[u8; 20]> = None;
    for part in parts.into_iter().rev() {
        ensure!(
            !part.is_empty() && part.trim() == part && part.len() < 64,
            "invalid chain name"
        );
        let mut hash = double_sha(part.as_bytes());
        if let Some(parent) = parent {
            let mut combined = parent.to_vec();
            combined.extend_from_slice(&hash);
            hash = double_sha(&combined);
        }
        parent = Some(ripemd::Ripemd160::digest(Sha256::digest(hash)).into());
    }
    parent.ok_or_else(|| anyhow::anyhow!("empty chain name"))
}
fn double_sha(bytes: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(Sha256::digest(bytes)).into()
}
const BASE58: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
fn decode_identity(text: &str) -> Result<[u8; 20]> {
    let mut bytes = vec![0_u8];
    for character in text.bytes() {
        let mut carry = BASE58
            .iter()
            .position(|c| *c == character)
            .ok_or_else(|| anyhow::anyhow!("invalid identity address"))?
            as u32;
        for byte in bytes.iter_mut().rev() {
            carry += u32::from(*byte) * 58;
            *byte = carry as u8;
            carry >>= 8;
        }
        while carry > 0 {
            bytes.insert(0, carry as u8);
            carry >>= 8;
        }
    }
    ensure!(
        bytes.len() == 25 && bytes[0] == 102 && double_sha(&bytes[..21])[..4] == bytes[21..],
        "invalid identity address checksum"
    );
    Ok(bytes[1..21].try_into()?)
}

pub fn system_id(chain: &str, network: Network) -> Result<SystemId> {
    let hash = if chain.starts_with('i') && chain.len() == 34 {
        decode_identity(chain)?
    } else if chain.len() == 40 && chain.bytes().all(|c| c.is_ascii_hexdigit()) {
        let mut bytes = [0u8; 20];
        for (index, byte) in bytes.iter_mut().rev().enumerate() {
            *byte = u8::from_str_radix(&chain[index * 2..index * 2 + 2], 16)?;
        }
        bytes
    } else {
        name_hash(
            if chain.eq_ignore_ascii_case("VRSC") && network == Network::Testnet {
                "vrsctest"
            } else {
                chain
            },
            network,
        )?
    };
    SystemId::try_from(base58_check(102, &hash))
}

pub(crate) fn base58_check(prefix: u8, hash: &[u8]) -> String {
    let mut bytes = vec![prefix];
    bytes.extend_from_slice(hash);
    let checksum = double_sha(&bytes);
    bytes.extend_from_slice(&checksum[..4]);
    let leading_zeros = bytes.iter().take_while(|&&byte| byte == 0).count();
    let mut digits = Vec::new();
    for byte in bytes {
        let mut carry = u32::from(byte);
        for digit in &mut digits {
            carry += u32::from(*digit) * 256;
            *digit = (carry % 58) as u8;
            carry /= 58;
        }
        while carry > 0 {
            digits.push((carry % 58) as u8);
            carry /= 58;
        }
    }
    std::iter::repeat_n('1', leading_zeros)
        .chain(
            digits
                .iter()
                .rev()
                .map(|digit| BASE58[*digit as usize] as char),
        )
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn network_and_qualified_name_paths_are_distinct() {
        assert_eq!(chain_directory("VRSC", Network::Mainnet).unwrap(), "VRSC");
        assert_eq!(
            chain_directory("VRSC", Network::Testnet).unwrap(),
            "vrsctest"
        );
        assert_eq!(
            chain_directory("vdex", Network::Mainnet).unwrap(),
            chain_directory("VDEX.VRSC@", Network::Mainnet).unwrap()
        );
        assert_ne!(
            chain_directory("vdex", Network::Mainnet).unwrap(),
            chain_directory("vdex", Network::Testnet).unwrap()
        );
        // Known VRSC identity locator uses the same name hash as the daemon.
        assert_eq!(
            decode_identity("i5w5MuNik5NtLcYmNzcvaoixooEebB6MGV").unwrap(),
            name_hash("VRSC", Network::Mainnet).unwrap()
        );
    }
}
