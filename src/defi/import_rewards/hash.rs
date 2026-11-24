//! BLAKE2b-256 with VerusDefaultHash personalization.

const IV: [u64; 8] = [
    0x6a09e667f3bcc908,
    0xbb67ae8584caa73b,
    0x3c6ef372fe94f82b,
    0xa54ff53a5f1d36f1,
    0x510e527fade682d1,
    0x9b05688c2b3e6c1f,
    0x1f83d9abfb41bd6b,
    0x5be0cd19137e2179,
];
const SIGMA: [[usize; 16]; 10] = [
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    [14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3],
    [11, 8, 12, 0, 5, 2, 15, 13, 10, 14, 3, 6, 7, 1, 9, 4],
    [7, 9, 3, 1, 13, 12, 11, 14, 2, 6, 5, 10, 4, 0, 15, 8],
    [9, 0, 5, 7, 2, 4, 10, 15, 14, 1, 11, 12, 6, 8, 3, 13],
    [2, 12, 6, 10, 0, 11, 8, 3, 4, 13, 7, 5, 15, 14, 1, 9],
    [12, 5, 1, 15, 14, 13, 4, 10, 0, 7, 6, 3, 9, 2, 8, 11],
    [13, 11, 7, 14, 12, 1, 3, 9, 5, 0, 15, 4, 8, 6, 2, 10],
    [6, 15, 14, 9, 11, 3, 0, 8, 12, 2, 13, 7, 1, 4, 10, 5],
    [10, 2, 8, 4, 7, 6, 1, 5, 15, 11, 9, 14, 3, 12, 13, 0],
];
const LANES: [[usize; 4]; 8] = [
    [0, 4, 8, 12],
    [1, 5, 9, 13],
    [2, 6, 10, 14],
    [3, 7, 11, 15],
    [0, 5, 10, 15],
    [1, 6, 11, 12],
    [2, 7, 8, 13],
    [3, 4, 9, 14],
];

pub(super) fn verus_hash(bytes: &[u8]) -> Option<[u8; 32]> {
    let mut initial = IV;
    initial[0] ^= 0x0101_0020;
    initial[6] ^= u64::from_le_bytes(*b"VerusDef");
    initial[7] ^= u64::from_le_bytes(*b"aultHash");
    let mut size = 0;
    let mut blocks = bytes.chunks(128).peekable();
    if blocks.peek().is_none() {
        compress(&mut initial, &[], 0, true);
    }
    while let Some(block) = blocks.next() {
        size += block.len() as u64;
        compress(&mut initial, block, size, blocks.peek().is_none());
    }
    let mut output = [0; 32];
    for (index, word) in output.as_chunks_mut::<8>().0.iter_mut().enumerate() {
        word.copy_from_slice(&initial[index].to_le_bytes());
    }
    Some(output)
}

fn compress(initial: &mut [u64; 8], bytes: &[u8], size: u64, last: bool) {
    let mut block = [0; 128];
    block[..bytes.len()].copy_from_slice(bytes);
    let mut words = [0; 16];
    for (word, chunk) in words.iter_mut().zip(block.as_chunks::<8>().0) {
        *word = u64::from_le_bytes(*chunk);
    }
    let mut v = [0; 16];
    v[..8].copy_from_slice(initial);
    v[8..].copy_from_slice(&IV);
    v[12] ^= size;
    if last {
        v[14] = !v[14];
    }
    for round in 0..12 {
        let permutation = SIGMA[round % 10];
        for (index, [a, b, c, d]) in LANES.into_iter().enumerate() {
            let x = words[permutation[2 * index]];
            let y = words[permutation[2 * index + 1]];
            v[a] = v[a].wrapping_add(v[b]).wrapping_add(x);
            v[d] = (v[d] ^ v[a]).rotate_right(32);
            v[c] = v[c].wrapping_add(v[d]);
            v[b] = (v[b] ^ v[c]).rotate_right(24);
            v[a] = v[a].wrapping_add(v[b]).wrapping_add(y);
            v[d] = (v[d] ^ v[a]).rotate_right(16);
            v[c] = v[c].wrapping_add(v[d]);
            v[b] = (v[b] ^ v[c]).rotate_right(63);
        }
    }
    for index in 0..8 {
        initial[index] ^= v[index] ^ v[index + 8];
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn personalized_empty_digest_matches_reference() {
        let digest = verus_hash(b"")
            .unwrap()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        assert_eq!(
            digest,
            "ffb28f63665ccf5ed7ab327a93af43244e35586ed3fc78e06bc57c66e5565844"
        );
    }

    #[test]
    fn personalized_digest_matches_full_and_partial_block_vectors() {
        for (size, expected) in [
            (
                128,
                "d066f1f47dd15a2a6757b26646e5390ac73adcd5d67109968b3a1edcd95f911b",
            ),
            (
                129,
                "33f11784a00ef7c19c95a2c769cd31e4f43528b8de3f77a39e2ef5eb2b795c8e",
            ),
            (
                256,
                "d71bf2c5c2386296052238b3b407170becdc2288727f505b01fc41477d7ff361",
            ),
        ] {
            let actual = verus_hash(&vec![0; size])
                .unwrap()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            assert_eq!(actual, expected);
        }
    }
}
