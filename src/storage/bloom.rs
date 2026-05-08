const DEFAULT_BITS_PER_KEY: u64 = 10;
const DEFAULT_HASH_COUNT: u32 = 7;
const MIN_BITS: u64 = 64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BloomFilter {
    bit_len: u64,
    hash_count: u32,
    bits: Vec<u8>,
}

impl BloomFilter {
    pub fn new(expected_items: usize) -> Self {
        let bit_len = ((expected_items as u64).saturating_mul(DEFAULT_BITS_PER_KEY)).max(MIN_BITS);
        let byte_len = bit_len.div_ceil(8) as usize;

        Self {
            bit_len,
            hash_count: DEFAULT_HASH_COUNT,
            bits: vec![0; byte_len],
        }
    }

    pub fn from_parts(bit_len: u64, hash_count: u32, bits: Vec<u8>) -> Option<Self> {
        let expected_bytes = bit_len.div_ceil(8) as usize;

        if bit_len == 0 || hash_count == 0 || bits.len() != expected_bytes {
            return None;
        }

        Some(Self {
            bit_len,
            hash_count,
            bits,
        })
    }

    pub fn insert<K>(&mut self, key: K)
    where
        K: AsRef<[u8]>,
    {
        let key = key.as_ref();

        for bit_index in self.bit_indices(key) {
            self.set_bit(bit_index);
        }
    }

    pub fn might_contain<K>(&self, key: K) -> bool
    where
        K: AsRef<[u8]>,
    {
        self.bit_indices(key.as_ref())
            .into_iter()
            .all(|bit_index| self.is_bit_set(bit_index))
    }

    pub fn bit_len(&self) -> u64 {
        self.bit_len
    }

    pub fn hash_count(&self) -> u32 {
        self.hash_count
    }

    pub fn bits(&self) -> &[u8] {
        &self.bits
    }

    fn bit_indices(&self, key: &[u8]) -> Vec<u64> {
        let h1 = hash_with_seed(key, 0xcbf2_9ce4_8422_2325);
        let h2 = hash_with_seed(key, 0x9e37_79b9_7f4a_7c15) | 1;

        (0..self.hash_count)
            .map(|i| h1.wrapping_add((i as u64).wrapping_mul(h2)) % self.bit_len)
            .collect()
    }

    fn set_bit(&mut self, bit_index: u64) {
        let byte_index = (bit_index / 8) as usize;
        let bit_mask = 1u8 << ((bit_index % 8) as u32);
        self.bits[byte_index] |= bit_mask;
    }

    fn is_bit_set(&self, bit_index: u64) -> bool {
        let byte_index = (bit_index / 8) as usize;
        let bit_mask = 1u8 << ((bit_index % 8) as u32);
        self.bits[byte_index] & bit_mask != 0
    }
}

fn hash_with_seed(bytes: &[u8], seed: u64) -> u64 {
    let mut hash = seed;

    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }

    hash
}
