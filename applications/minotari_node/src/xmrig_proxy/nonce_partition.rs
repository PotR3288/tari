// Copyright 2025. The Tari Project
//
// Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
// following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
// disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
// following disclaimer in the documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote
// products derived from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
// USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

use std::ops::Range;

use super::MinerId;

/// Full 32-bit nonce space
const NONCE_SPACE_START: u32 = 0x00000000;
const NONCE_SPACE_END: u32 = 0xFFFFFFFF;

/// Minimum nonces per miner (65,536) — below this, reject new miners
const MIN_NONCE_RANGE_SIZE: u32 = 0x10000;

/// Hard cap on concurrent miners
const MAX_MINERS_HARD_CAP: usize = 128;

/// Tracks which portions of the 32-bit nonce space are allocated.
/// Each entry maps a MinerId to its assigned contiguous range.
pub struct NoncePartitioner {
    allocations: std::collections::HashMap<MinerId, Range<u32>>,
}

impl NoncePartitioner {
    /// Create a new, empty partitioner.
    pub fn new() -> Self {
        Self {
            allocations: std::collections::HashMap::new(),
        }
    }

    /// Assign a non-overlapping nonce range to a miner.
    ///
    /// Returns `None` if remaining space is below `MIN_NONCE_RANGE_SIZE`.
    ///
    /// **Solo miner optimization:** if this is the only miner, it receives the full
    /// 32-bit nonce space (`0x00000000..0xFFFFFFFF`).
    ///
    /// **Multi-miner strategy:** divide the space evenly among all active miners,
    /// re-partitioning existing ranges so that no two miners overlap.
    pub fn assign(&mut self, miner_id: &MinerId) -> Option<Range<u32>> {
        // Solo miner gets the full range
        if self.allocations.is_empty() {
            let range = NONCE_SPACE_START..NONCE_SPACE_END;
            self.allocations.insert(miner_id.clone(), range.clone());
            return Some(range);
        }

        // Check hard cap
        if self.allocations.len() >= MAX_MINERS_HARD_CAP {
            return None;
        }

        let n = (self.allocations.len() + 1) as u32; // including the new miner
        let range_size = NONCE_SPACE_END / n;

        // If the per-miner slice is too small, reject
        if range_size < MIN_NONCE_RANGE_SIZE {
            return None;
        }

        // Re-partition: shrink all existing ranges to equal slots so nothing overlaps
        let existing_ids: Vec<MinerId> = self.allocations.keys().cloned().collect();
        for (i, id) in existing_ids.iter().enumerate() {
            let start = (i as u32).saturating_mul(range_size);
            let end = start.saturating_add(range_size);
            if let Some(entry) = self.allocations.get_mut(id) {
                *entry = start..end;
            }
        }

        // Assign new miner to the next slot
        let slot_index = existing_ids.len();
        let start = (slot_index as u32).saturating_mul(range_size);
        let end = start.saturating_add(range_size);

        if start == NONCE_SPACE_END {
            return None;
        }

        let range = start..end;
        self.allocations.insert(miner_id.clone(), range.clone());
        Some(range)
    }

    /// Reclaim a miner's range back into the free pool.
    pub fn reclaim(&mut self, miner_id: &MinerId) {
        self.allocations.remove(miner_id);
    }

    /// Verify a submitted nonce belongs to the miner's assigned range.
    #[allow(dead_code)]
    pub fn is_nonce_in_range(&self, miner_id: &MinerId, nonce: u64) -> bool {
        let range = match self.allocations.get(miner_id) {
            Some(r) => r,
            None => return false,
        };
        let nonce_u32 = nonce as u32;
        range.contains(&nonce_u32)
    }

    /// Reset all allocations (called on template invalidation / chain advance).
    #[allow(dead_code)]
    pub fn reset(&mut self) {
        self.allocations.clear();
    }

    /// Get number of active allocations.
    #[allow(dead_code)]
    pub fn active_count(&self) -> usize {
        self.allocations.len()
    }

    /// Retrieve the nonce range for a given miner (if assigned).
    #[allow(dead_code)]
    pub fn get_range(&self, miner_id: &str) -> Option<Range<u32>> {
        self.allocations.get(miner_id).cloned()
    }

    /// Check whether a miner has an assigned range.
    #[allow(dead_code)]
    pub fn has_miner(&self, miner_id: &MinerId) -> bool {
        self.allocations.contains_key(miner_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn solo_miner_gets_full_range() {
        let mut p = NoncePartitioner::new();
        let range = p.assign(&"solo".to_string()).unwrap();
        assert_eq!(range, 0x00000000..0xFFFFFFFF);
        assert_eq!(p.active_count(), 1);
    }

    #[test]
    fn multi_miner_gets_split_range() {
        let mut p = NoncePartitioner::new();
        p.assign(&"m1".to_string()).unwrap();
        let r2 = p.assign(&"m2".to_string()).unwrap();
        assert_eq!(p.active_count(), 2);
        assert!(!r2.is_empty());
    }

    #[test]
    fn nonce_in_range_for_owner() {
        let mut p = NoncePartitioner::new();
        let range = p.assign(&"alice".to_string()).unwrap();
        let mid = (range.start..range.end).nth(100).unwrap_or(range.start);
        assert!(p.is_nonce_in_range(&"alice".to_string(), mid as u64));
    }

    #[test]
    fn nonce_out_of_range_rejected() {
        let mut p = NoncePartitioner::new();
        p.assign(&"bob".to_string()).unwrap();
        assert!(!p.is_nonce_in_range(&"bob".to_string(), u64::MAX));
    }

    #[test]
    fn unknown_miner_fails_nonce_check() {
        let mut p = NoncePartitioner::new();
        p.assign(&"carol".to_string()).unwrap();
        assert!(!p.is_nonce_in_range(&"unknown".to_string(), 0));
    }

    #[test]
    fn reclaim_frees_miner() {
        let mut p = NoncePartitioner::new();
        p.assign(&"dave".to_string()).unwrap();
        assert_eq!(p.active_count(), 1);
        p.reclaim(&"dave".to_string());
        assert_eq!(p.active_count(), 0);
    }

    #[test]
    fn reset_clears_all() {
        let mut p = NoncePartitioner::new();
        p.assign(&"a".to_string()).unwrap();
        p.assign(&"b".to_string()).unwrap();
        p.assign(&"c".to_string()).unwrap();
        assert_eq!(p.active_count(), 3);
        p.reset();
        assert_eq!(p.active_count(), 0);
    }

    #[test]
    fn assign_returns_none_when_space_exhausted() {
        let mut p = NoncePartitioner::new();
        for i in 0..MAX_MINERS_HARD_CAP {
            let id = format!("miner_{i}");
            let result = p.assign(&id);
            _ = result;
        }
        assert!(p.assign(&"overflow".to_string()).is_none());
    }

    #[test]
    fn ranges_do_not_overlap() {
        let mut p = NoncePartitioner::new();
        let ranges: Vec<Range<u32>> = (0..32).map(|i| p.assign(&format!("m{i}")).unwrap()).collect();

        for i in 0..ranges.len() {
            for j in (i + 1)..ranges.len() {
                assert!(
                    ranges[i].end <= ranges[j].start || ranges[j].end <= ranges[i].start,
                    "ranges[{}] {:?} overlaps with ranges[{}] {:?}",
                    i,
                    ranges[i],
                    j,
                    ranges[j]
                );
            }
        }
    }
}
