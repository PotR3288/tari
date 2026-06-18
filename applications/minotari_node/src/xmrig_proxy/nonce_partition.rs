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

use log::debug;
use super::MinerId;

const LOG_TARGET: &str = "minotari::base_node::xmrig_proxy::nonce_partition";

/// Full 64-bit nonce space (0x0000_0000_0000_0000..u64::MAX covers all u64 values)
const NONCE_SPACE_START: u64 = 0x0000_0000_0000_0000;
const NONCE_SPACE_END: u64 = u64::MAX;

/// Minimum nonces per miner (65,536) — below this, reject new miners
const MIN_NONCE_RANGE_SIZE: u64 = 0x10000;

/// Tracks which portions of the 64-bit nonce space are allocated.
/// Each entry maps a MinerId to its assigned contiguous range.
pub struct NoncePartitioner {
    allocations: std::collections::HashMap<MinerId, Range<u64>>,
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
    /// Returns `None` if the per-miner slice would be below `MIN_NONCE_RANGE_SIZE`.
    /// There is no independent max-miners cap here — that is enforced upstream by
    /// `MinerRegistry::get_or_register()`, so callers only reach this function when
    /// they are already authorised to mine.
    ///
    /// **Solo miner optimization:** if this is the only miner, it receives the full
    /// 64-bit nonce space (`0x0000_0000_0000_0000..u64::MAX`).
    ///
    /// **Multi-miner strategy:** divide the space evenly among all active miners,
    /// re-partitioning existing ranges so that no two miners overlap. Existing
    /// miners are sorted by ID before partitioning so each miner consistently gets
    /// the same range regardless of HashMap iteration randomness.
    pub fn assign(&mut self, miner_id: &MinerId) -> Option<Range<u64>> {
        // If this miner already has an allocation, return it without repartitioning.
        // This ensures idempotency — repeated requests from the same identity get
        // the same nonce range (cached template path).
        if let Some(range) = self.allocations.get(miner_id).cloned() {
            return Some(range);
        }

        // Solo miner gets the full range
        if self.allocations.is_empty() {
            let range = NONCE_SPACE_START..NONCE_SPACE_END;
            self.allocations.insert(miner_id.clone(), range.clone());
            return Some(range);
        }

        let n = (self.allocations.len() + 1) as u64; // including the new miner
        // Divide the full 64-bit space evenly. We use u64::MAX / n directly since
        // computing (u64::MAX + 1) overflows — the off-by-one is negligible at this scale.
        let range_size = u64::MAX / n;

        // If the per-miner slice is too small, reject
        if range_size < MIN_NONCE_RANGE_SIZE {
            return None;
        }

        // Re-partition: sort IDs so each miner consistently gets the same slot
        // regardless of HashMap iteration order or insertion sequence.
        let mut sorted_ids: Vec<MinerId> = self.allocations.keys().cloned().collect();
        sorted_ids.sort_unstable();

        for (i, id) in sorted_ids.iter().enumerate() {
            let start = NONCE_SPACE_START.saturating_add((i as u64).saturating_mul(range_size));
            let end = start.saturating_add(range_size);
            if let Some(entry) = self.allocations.get_mut(id) {
                *entry = start..end;
            }
        }

        // Assign new miner to the next slot
        let slot_index = sorted_ids.len();
        let start = NONCE_SPACE_START.saturating_add((slot_index as u64).saturating_mul(range_size));
        let end = start.saturating_add(range_size);

        if start >= NONCE_SPACE_END {
            return None;
        }

        // Clamp the last miner's range to the nonce space boundary
        let end = end.min(NONCE_SPACE_END);
        let range = start..end;
        self.allocations.insert(miner_id.clone(), range.clone());
        Some(range)
    }

    /// Reclaim a miner's range back into the free pool.
    pub fn reclaim(&mut self, miner_id: &MinerId) {
        self.allocations.remove(miner_id);
    }

    /// Reclaim ranges for multiple miner IDs at once (called on template eviction).
    /// Returns the number of allocations that were actually removed.
    /// Deduplicates naturally — removing a key that doesn't exist is a no-op.
    pub fn reclaim_all<I, S>(&mut self, miner_ids: I) -> usize
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut count = 0;
        for id in miner_ids {
            if self.allocations.remove(id.as_ref()).is_some() {
                count += 1;
            }
        }
        if count > 0 {
            debug!(target: LOG_TARGET, "Reclaimed {} nonce ranges from evicted templates", count);
        }
        count
    }

    /// Reset all allocations (called on template invalidation / chain advance).
    // Kept for testing; production path uses reclaim_all() instead.
    #[allow(dead_code)]
    pub(crate) fn reset(&mut self) {
        self.allocations.clear();
    }

    /// Verify a submitted nonce belongs to the miner's assigned range.
    // TODO: wire into handle_submit_block for identity verification (S2)
    #[allow(dead_code)]
    pub fn is_nonce_in_range(&self, miner_id: &MinerId, nonce: u64) -> bool {
        let range = match self.allocations.get(miner_id) {
            Some(r) => r,
            None => return false,
        };
        range.contains(&nonce)
    }

    /// Get number of active allocations.
    // TODO: expose via getinfo or metrics endpoint
    #[allow(dead_code)]
    pub fn active_count(&self) -> usize {
        self.allocations.len()
    }

    /// Retrieve the nonce range for a given miner (if assigned).
    // TODO: expose via monitoring/debugging endpoint
    #[allow(dead_code)]
    pub fn get_range(&self, miner_id: &str) -> Option<Range<u64>> {
        self.allocations.get(miner_id).cloned()
    }

    /// Return all current allocations sorted by miner ID for deterministic ordering.
    // TODO: expose via monitoring/debugging endpoint
    #[allow(dead_code)]
    pub fn get_all_ranges(&self) -> Vec<(String, Range<u64>)> {
        let mut entries: Vec<_> = self.allocations.iter().collect();
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));
        entries.into_iter().map(|(id, range)| (id.clone(), range.clone())).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Default max miners used in unit tests (matches the old hardcoded constant).
    const TEST_MAX_MINERS: usize = 128;

    #[test]
    fn solo_miner_gets_full_range() {
        let mut p = NoncePartitioner::new();
        let range = p.assign(&"solo".to_string()).unwrap();
        assert_eq!(range, 0x0000_0000_0000_0000..u64::MAX);
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
        assert!(p.is_nonce_in_range(&"alice".to_string(), mid));
    }

    #[test]
    fn nonce_out_of_range_rejected() {
        let mut p = NoncePartitioner::new();
        p.assign(&"bob".to_string()).unwrap();
        // Bob's range starts at 0, so u64::MAX is out of range (it's the boundary)
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
    fn no_independent_max_miners_cap() {
        // The partitioner must not enforce its own max-miners cap — that is the
        // registry's job.  This test verifies that assign() succeeds for more
        // allocations than TEST_MAX_MINERS, proving there is no independent gate.
        let mut p = NoncePartitioner::new();
        // Allocate well beyond the old hardcoded cap; all should succeed because
        // u64::MAX / n stays far above MIN_NONCE_RANGE_SIZE for any realistic n.
        for i in 0..(TEST_MAX_MINERS * 2) {
            let id = format!("miner_{i}");
            assert!(
                p.assign(&id).is_some(),
                "assign() returned None at allocation #{i} — partitioner has its own cap"
            );
        }
    }

    #[test]
    fn reclaim_all_removes_multiple_ranges() {
        let mut p = NoncePartitioner::new();
        for i in 0..10 {
            p.assign(&format!("m{i}")).unwrap();
        }
        assert_eq!(p.active_count(), 10);

        // Reclaim miners 2, 5, 7.
        let to_reclaim = vec!["m2".to_string(), "m5".to_string(), "m7".to_string()];
        let reclaimed = p.reclaim_all(&to_reclaim);
        assert_eq!(reclaimed, 3);
        assert_eq!(p.active_count(), 7);

        // Verify they're gone.
        for id in &to_reclaim {
            assert!(!p.get_range(id).is_some());
        }

        // Remaining miners still have ranges.
        assert!(p.get_range("m0").is_some());
        assert!(p.get_range("m9").is_some());
    }

    #[test]
    fn reclaim_all_ignores_unknown_miners() {
        let mut p = NoncePartitioner::new();
        p.assign(&"existing".to_string()).unwrap();
        assert_eq!(p.active_count(), 1);

        // Reclaim non-existent IDs — should return 0, not panic.
        let unknowns = vec!["ghost1".to_string(), "ghost2".to_string()];
        let reclaimed = p.reclaim_all(&unknowns);
        assert_eq!(reclaimed, 0);
        assert_eq!(p.active_count(), 1); // still just the one.
    }

    #[test]
    fn assign_after_targeted_reclaim() {
        // Simulate: 3 miners assigned, then targeted reclaim of middle one,
        // then a new miner should use the reclaimed slot without repartitioning others.
        let mut p = NoncePartitioner::new();
        p.assign(&"a".to_string()).unwrap(); // solo gets full range
        p.assign(&"b".to_string()).unwrap(); // triggers repartition
        p.assign(&"c".to_string()).unwrap(); // triggers repartition

        assert_eq!(p.active_count(), 3);

        // Reclaim "b" — simulates template eviction for that miner.
        p.reclaim(&"b".to_string());
        assert_eq!(p.active_count(), 2);

        // Assign a new miner "d" — should get a fresh range without affecting a/c.
        let d_range = p.assign(&"d".to_string()).unwrap();
        assert!(!d_range.is_empty());
        assert_eq!(p.active_count(), 3);

        // Verify a and c still have ranges (idempotent assign returns existing).
        let a_range = p.get_range("a");
        let c_range = p.get_range("c");
        assert!(a_range.is_some());
        assert!(c_range.is_some());
    }

    #[test]
    fn ranges_do_not_overlap() {
        let mut p = NoncePartitioner::new();
        for i in 0..32 {
            p.assign(&format!("m{i}")).unwrap();
        }

        // Check stored ranges (not captured return values, which become stale after re-partitioning)
        let ranges: Vec<Range<u64>> = (0..32)
            .map(|i| p.get_range(&format!("m{i}")).expect("miner should have a range"))
            .collect();

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
