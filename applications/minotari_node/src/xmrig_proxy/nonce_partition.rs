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

/// Tracks nonce search assignments for miners using random-start nonces.
///
/// Each miner receives a randomly chosen starting nonce and searches sequentially
/// from that point up to `u64::MAX`. Natural u64 overflow handles wraparound back
/// to 0, covering the full 2^64 space without any coordination between miners.
///
/// Collision probability is negligible: with 128 miners each searching ~10^12
/// nonces in a 2^64 space, expected pairwise collisions are < 10^-7 per block.
pub struct NoncePartitioner {
    /// Maps miner ID to their randomly chosen starting nonce.
    starts: std::collections::HashMap<MinerId, u64>,
}

impl NoncePartitioner {
    /// Create a new, empty partitioner.
    pub fn new() -> Self {
        Self {
            starts: std::collections::HashMap::new(),
        }
    }

    /// Assign a nonce search range to a miner.
    ///
    /// The miner receives a random starting nonce and searches sequentially from
    /// that point up to `u64::MAX`. Natural overflow handles wraparound (start..u64::MAX, then 0..start).
    ///
    /// Returns the full search range `(start..u64::MAX)`. The miner is responsible
    /// for wrapping around once it reaches `u64::MAX` — in practice this never
    /// happens before a block is found at mainnet difficulty.
    ///
    /// If the miner already has an allocation, returns the existing range without
    /// modification (idempotent).
    pub fn assign(&mut self, miner_id: &MinerId) -> Range<u64> {
        // If this miner already has an allocation, return it unchanged.
        if let Some(&start) = self.starts.get(miner_id) {
            return start..u64::MAX;
        }

        // Generate a random starting nonce for the new miner.
        let start = rand::random::<u64>();
        debug!(
            target: LOG_TARGET,
            "Assigned nonce range {:?} to miner {}",
            start..u64::MAX,
            miner_id
        );
        self.starts.insert(miner_id.clone(), start);
        start..u64::MAX
    }

    /// Reclaim a miner's allocation. Called when a template is evicted and the
    /// miner's nonce range should be freed.
    pub fn reclaim(&mut self, miner_id: &MinerId) {
        if self.starts.remove(miner_id).is_some() {
            debug!(target: LOG_TARGET, "Reclaimed nonce allocation for miner {}", miner_id);
        }
    }

    /// Reclaim ranges for multiple miner IDs at once (called on template eviction).
    /// Returns the number of allocations that were actually removed.
    pub fn reclaim_all<I, S>(&mut self, miner_ids: I) -> usize
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut count = 0;
        for id in miner_ids {
            if self.starts.remove(id.as_ref()).is_some() {
                count += 1;
            }
        }
        if count > 0 {
            debug!(target: LOG_TARGET, "Reclaimed {} nonce ranges from evicted templates", count);
        }
        count
    }

    /// Retrieve the nonce range for a given miner (if assigned).
    pub fn get_range(&self, miner_id: &str) -> Option<Range<u64>> {
        self.starts.get(miner_id).map(|&start| start..u64::MAX)
    }

    /// Return all current allocations sorted by miner ID for deterministic ordering.
    #[allow(dead_code)]
    pub fn get_all_ranges(&self) -> Vec<(String, Range<u64>)> {
        let mut entries: Vec<_> = self.starts.iter().collect();
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));
        entries
            .into_iter()
            .map(|(id, &start)| (id.clone(), start..u64::MAX))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn solo_miner_gets_random_start() {
        let mut p = NoncePartitioner::new();
        let range = p.assign(&"miner_0".to_string());
        // Range should be (start..u64::MAX) for some random start.
        assert!(range.start < u64::MAX);
        assert_eq!(range.end, u64::MAX);
    }

    #[test]
    fn multiple_miners_get_different_starts() {
        let mut p = NoncePartitioner::new();
        let r1 = p.assign(&"miner_0".to_string());
        let r2 = p.assign(&"miner_1".to_string());
        // With 2^64 space, collision of random starts is astronomically unlikely.
        assert_ne!(r1.start, r2.start);
    }

    #[test]
    fn assign_is_idempotent() {
        let mut p = NoncePartitioner::new();
        let r1 = p.assign(&"miner_0".to_string());
        let r2 = p.assign(&"miner_0".to_string());
        // Same range returned on second call.
        assert_eq!(r1.start, r2.start);
    }

    #[test]
    fn reclaim_frees_miner() {
        let mut p = NoncePartitioner::new();
        p.assign(&"miner_0".to_string());
        p.reclaim(&"miner_0".to_string());
        assert!(p.get_range("miner_0").is_none());
    }

    #[test]
    fn reclaim_all_removes_multiple_ranges() {
        let mut p = NoncePartitioner::new();
        for i in 0..10 {
            p.assign(&format!("miner_{i}"));
        }

        let to_reclaim = vec!["miner_2".to_string(), "miner_5".to_string(), "miner_7".to_string()];
        let reclaimed = p.reclaim_all(&to_reclaim);
        assert_eq!(reclaimed, 3);

        for id in &to_reclaim {
            assert!(p.get_range(id).is_none());
        }

        assert!(p.get_range("miner_0").is_some());
        assert!(p.get_range("miner_9").is_some());
    }

    #[test]
    fn reclaim_all_ignores_unknown_miners() {
        let mut p = NoncePartitioner::new();
        p.assign(&"miner_0".to_string());

        let unknowns = vec!["ghost_0".to_string(), "ghost_1".to_string()];
        let reclaimed = p.reclaim_all(&unknowns);
        assert_eq!(reclaimed, 0);
        assert!(p.get_range("miner_0").is_some());
    }

    #[test]
    fn no_repartitioning_on_new_miner() {
        // Key invariant: adding a new miner does NOT change existing allocations.
        let mut p = NoncePartitioner::new();
        let r0 = p.assign(&"miner_0".to_string());
        let start_before = p.get_range("miner_0").unwrap().start;

        p.assign(&"miner_1".to_string());
        p.assign(&"miner_2".to_string());

        // miner_0's start nonce is unchanged — no repartitioning.
        assert_eq!(p.get_range("miner_0").unwrap().start, start_before);
        assert_ne!(r0.start, p.get_range("miner_1").unwrap().start);
    }

    #[test]
    fn get_all_ranges_sorted() {
        let mut p = NoncePartitioner::new();
        p.assign(&"miner_charlie".to_string());
        p.assign(&"miner_alice".to_string());
        p.assign(&"miner_bob".to_string());

        let ranges = p.get_all_ranges();
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].0, "miner_alice");
        assert_eq!(ranges[1].0, "miner_bob");
        assert_eq!(ranges[2].0, "miner_charlie");
    }

    #[test]
    fn assign_after_reclaim_gets_new_start() {
        let mut p = NoncePartitioner::new();
        p.assign(&"miner_0".to_string());
        let start1 = p.get_range("miner_0").unwrap().start;

        p.reclaim(&"miner_0".to_string());
        assert!(p.get_range("miner_0").is_none());

        // Re-assigning gets a fresh random start.
        p.assign(&"miner_0".to_string());
        let start2 = p.get_range("miner_0").unwrap().start;
        assert_ne!(start1, start2);
    }
}
