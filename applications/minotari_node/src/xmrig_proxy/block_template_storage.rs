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

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use log::{debug, info};
use tari_common_types::{tari_address::TariAddress, types::BlockHash};
use tari_node_components::blocks::Block;
use tari_transaction_components::tari_proof_of_work::PowAlgorithm;
use tari_utilities::ByteArray;
use tokio::sync::RwLock;

use super::MinerId;

const LOG_TARGET: &str = "minotari::base_node::xmrig_proxy::storage";

/// Tracks the last known chain tip so we can detect when Tari's state has advanced.
#[derive(Clone, Copy, Debug, Default)]
pub struct ChainTip {
    pub height: u64,
    pub top_hash: BlockHash,
}

impl ChainTip {
    /// Returns `true` if the given tip represents an advance over this one.
    fn is_advanced_by(&self, other: &ChainTip) -> bool {
        other.height > self.height || (other.height == self.height && other.top_hash != self.top_hash)
    }

    /// Returns true if this chain tip has been initialized to a non-default value.
    fn is_initialized(&self) -> bool {
        self.height > 0 || !self.top_hash.as_bytes().iter().all(|&b| b == 0)
    }
}

/// Entry stored in the template cache.
#[derive(Clone)]
pub struct TemplateEntry {
    pub block: Block,
    pub wallet_address: TariAddress,
    pub assigned_miners: HashSet<MinerId>,
    /// Difficulty from the node's block template (used for miner responses).
    pub target_difficulty: u64,
    /// PoW algorithm used to generate this template.
    pub pow_algo: PowAlgorithm,
}

/// Thread-safe in-memory store for block templates, keyed by the 32-byte mining hash.
///
/// Chain tip advances (new blocks found or reorgs) are tracked to detect stale caches early.
#[derive(Clone)]
pub struct BlockTemplateStorage {
    inner: Arc<RwLock<HashMap<[u8; 32], TemplateEntry>>>,
    /// Last known chain tip — updated whenever we fetch a fresh template from Tari.
    last_known_tip: Arc<RwLock<ChainTip>>,
}

impl BlockTemplateStorage {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            last_known_tip: Arc::new(RwLock::new(ChainTip::default())),
        }
    }

    /// Update the stored chain tip to the given value.
    /// Returns `true` if the new tip represents an advance (height increased or top_hash changed).
    /// The first call always returns `false` (initialization from default is not an "advance").
    pub async fn update_chain_tip(&self, tip: ChainTip) -> bool {
        let mut stored = self.last_known_tip.write().await;
        if !stored.is_initialized() {
            *stored = tip;
            return false;
        }
        let advanced = stored.is_advanced_by(&tip);
        *stored = tip;
        advanced
    }

    /// Store a block template. If a template with the same key already exists it is replaced.
    pub async fn store(
        &self,
        key: [u8; 32],
        block: Block,
        wallet_address: TariAddress,
        miner_id: MinerId,
        target_difficulty: u64,
        pow_algo: PowAlgorithm,
    ) {
        info!(target: LOG_TARGET, "Storing template for address {} and miner ID {}", wallet_address.clone(), miner_id.clone());
        let mut map = self.inner.write().await;
        map.insert(
            key,
            TemplateEntry {
                block,
                wallet_address,
                assigned_miners: {
                    let mut set = HashSet::new();
                    set.insert(miner_id.clone());
                    set
                },
                target_difficulty,
                pow_algo,
            },
        );
        debug!(target: LOG_TARGET, "Stored template, total templates={}", map.len());
    }

    /// Look up a cached template by wallet address.
    ///
    /// Returns `Some((key, TemplateEntry))` if a template for the given address exists.
    /// Returns `None` if no match is found.
    pub async fn get_for_address(&self, wallet_address: &TariAddress) -> Option<([u8; 32], TemplateEntry)> {
        let map = self.inner.read().await;

        for (key, entry) in map.iter() {
            if entry.wallet_address == *wallet_address {
                return Some((*key, entry.clone()));
            }
        }
        None
    }

    /// Add a new miner to an existing template entry (template caching hit).
    ///
    /// Returns `true` if the template was found and the miner was added.
    pub async fn add_miner_to_template(&self, key: [u8; 32], miner_id: MinerId) -> bool {
        debug!(target: LOG_TARGET, "Cache hit for miner ID {}", miner_id.clone());
        let mut map = self.inner.write().await;
        if let Some(entry) = map.get_mut(&key) {
            entry.assigned_miners.insert(miner_id.clone());
            true
        } else {
            false
        }
    }

    /// Retrieve a clone of the stored block for the given mining hash (without removing it).
    pub async fn get(&self, mining_hash: &[u8; 32]) -> Option<Block> {
        let map = self.inner.read().await;
        map.get(mining_hash).map(|entry| entry.block.clone())
    }

    /// Retrieve a clone of the full template entry for the given mining hash.
    // TODO: wire into handle_submit_block for identity verification (S2) — check assigned_miners
    #[allow(dead_code)]
    pub async fn get_entry(&self, mining_hash: &[u8; 32]) -> Option<TemplateEntry> {
        let map = self.inner.read().await;
        map.get(mining_hash).cloned()
    }

    /// Retrieve the target difficulty for a stored template.
    pub async fn get_target_difficulty(&self, mining_hash: &[u8; 32]) -> Option<u64> {
        let map = self.inner.read().await;
        map.get(mining_hash).map(|entry| entry.target_difficulty)
    }

    /// Retrieve and remove a block template by its mining hash key.
    pub async fn take(&self, key: &[u8; 32]) -> Option<Block> {
        let mut map = self.inner.write().await;
        map.remove(key).map(|e| e.block)
    }

    /// Evict cached templates for a specific PoW algorithm. Called when Tari's chain tip advances
    /// so that stale templates are not served to miners on the next request. Only evicts templates
    /// whose `pow_algo` matches the given algorithm — templates from other algorithms remain valid.
    ///
    /// Returns the set of all miner IDs associated with evicted entries for future nonce-partitioning cleanup.
    pub async fn evict_for_algorithm(&self, algo: PowAlgorithm) -> HashSet<MinerId> {
        let mut map = self.inner.write().await;
        let before = map.len();

        // Collect assigned_miners before eviction so we can reclaim their nonce ranges if we implement range assignments.
        let evicted_miners: HashSet<MinerId> = map
            .iter()
            .filter(|(_, e)| e.pow_algo == algo)
            .flat_map(|(_, e)| e.assigned_miners.iter().cloned())
            .collect();

        // Then evict matching templates.
        map.retain(|_, e| e.pow_algo != algo);
        let removed = before.saturating_sub(map.len());
        if removed > 0 {
            debug!(target: LOG_TARGET, "Evicted {} cached templates for algorithm {:?} (chain tip advanced)", removed, algo);
        }
        evicted_miners
    }
}

impl Default for BlockTemplateStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tari_node_components::blocks::BlockBuilder;

    fn make_test_block() -> Block {
        BlockBuilder::new(1).build()
    }

    fn make_test_key() -> [u8; 32] {
        [42u8; 32]
    }

    #[tokio::test]
    async fn store_and_get_block() {
        let storage = BlockTemplateStorage::new();
        let key = make_test_key();
        let block = make_test_block();
        let address = TariAddress::default();
        let miner = "miner_1".to_string();

        storage
            .store(key, block.clone(), address, miner, 1, PowAlgorithm::RandomXT)
            .await;

        let retrieved = storage.get(&key).await.unwrap();
        assert_eq!(retrieved, block);
    }

    #[tokio::test]
    async fn get_returns_none_for_missing_key() {
        let storage = BlockTemplateStorage::new();
        let key = make_test_key();
        assert!(storage.get(&key).await.is_none());
    }

    #[tokio::test]
    async fn take_removes_block() {
        let storage = BlockTemplateStorage::new();
        let key = make_test_key();
        let block = make_test_block();
        let address = TariAddress::default();

        storage
            .store(key, block.clone(), address, "m".to_string(), 1, PowAlgorithm::RandomXT)
            .await;

        let taken = storage.take(&key).await.unwrap();
        assert_eq!(taken, block);
        assert!(storage.get(&key).await.is_none());
    }

    #[tokio::test]
    async fn take_returns_none_when_absent() {
        let storage = BlockTemplateStorage::new();
        assert!(storage.take(&make_test_key()).await.is_none());
    }

    #[tokio::test]
    async fn get_for_address_returns_template() {
        let storage = BlockTemplateStorage::new();
        let key = make_test_key();
        let address = TariAddress::default();

        storage
            .store(
                key,
                make_test_block(),
                address.clone(),
                "m".to_string(),
                1,
                PowAlgorithm::RandomXT,
            )
            .await;

        let (found_key, entry) = storage.get_for_address(&address).await.unwrap();
        assert_eq!(found_key, key);
        assert_eq!(entry.wallet_address, address);
    }

    #[tokio::test]
    async fn store_replaces_existing_template() {
        let storage = BlockTemplateStorage::new();
        let key = make_test_key();
        let address = TariAddress::default();

        storage
            .store(
                key,
                make_test_block(),
                address.clone(),
                "m1".to_string(),
                1,
                PowAlgorithm::RandomXT,
            )
            .await;
        storage
            .store(
                key,
                make_test_block(),
                address,
                "m2".to_string(),
                1,
                PowAlgorithm::RandomXT,
            )
            .await;

        let entry = storage.get_entry(&key).await.unwrap();
        assert!(!entry.assigned_miners.contains("m1"));
        assert!(entry.assigned_miners.contains("m2"));
    }

    #[tokio::test]
    async fn update_chain_tip_returns_true_on_height_advance() {
        let storage = BlockTemplateStorage::new();
        let tip1 = ChainTip {
            height: 10,
            top_hash: BlockHash::default(),
        };
        let tip2 = ChainTip {
            height: 11,
            top_hash: BlockHash::default(),
        };

        assert!(!storage.update_chain_tip(tip1).await); // first update is always "no advance" from default
        assert!(storage.update_chain_tip(tip2).await);
    }

    #[tokio::test]
    async fn update_chain_tip_returns_true_on_hash_change() {
        let storage = BlockTemplateStorage::new();
        let tip1 = ChainTip {
            height: 10,
            top_hash: [0u8; 32].into(),
        };
        let tip2 = ChainTip {
            height: 10,
            top_hash: [1u8; 32].into(),
        };

        storage.update_chain_tip(tip1).await;
        assert!(storage.update_chain_tip(tip2).await); // same height, different hash
    }

    #[tokio::test]
    async fn update_chain_tip_returns_false_on_same_tip() {
        let storage = BlockTemplateStorage::new();
        let tip = ChainTip {
            height: 10,
            top_hash: [42u8; 32].into(),
        };

        assert!(!storage.update_chain_tip(tip).await); // first update from default
        assert!(!storage.update_chain_tip(tip).await); // same tip again
    }

    #[tokio::test]
    async fn evict_for_algorithm_clears_matching() {
        let storage = BlockTemplateStorage::new();
        let key1 = [1u8; 32];
        let key2 = [2u8; 32];
        let address = TariAddress::default();

        // Store a RandomXT template and a Sha3x template.
        // In practice we only store RandomXT templates, so this test exercises the
        // per-algorithm filtering path that would matter if multi-algo support is added later.
        storage
            .store(
                key1,
                make_test_block(),
                address.clone(),
                "m".to_string(),
                1,
                PowAlgorithm::RandomXT,
            )
            .await;
        storage
            .store(
                key2,
                make_test_block(),
                address,
                "m".to_string(),
                1,
                PowAlgorithm::Sha3x,
            )
            .await;

        assert!(storage.get(&key1).await.is_some());
        assert!(storage.get(&key2).await.is_some());

        // Evict only RandomXT templates
        storage.evict_for_algorithm(PowAlgorithm::RandomXT).await;

        assert!(storage.get(&key1).await.is_none()); // RandomXT removed
        assert!(storage.get(&key2).await.is_some()); // Sha3x preserved
    }

    #[tokio::test]
    async fn evict_for_algorithm_does_not_affect_chain_tip() {
        let storage = BlockTemplateStorage::new();
        let tip = ChainTip {
            height: 42,
            top_hash: [99u8; 32].into(),
        };
        storage.update_chain_tip(tip).await;

        storage.evict_for_algorithm(PowAlgorithm::RandomXT).await;

        // Verify the chain tip survived evict_for_algorithm by checking that a subsequent
        // update is still detected as an advance. If eviction had cleared the stored tip,
        // this call would return false (reset from default) instead of true.
        let new_tip = ChainTip {
            height: 43,
            top_hash: [99u8; 32].into(),
        };
        assert!(storage.update_chain_tip(new_tip).await);
    }

    #[tokio::test]
    async fn evict_for_algorithm_returns_empty_when_no_match() {
        let storage = BlockTemplateStorage::new();
        let key = [1u8; 32];
        let address = TariAddress::default();

        // Store a Sha3x template — RandomXT eviction should return empty.
        storage
            .store(
                key,
                make_test_block(),
                address.clone(),
                "m".to_string(),
                1,
                PowAlgorithm::Sha3x,
            )
            .await;

        let evicted = storage.evict_for_algorithm(PowAlgorithm::RandomXT).await;
        assert!(evicted.is_empty());
    }

    #[tokio::test]
    async fn evict_for_algorithm_preserves_other_algorithms() {
        let storage = BlockTemplateStorage::new();
        let key_rx = [1u8; 32];
        let key_cuckaroo = [2u8; 32];
        let address = TariAddress::default();

        // Store templates for different algorithms
        storage
            .store(
                key_rx,
                make_test_block(),
                address.clone(),
                "m".to_string(),
                1,
                PowAlgorithm::RandomXT,
            )
            .await;
        storage
            .store(
                key_cuckaroo,
                make_test_block(),
                address,
                "m".to_string(),
                1,
                PowAlgorithm::Cuckaroo,
            )
            .await;

        // Evict RandomXT — Cuckaroo should survive
        storage.evict_for_algorithm(PowAlgorithm::RandomXT).await;

        assert!(storage.get(&key_rx).await.is_none());
        assert!(storage.get(&key_cuckaroo).await.is_some());
    }
}
