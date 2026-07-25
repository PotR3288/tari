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

use std::{collections::HashMap, sync::Arc, time::Instant};

use tari_common_types::tari_address::TariAddress;
use tokio::sync::RwLock;

use super::MinerId;
use crate::xmrig_proxy::error::XmrigProxyError;

/// Tracks a single registered miner's identity and connection state.
#[derive(Clone, Debug)]
pub struct MinerEntry {
    /// Payment address for coinbase rewards (per-miner or config default).
    pub payment_address: TariAddress,
    /// Last time the miner sent a valid request.
    pub last_activity: Instant,
}

/// Configuration for the miner registry.
#[derive(Clone)]
pub struct MinerRegistryConfig {
    /// Maximum concurrent miners allowed.
    pub max_miners: usize,
    /// Seconds of inactivity before a miner is considered stale.
    pub miner_timeout_secs: u64,
}

/// Registry of connected miners — the authoritative source for miner identity and state.
///
/// Keyed by resolved payment address (wallet address string). This means all miners
/// paying to the same wallet share one registry entry, which aligns with template caching:
/// multiple miners with the same wallet get one cached block template instead of generating
/// separate ones. The max_miners cap limits unique payment addresses rather than IP+wallet combos.
#[derive(Clone)]
pub struct MinerRegistry {
    inner: Arc<RwLock<HashMap<MinerId, MinerEntry>>>,
    config: MinerRegistryConfig,
}

impl MinerRegistry {
    /// Create a new registry with the given configuration.
    pub fn new(config: MinerRegistryConfig) -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Look up an existing miner or register a new one by resolved payment address.
    ///
    /// The `payment_address` is the already-resolved wallet address (miner-provided if valid,
    /// otherwise the config default). This means all miners paying to the same address share
    /// one registry entry and one cached template.
    ///
    /// Returns `Err(MaxMinersReached)` if the cap is exceeded and the miner is unknown.
    /// For known miners, updates `last_activity`.
    pub async fn get_or_register(&self, payment_address: &TariAddress) -> Result<MinerEntry, XmrigProxyError> {
        let mut map = self.inner.write().await;

        // Check if any existing entry has the same payment address (dedup by wallet).
        for (_key, entry) in map.iter_mut() {
            if &entry.payment_address == payment_address {
                entry.last_activity = Instant::now();
                return Ok(entry.clone());
            }
        }

        // New miner — check capacity.
        if map.len() >= self.config.max_miners {
            return Err(XmrigProxyError::MaxMinersReached(self.config.max_miners));
        }

        let now = Instant::now();
        let entry = MinerEntry {
            payment_address: payment_address.clone(),
            last_activity: now,
        };

        // Use the wallet address string as the key.
        let key = payment_address.to_string();
        let entry_clone = entry.clone();
        map.insert(key, entry);

        Ok(entry_clone)
    }

    /// Remove all miners inactive for longer than `config.miner_timeout_secs`.
    pub async fn evict_stale(&self) -> Vec<MinerId> {
        let mut map = self.inner.write().await;
        let timeout = std::time::Duration::from_secs(self.config.miner_timeout_secs);
        let cutoff = Instant::now().checked_sub(timeout).unwrap_or(Instant::now());

        let stale_ids: Vec<MinerId> = map
            .iter()
            .filter(|(_, entry)| entry.last_activity < cutoff)
            .map(|(id, _)| id.clone())
            .collect();

        for id in &stale_ids {
            map.remove(id);
        }

        stale_ids
    }

    /// Get current miner count.
    // TODO: expose via getinfo or metrics endpoint
    #[allow(dead_code)]
    pub async fn len(&self) -> usize {
        self.inner.read().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config() -> MinerRegistryConfig {
        MinerRegistryConfig {
            max_miners: 32,
            miner_timeout_secs: 300,
        }
    }

    #[tokio::test]
    async fn register_new_miner() {
        let registry = MinerRegistry::new(make_config());
        let addr = TariAddress::default();
        let entry = registry.get_or_register(&addr).await.unwrap();
        assert_eq!(entry.payment_address, addr);
    }

    #[tokio::test]
    async fn same_payment_address_returns_existing_entry() {
        let registry = MinerRegistry::new(make_config());
        let addr = TariAddress::default();
        let e1 = registry.get_or_register(&addr).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let e2 = registry.get_or_register(&addr).await.unwrap();
        // Same entry — activity refreshed
        assert!(e2.last_activity > e1.last_activity);
        // Registry still has only 1 entry (dedup by wallet address)
        assert_eq!(registry.len().await, 1);
    }

    #[tokio::test]
    async fn evict_stale_removes_inactive_miners() {
        let mut config = make_config();
        config.miner_timeout_secs = 1;
        let registry = MinerRegistry::new(config);
        let addr = TariAddress::default();
        registry.get_or_register(&addr).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        // evict_stale is called by the periodic cleanup task in mod.rs (every 10 min)
        let reg_ids = registry.evict_stale().await;
        assert_eq!(reg_ids.len(), 1);
    }

    #[tokio::test]
    async fn len_returns_current_miner_count() {
        use tari_common_types::tari_address::TARI_ADDRESS_INTERNAL_SINGLE_SIZE;

        let registry = MinerRegistry::new(make_config());
        assert_eq!(registry.len().await, 0);

        // Create two distinct addresses by setting different network bytes.
        fn make_addr(network_byte: u8) -> TariAddress {
            use tari_common_types::dammsum::compute_checksum;
            let mut buf = [0u8; TARI_ADDRESS_INTERNAL_SINGLE_SIZE];
            buf[0] = network_byte; // set network byte
            buf[34] = compute_checksum(&buf[0..34]);
            TariAddress::from_bytes(&buf).expect("valid address bytes")
        }

        let addr1 = make_addr(0x10); // LocalNet network byte
        let addr2 = make_addr(0x24); // Igor network byte → distinct address

        registry.get_or_register(&addr1).await.unwrap();
        assert_eq!(registry.len().await, 1);

        registry.get_or_register(&addr2).await.unwrap();
        assert_eq!(registry.len().await, 2);

        // Registering the same address again should not increase count.
        registry.get_or_register(&addr1).await.unwrap();
        assert_eq!(registry.len().await, 2);
    }
}
