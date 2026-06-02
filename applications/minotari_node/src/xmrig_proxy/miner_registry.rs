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

use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Instant};

use tari_common_types::tari_address::TariAddress;
use tokio::sync::RwLock;
use log::{info, debug};

use super::MinerId;
use crate::xmrig_proxy::error::XmrigProxyError;

/// Tracks a single registered miner's identity and connection state.
#[derive(Clone, Debug)]
pub struct MinerEntry {
    /// Unique identifier (extra_nonce hex string).
    #[allow(dead_code)]
    pub id: MinerId,
    /// Remote socket address of the miner connection.
    #[allow(dead_code)]
    pub remote_addr: SocketAddr,
    /// Payment address for coinbase rewards (per-miner or config default).
    pub payment_address: TariAddress,
    /// Assigned nonce partition (filled by NoncePartitioner after registration).
    #[allow(dead_code)]
    pub nonce_range: std::ops::Range<u32>,
    /// Mining hash key of the template this miner is assigned to (if any).
    #[allow(dead_code)]
    pub assigned_template: Option<[u8; 32]>,
    /// When the miner first connected.
    #[allow(dead_code)]
    pub connected_at: Instant,
    /// Last time the miner sent a valid request.
    pub last_activity: Instant,
}

/// Configuration for the miner registry.
#[derive(Clone)]
pub struct MinerRegistryConfig {
    /// Default coinbase payment address when miners don't provide their own.
    pub default_payment_address: TariAddress,
    /// Maximum concurrent miners allowed.
    pub max_miners: usize,
    /// Seconds of inactivity before a miner is considered stale.
    pub miner_timeout_secs: u64,
    /// Whether miners may supply their own payment address.
    pub allow_custom_payment: bool,
    /// Minimum nonce range size per miner (used by NoncePartitioner, stored here for config centralization).
    #[allow(dead_code)]
    pub min_nonce_range_size: u32,
}

/// Registry of connected miners — the authoritative source for miner identity and state.
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

    /// Look up an existing miner or register a new one.
    ///
    /// Returns `Err(MaxMinersReached)` if the cap is exceeded and the miner is unknown.
    /// For known miners, updates `last_activity` and optionally `payment_address`.
    pub async fn get_or_register(
        &self,
        miner_id: &MinerId,
        remote_addr: SocketAddr,
        payment_address: Option<TariAddress>,
    ) -> Result<MinerEntry, XmrigProxyError> {
        let mut map = self.inner.write().await;

        if map.contains_key(miner_id) {
            // Known miner — refresh activity stamp
            if let Some(entry) = map.get_mut(miner_id) {
                entry.last_activity = Instant::now();

                // Update payment address if provided and allowed
                if let Some(addr) = payment_address
                    && self.config.allow_custom_payment
                {
                    entry.payment_address = addr;
                }
            }

            // Return a clone to avoid lifetime issues with the RwLock guard
            return Ok(map.get(miner_id).unwrap().clone());
        }

        // New miner — check capacity
        if map.len() >= self.config.max_miners {
            return Err(XmrigProxyError::MaxMinersReached(self.config.max_miners));
        }

        // Resolve payment address: use provided value or fall back to config default
        let resolved_address = if self.config.allow_custom_payment {
            payment_address.unwrap_or_else(|| self.config.default_payment_address.clone())
        } else {
            self.config.default_payment_address.clone()
        };

        let now = Instant::now();
        let entry = MinerEntry {
            id: miner_id.clone(),
            remote_addr,
            payment_address: resolved_address,
            nonce_range: 0..0, // placeholder — assigned by NoncePartitioner in step 3
            assigned_template: None,
            connected_at: now,
            last_activity: now,
        };

        let entry_clone = entry.clone();
        map.insert(miner_id.clone(), entry);

        Ok(entry_clone)
    }

    /// Remove a miner and return its entry (for nonce range reclamation).
    #[allow(dead_code)]
    pub async fn unregister(&self, miner_id: &MinerId) -> Option<MinerEntry> {
        self.inner.write().await.remove(miner_id)
    }

    /// Remove all miners inactive for longer than `config.miner_timeout_secs`.
    /// Returns the list of evicted miner IDs (for nonce range reclamation).
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    pub async fn len(&self) -> usize {
        self.inner.read().await.len()
    }

    /// Check if adding one more miner would exceed the cap.
    #[allow(dead_code)]
    pub async fn would_exceed_cap(&self) -> bool {
        self.inner.read().await.len() >= self.config.max_miners
    }

    /// Check whether a miner is currently registered.
    pub async fn is_registered(&self, miner_id: &MinerId) -> bool {
        self.inner.read().await.contains_key(miner_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config() -> MinerRegistryConfig {
        MinerRegistryConfig {
            default_payment_address: TariAddress::default(),
            max_miners: 32,
            miner_timeout_secs: 300,
            allow_custom_payment: true,
            min_nonce_range_size: 65536,
        }
    }

    fn dummy_addr() -> SocketAddr {
        "127.0.0.1:0".parse().unwrap()
    }

    #[tokio::test]
    async fn register_new_miner() {
        let registry = MinerRegistry::new(make_config());
        let entry = registry
            .get_or_register(&"miner1".to_string(), dummy_addr(), None)
            .await
            .unwrap();
        assert_eq!(entry.id, "miner1");
        assert_eq!(registry.len().await, 1);
    }

    #[tokio::test]
    async fn re_register_existing_miner_refreshes_activity() {
        let registry = MinerRegistry::new(make_config());
        let e1 = registry
            .get_or_register(&"m".to_string(), dummy_addr(), None)
            .await
            .unwrap();
        let old_activity = e1.last_activity;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let e2 = registry
            .get_or_register(&"m".to_string(), dummy_addr(), None)
            .await
            .unwrap();
        assert!(e2.last_activity > old_activity);
        assert_eq!(registry.len().await, 1);
    }

    #[tokio::test]
    async fn exceeds_max_miners() {
        let mut config = make_config();
        config.max_miners = 2;
        let registry = MinerRegistry::new(config);
        registry
            .get_or_register(&"a".to_string(), dummy_addr(), None)
            .await
            .unwrap();
        registry
            .get_or_register(&"b".to_string(), dummy_addr(), None)
            .await
            .unwrap();
        let err = registry
            .get_or_register(&"c".to_string(), dummy_addr(), None)
            .await
            .unwrap_err();
        assert!(matches!(err, XmrigProxyError::MaxMinersReached(2)));
    }

    #[tokio::test]
    async fn unregister_returns_entry() {
        let registry = MinerRegistry::new(make_config());
        registry
            .get_or_register(&"x".to_string(), dummy_addr(), None)
            .await
            .unwrap();
        let entry = registry.unregister(&"x".to_string()).await.unwrap();
        assert_eq!(entry.id, "x");
        assert_eq!(registry.len().await, 0);
    }

    #[tokio::test]
    async fn evict_stale_removes_inactive_miners() {
        let mut config = make_config();
        config.miner_timeout_secs = 1;
        let registry = MinerRegistry::new(config);
        registry
            .get_or_register(&"old".to_string(), dummy_addr(), None)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let evicted = registry.evict_stale().await;
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0], "old");
        assert_eq!(registry.len().await, 0);
    }

    #[tokio::test]
    async fn would_exceed_cap() {
        let mut config = make_config();
        config.max_miners = 1;
        let registry = MinerRegistry::new(config);
        assert!(!registry.would_exceed_cap().await);
        registry
            .get_or_register(&"a".to_string(), dummy_addr(), None)
            .await
            .unwrap();
        assert!(registry.would_exceed_cap().await);
    }
}
