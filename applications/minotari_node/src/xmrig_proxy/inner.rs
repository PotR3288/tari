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

use std::{net::SocketAddr, str::FromStr, sync::atomic::{AtomicU64, Ordering}, sync::Arc};

use hyper::{Response, StatusCode, body::Bytes};
use log::{debug, info, trace, warn};
use serde_json::{Value, json};
use tari_common::configuration::Network;
use tari_common_types::{
    tari_address::TariAddress,
    types::{
        CompressedCommitment, CompressedPublicKey, CompressedSignature, UncompressedCommitment,
        UncompressedPublicKey,
    },
};
use tari_core::{
    base_node::{LocalNodeCommsInterface, StateMachineHandle},
    consensus::BaseNodeConsensusManager,
    validation::tari_rx_vm_key_height,
};
use tari_transaction_components::{
    generate_coinbase_with_wallet_output,
    key_manager::{KeyManager, TariKeyId, TransactionKeyManagerInterface, TxoStage},
    tari_proof_of_work::PowAlgorithm,
    transaction_components::{
        CoinBaseExtra, KernelBuilder, RangeProofType, TransactionKernel, TransactionKernelVersion,
        memo_field::{MemoField, TxType},
    },
};
use tari_utilities::ByteArray;
use tokio::sync::RwLock;

use super::{
    MinerId,
    block_template_storage::{BlockTemplateStorage, ChainTip},
    error::XmrigProxyError,
    miner_registry::MinerRegistry,
    nonce_partition::NoncePartitioner,
    service::{ProxyBody, json_response},
};

const LOG_TARGET: &str = "minotari::base_node::xmrig_proxy";

/// The byte offset in the 76-byte mining blob where the extra nonce starts.
/// XMRig writes a per-thread extra nonce here so mining threads don't duplicate work.
/// This corresponds to the high 4 bytes of the u64 nonce field.
pub const TARI_BLOB_RESERVED_OFFSET: u32 = 35;

/// Offset where the mining hash starts in the blob (after 3 zero padding bytes).
const TARI_HASH_OFFSET: usize = 3;

/// Size of the nonce field in bytes.
const TARI_NONCE_SIZE: usize = 8;

/// The total size of the Tari mining blob in bytes.
const TARI_MINING_BLOB_SIZE: usize = 76;
/// The pow_algo byte value for RandomXT (= 2).
const POW_ALGO_RANDOMXT: u8 = 2;

#[derive(Clone)]
pub struct InnerService {
    pub node_service: LocalNodeCommsInterface,
    pub consensus_rules: BaseNodeConsensusManager,
    /// State machine handle, available for future sync-status checks.
    // TODO: use for sync status check before serving templates
    #[allow(dead_code)]
    pub state_machine: StateMachineHandle,
    pub block_templates: BlockTemplateStorage,
    pub wallet_payment_address: TariAddress,
    /// The node's network — used to validate miner-provided payment addresses.
    pub network: Network,
    pub coinbase_extra: Vec<u8>,
    pub range_proof_type: RangeProofType,
    pub miner_registry: MinerRegistry,
    pub nonce_partitioner: Arc<RwLock<NoncePartitioner>>,
    /// The remote socket address of the miner that opened this connection.
    pub peer_addr: SocketAddr,
}

/// Extract miner identity from a JSON-RPC request and peer address.
///
/// Used for nonce partitioning — each connection gets a unique ID so threads
/// don't search overlapping nonce ranges.
///
/// Two-layer resolution:
/// 1. `params.extra_nonce` — per-connection random nonce sent by XMRig (Tari fork).
///    Guarantees unique miner IDs even when multiple instances share the same wallet.
/// 2. `peer_addr` — fall back to the remote socket address (IP:port) of the TCP connection.
/// 3. Auto-generated — monotonic counter + timestamp (last resort).
fn parse_miner_id_from_request(req: &Value, peer_addr: SocketAddr) -> MinerId {
    // Layer 1: per-connection extra_nonce from XMRig (Tari fork).
    // This is the correct identity for nonce partitioning — each XMRig instance
    // generates unique random bytes, so two miners with the same wallet get
    // distinct IDs and non-overlapping nonce ranges.
    if let Some(extra_nonce) = req
        .get("params")
        .and_then(|p| p.get("extra_nonce"))
        .and_then(Value::as_str)
    {
        return extra_nonce.to_string();
    }

    // Layer 2: peer socket address (IP:port) — distinguishes miners behind NAT
    // sharing a wallet but on different local ports.
    if peer_addr != SocketAddr::from(([0, 0, 0, 0], 0)) {
        return peer_addr.to_string();
    }

    // Layer 3: auto-generated unique ID — monotonic counter guarantees uniqueness
    // even when calls land in the same millisecond.
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    format!(
        "auto_{}_{}",
        id,
        std::time::SystemTime::now()
            .elapsed()
            .map(|d| d.as_millis())
            .unwrap_or(0)
    )
}

/// Extract a registration ID for the MinerRegistry.
///
/// Used to deduplicate miners in the registry (max_miners counting). Multiple TCP
/// connections from the same physical miner — whether multi-threaded or behind NAT —
/// share one registration entry keyed by IP + wallet address.
///
/// Resolution:
/// 1. `params.wallet_address` — if present, use its base58 string as part of the ID.
/// 2. Peer IP (without port) — fallback for clients that don't provide a wallet address.
fn parse_registration_id(req: &Value, peer_addr: SocketAddr) -> String {
    // Layer 1: wallet address from request
    if let Some(addr) = parse_wallet_address_from_request(req) {
        return format!("{}:{}", peer_addr.ip(), addr);
    }

    // Layer 2: peer IP (without port) — fallback for clients that don't provide a wallet address.
    if peer_addr != SocketAddr::from(([0, 0, 0, 0], 0)) {
        return format!("{}:default", peer_addr.ip());
    }

    // Layer 3: auto-generated unique ID — monotonic counter guarantees uniqueness
    static NEXT_REG_ID: AtomicU64 = AtomicU64::new(0);
    let id = NEXT_REG_ID.fetch_add(1, Ordering::Relaxed);
    format!(
        "auto_{}_{}",
        id,
        std::time::SystemTime::now()
            .elapsed()
            .map(|d| d.as_millis())
            .unwrap_or(0)
    )
}

/// Extract an optional wallet address override from a JSON-RPC request.
fn parse_wallet_address_from_request(req: &Value) -> Option<TariAddress> {
    let address_str = req
        .get("params")
        .and_then(|p| p.get("wallet_address"))
        .and_then(|v| v.as_str())?;
    TariAddress::from_str(address_str).ok()
}

impl InnerService {
    /// Handle a JSON-RPC request from the miner.
    pub async fn handle(&self, body: Bytes) -> Result<Response<ProxyBody>, XmrigProxyError> {
        let json: Value = serde_json::from_slice(&body)?;
        let method = json.get("method").and_then(Value::as_str).unwrap_or("");
        trace!(target: LOG_TARGET, "Received method: {method}");
        match method {
            "getblocktemplate" => self.handle_get_block_template(&json).await,
            "submitblock" => self.handle_submit_block(&json).await,
            "getblockcount" | "get_height" => self.handle_get_height(&json).await,
            "getheight" => self.handle_get_height_hash().await,
            "getinfo" => self.handle_get_info().await,
            _ => {
                debug!(target: LOG_TARGET, "Unknown method: {method}");
                json_response(
                    StatusCode::OK,
                    &json_rpc_error(
                        json.get("id").map(|v| v.as_i64()).unwrap_or_default(),
                        -32601,
                        "Method not found",
                    ),
                )
            },
        }
    }

    /// Fetch the current chain tip height and block hash from the node.
    async fn get_chain_tip(&self) -> Result<ChainTip, XmrigProxyError> {
        let mut handler = self.node_service.clone();
        let meta = handler.get_metadata().await?;
        Ok(ChainTip {
            height: meta.best_block_height(),
            top_hash: *meta.best_block_hash(),
        })
    }

    /// Handle GET /get_height, /getinfo, /getheight requests (some mining software uses these).
    pub async fn handle_get(&self, path: &str) -> Result<Response<ProxyBody>, XmrigProxyError> {
        match path {
            "/get_height" | "/getblockcount" => self.handle_get_height(&json!({})).await,
            "/getheight" => self.handle_get_height_hash().await,
            "/getinfo" | "/get_info" => self.handle_get_info().await,
            _ => json_response(StatusCode::NOT_FOUND, &json!({"error": "Not found"})),
        }
    }

    async fn handle_get_height(&self, req: &Value) -> Result<Response<ProxyBody>, XmrigProxyError> {
        let tip = self.get_chain_tip().await?;
        json_response(
            StatusCode::OK,
            &json_rpc_success(
                req["id"].get("id").map(|v| v.as_i64()).unwrap_or_default(),
                json!({ "count": tip.height, "status": "OK" }),
            ),
        )
    }

    async fn handle_get_height_hash(&self) -> Result<Response<ProxyBody>, XmrigProxyError> {
        let tip = self.get_chain_tip().await?;
        json_response(
            StatusCode::OK,
            &json!({
                "height": tip.height,
                "hash": format!("{}", tip.top_hash),
                "status": "OK",
            }),
        )
    }

    async fn handle_get_info(&self) -> Result<Response<ProxyBody>, XmrigProxyError> {
        let tip = self.get_chain_tip().await?;
        json_response(
            StatusCode::OK,
            &json!({
                "top_block_hash": format!("{}", tip.top_hash),
                "height": tip.height,
                "status": "OK",
            }),
        )
    }

    #[allow(clippy::too_many_lines)]
    async fn handle_get_block_template(&self, req: &Value) -> Result<Response<ProxyBody>, XmrigProxyError> {
        // 1. Parse miner identity from request (two-layer: extra_nonce > peer_addr for nonce partitioning).
        let miner_id = parse_miner_id_from_request(req, self.peer_addr);

        // 2. Parse registration ID for MinerRegistry deduplication (IP + wallet_address).
        let registration_id = parse_registration_id(req, self.peer_addr);

        let requested_wallet_address = parse_wallet_address_from_request(req);
        let payment_address = match &requested_wallet_address {
            Some(addr) if addr.network() == self.network => requested_wallet_address.clone().unwrap(),
            Some(addr) => {
                debug!(
                    target: LOG_TARGET,
                    "Miner-provided address network '{}' does not match node network '{}'; falling back to config default (fallback active)",
                    addr.network(),
                    self.network
                );
                self.wallet_payment_address.clone()
            },
            None => {
                debug!(
                    target: LOG_TARGET,
                    "No wallet address provided by miner; using config default (fallback active)"
                );
                self.wallet_payment_address.clone()
            },
        };

        // 3. Register or refresh miner (uses registration_id for dedup).
        self.miner_registry
            .get_or_register(&registration_id, requested_wallet_address.clone())
            .await?;

        // 3b. Detect chain tip advance — if Tari's state has moved on, evict stale caches.
        let mut handler = self.node_service.clone();
        let meta = handler.get_metadata().await?;
        let current_tip = ChainTip {
            height: meta.best_block_height(),
            top_hash: *meta.best_block_hash(),
        };
        let advanced = self.block_templates.update_chain_tip(current_tip).await;
        if advanced {
            // Only evict RandomXT templates when a RandomXT block advances the tip.
            // Non-RandomXT blocks don't invalidate our cached template since we only
            // mine RandomXT and its target interval (480s) is much longer than other lanes.
            let new_block_algo = handler.get_metadata().await.ok().and_then(|m| {
                m.best_block_height()
                    .checked_sub(1)
                    .map(|h| h)
            });
            let should_evict = if let Some(height) = new_block_algo {
                match handler.get_header(height).await {
                    Ok(Some(header)) => header.header().pow.pow_algo == PowAlgorithm::RandomXT,
                    _ => false,
                }
            } else {
                false
            };
            if should_evict {
                debug!(target: LOG_TARGET, "Chain tip advanced to height #{} (hash {}), evicting RandomXT templates", current_tip.height, current_tip.top_hash);
                let evicted_miner_ids = self.block_templates.evict_for_algorithm(PowAlgorithm::RandomXT).await;

                // Reclaim nonce ranges for miners associated with evicted templates.
                // Active miners whose templates survived (non-RandomXT tip advance) keep their ranges.
                if !evicted_miner_ids.is_empty() {
                    let reclaimed = self.nonce_partitioner.write().await.reclaim_all(&evicted_miner_ids);
                    debug!(target: LOG_TARGET, "Reclaimed {} nonce ranges after template eviction", reclaimed);
                }
            } else {
                debug!(target: LOG_TARGET, "Chain tip advanced to height #{} (hash {}) by non-RandomXT block, keeping RandomXT templates", current_tip.height, current_tip.top_hash);
            }
        }

        let next_height = meta.best_block_height().saturating_add(1);
        if let Some((cached_key, _cached_entry)) = self.block_templates.get_for_address(&payment_address).await {
            let nonce_range = self.nonce_partitioner.write().await.assign(&miner_id);

            // Record the mapping so eviction can reclaim this nonce range later.
            self.miner_registry
                .register_nonce_partitioner_id(&registration_id, &miner_id)
                .await;

            self.block_templates
                .add_miner_to_template(cached_key, miner_id.clone(), nonce_range)
                .await;

            debug!(
                target: LOG_TARGET,
                "Cached template hit for miner {miner_id}, address {}",
                payment_address
            );

            return self.build_template_response(&cached_key, &miner_id, req).await;
        }

        // 4. No cached template — generate a new one
        let constants = self.consensus_rules.consensus_constants(next_height);
        let asking_weight = constants.max_block_transaction_weight();

        // Get a new RandomXT block template from the local node
        let mut new_template = handler
            .get_new_block_template(PowAlgorithm::RandomXT, asking_weight)
            .await
            .map_err(|e| {
                warn!(target: LOG_TARGET, "Failed to get block template: {e}");
                e
            })?;

        let height = new_template.header.height;
        // Capture target_difficulty from the template before it's consumed by get_new_block
        let target_difficulty = new_template.target_difficulty.as_u64();

        // Calculate the coinbase reward for this block
        let reward = self
            .consensus_rules
            .calculate_coinbase_and_fees(height, new_template.body.kernels())
            .map_err(|e| XmrigProxyError::InternalError(e.to_string()))?
            .as_u64();

        // Validate coinbase count
        let max_coinbases = self
            .consensus_rules
            .consensus_constants(height)
            .max_block_coinbase_count();
        if 1 > max_coinbases {
            return Err(XmrigProxyError::InternalError(
                "No coinbases allowed by consensus".to_string(),
            ));
        }

        // Generate the coinbase output and kernel for the payment address
        let coinbase_extra = CoinBaseExtra::try_from(self.coinbase_extra.clone())
            .map_err(|e| XmrigProxyError::InternalError(e.to_string()))?;
        let key_manager = KeyManager::new_random().map_err(|e| XmrigProxyError::InternalError(e.to_string()))?;
        let script_key_id = TariKeyId::default();

        let (_, coinbase_output, coinbase_kernel, wallet_output) = generate_coinbase_with_wallet_output(
            0.into(),
            reward.into(),
            height,
            &coinbase_extra,
            &key_manager,
            &script_key_id,
            &payment_address,
            false, // stealth_payment
            constants,
            self.range_proof_type,
            MemoField::new_open(vec![], TxType::Coinbase).expect("empty user-data should always be valid"),
        )
        .map_err(|e| XmrigProxyError::InternalError(e.to_string()))?;

        new_template.body.add_output(coinbase_output);

        // Build the kernel signature
        let new_nonce = key_manager
            .get_random_key(None, None)
            .map_err(|e| XmrigProxyError::InternalError(e.to_string()))?;
        let total_nonce: UncompressedPublicKey = new_nonce
            .pub_key
            .to_public_key()
            .map_err(|e| XmrigProxyError::InternalError(e.to_string()))?;
        let total_excess: UncompressedCommitment = coinbase_kernel
            .excess
            .to_commitment()
            .map_err(|e| XmrigProxyError::InternalError(e.to_string()))?;
        let kernel_message = TransactionKernel::build_kernel_signature_message(
            TransactionKernelVersion::get_current_version(),
            coinbase_kernel.fee,
            coinbase_kernel.lock_height,
            &coinbase_kernel.features,
            &None,
        );
        let kernel_signature = key_manager
            .get_partial_txo_kernel_signature(
                wallet_output.commitment_mask_key_id(),
                &new_nonce.key_id,
                &CompressedPublicKey::new_from_pk(total_nonce),
                &CompressedPublicKey::new_from_pk(total_excess.as_public_key().clone()),
                TransactionKernelVersion::get_current_version(),
                &kernel_message,
                &coinbase_kernel.features,
                TxoStage::Output,
            )
            .map_err(|e| XmrigProxyError::InternalError(e.to_string()))?
            .to_schnorr_signature()
            .map_err(|e| XmrigProxyError::InternalError(e.to_string()))?;

        let kernel_new = KernelBuilder::new()
            .with_fee(0.into())
            .with_features(coinbase_kernel.features)
            .with_lock_height(coinbase_kernel.lock_height)
            .with_excess(&CompressedCommitment::from_commitment(
                coinbase_kernel
                    .excess
                    .to_commitment()
                    .map_err(|e| XmrigProxyError::InternalError(e.to_string()))?,
            ))
            .with_signature(CompressedSignature::new_from_schnorr(kernel_signature))
            .build()
            .unwrap();

        new_template.body.add_kernel(kernel_new);
        new_template.body.sort();

        // Ask the node to finalize the block (fills in MMR roots etc.)
        let new_block = handler.get_new_block(new_template).await.map_err(|e| {
            warn!(target: LOG_TARGET, "Failed to get new block: {e}");
            e
        })?;

        let block_height = new_block.header.height;

        // Compute the RandomXT mining hash
        let mining_hash = match new_block.header.pow.pow_algo {
            PowAlgorithm::RandomXT => new_block.header.mining_hash().to_vec(),
            algo => {
                return Err(XmrigProxyError::InternalError(format!(
                    "Expected RandomXT block template, got {algo:?}"
                )));
            },
        };

        if mining_hash.len() != 32 {
            return Err(XmrigProxyError::MissingData(format!(
                "mining_hash has wrong length: {}",
                mining_hash.len()
            )));
        }

        // Get the RandomX VM key (seed hash for XMRig) from the block at tari_rx_vm_key_height
        let vm_key_height = tari_rx_vm_key_height(block_height);
        let _vm_key = *handler
            .get_header(vm_key_height)
            .await?
            .ok_or_else(|| XmrigProxyError::MissingData(format!("block header at height {vm_key_height} not found")))?
            .hash();

        // Assign nonce range and store template with miner context
        let nonce_range = self.nonce_partitioner.write().await.assign(&miner_id);

        // Record the mapping so eviction can reclaim this nonce range later.
        self.miner_registry
            .register_nonce_partitioner_id(&registration_id, &miner_id)
            .await;

        let mining_hash_key: [u8; 32] = mining_hash
            .as_slice()
            .try_into()
            .map_err(|_| XmrigProxyError::MissingData("mining hash not 32 bytes".to_string()))?;

        self.block_templates
            .store(
                mining_hash_key,
                new_block,
                payment_address.clone(),
                miner_id.clone(),
                nonce_range,
                target_difficulty,
                PowAlgorithm::RandomXT,
            )
            .await;

        debug!(
            target: LOG_TARGET,
            "New template for height #{block_height}, miner {miner_id}, address {}",
            payment_address
        );

        // Build response via shared helper
        self.build_template_response(&mining_hash_key, &miner_id, req).await
    }

    /// Build the JSON-RPC response for a block template (used by both cached and fresh paths).
    async fn build_template_response(
        &self,
        mining_hash_key: &[u8; 32],
        miner_id: &str,
        req: &Value,
    ) -> Result<Response<ProxyBody>, XmrigProxyError> {
        let block = match self.block_templates.get(mining_hash_key).await {
            Some(b) => b,
            None => {
                return Err(XmrigProxyError::InternalError(
                    "Template disappeared after store".to_string(),
                ));
            },
        };
        let block_height = block.header.height;

        // Re-derive mining hash from the stored block (nonce is zero at template time)
        let mining_hash = match block.header.pow.pow_algo {
            PowAlgorithm::RandomXT => block.header.mining_hash().to_vec(),
            algo => {
                return Err(XmrigProxyError::InternalError(format!(
                    "Expected RandomXT block template, got {algo:?}"
                )));
            },
        };

        // Get the RandomX VM key (seed hash for XMRig) from the block at tari_rx_vm_key_height
        let mut handler = self.node_service.clone();
        let vm_key_height = tari_rx_vm_key_height(block_height);
        let vm_key = *handler
            .get_header(vm_key_height)
            .await?
            .ok_or_else(|| XmrigProxyError::MissingData(format!("block header at height {vm_key_height} not found")))?
            .hash();

        let target_difficulty_val = self.block_templates.get_target_difficulty(mining_hash_key).await.unwrap_or(600);

        // Calculate expected reward
        let expected_reward = self
            .consensus_rules
            .calculate_coinbase_and_fees(block_height, block.body.kernels())
            .map_err(|e| XmrigProxyError::InternalError(e.to_string()))?
            .as_u64();

        // Build the 76-byte XMRig-compatible mining blob
        let miner_nonce_range = self
            .nonce_partitioner
            .read()
            .await
            .get_range(miner_id)
            .unwrap_or_else(|| 0..u64::MAX);

        // Collect all current nonce allocations for test verification
        let all_ranges = self.nonce_partitioner.read().await.get_all_ranges();
        let miner_nonce_ranges_json: Vec<serde_json::Value> = all_ranges
            .iter()
            .map(|(mid, range)| {
                json!({
                    "miner_id": mid,
                    "nonce_range": {
                        "start": range.start,
                        "end": range.end,
                    },
                })
            })
            .collect();

        let blob = build_tari_mining_blob(&mining_hash, 0u64, POW_ALGO_RANDOMXT);
        let blob_hex = hex::encode(&blob);
        let seed_hex = hex::encode(vm_key);
        let prev_hash_hex = hex::encode(block.header.prev_hash.to_vec());

        debug!(
            target: LOG_TARGET,
            "Template response for height #{block_height}, miner {miner_id}, range {:?}",
            miner_nonce_range
        );

        json_response(
            StatusCode::OK,
            &json_rpc_success(
                req.get("id").and_then(|v| v.as_i64()),
                json!({
                    "blocktemplate_blob": blob_hex,
                    "blockhashing_blob": blob_hex,
                    "seed_hash": seed_hex,
                    "difficulty": target_difficulty_val,
                    "height": block_height,
                    "prev_hash": prev_hash_hex,
                    "reserved_offset": TARI_BLOB_RESERVED_OFFSET,
                    "expected_reward": expected_reward,
                    "status": "OK",
                    "untrusted": false,
                    "miner_id": miner_id,
                    "nonce_range": {
                        "start": miner_nonce_range.start,
                        "end": miner_nonce_range.end,
                    },
                    "miner_nonce_ranges": miner_nonce_ranges_json,
                }),
            ),
        )
    }

    async fn handle_submit_block(&self, req: &Value) -> Result<Response<ProxyBody>, XmrigProxyError> {
        let params = match req["params"].as_array() {
            Some(p) => p,
            None => {
                return json_response(
                    StatusCode::OK,
                    &json_rpc_error(req["id"].as_i64(), -32602, "params must be an array"),
                );
            },
        };

        let blob_hex = match params.first().and_then(Value::as_str) {
            Some(s) => s,
            None => {
                return json_response(
                    StatusCode::OK,
                    &json_rpc_error(req["id"].as_i64(), -32602, "params[0] must be a hex string"),
                );
            },
        };

        let blob = hex::decode(blob_hex).map_err(|e| XmrigProxyError::InvalidRequest(e.to_string()))?;

        // Parse mining hash and nonce using shared parser (avoids magic numbers in callers)
        let (mining_hash, nonce) = parse_mining_blob(&blob)?;

        // Look up and remove the stored block template (prevents duplicate submissions)
        let mut block = match self.block_templates.take(&mining_hash).await {
            Some(b) => b,
            None => {
                let hash_hex = hex::encode(mining_hash);
                warn!(
                    target: LOG_TARGET,
                    "No block template found for mining hash {hash_hex} - possible duplicate submission"
                );
                return json_response(
                    StatusCode::OK,
                    &json_rpc_error(req["id"].as_i64(), -1, "Block template not found or already submitted"),
                );
            },
        };

        // Update the nonce in the block header
        block.header.nonce = nonce;

        let block_height = block.header.height;
        info!(target: LOG_TARGET, "Submitting block #{block_height} with nonce={nonce} to base node");

        // Submit to the base node via LocalNodeCommsInterface
        let mut handler = self.node_service.clone();
        match handler.submit_block(block).await {
            Ok(block_hash) => {
                let block_hash_hex = hex::encode(block_hash);
                info!(target: LOG_TARGET, "Block #{block_height} accepted, hash={block_hash_hex}");
                json_response(
                    StatusCode::OK,
                    &json_rpc_success(
                        req["id"].as_i64(),
                        json!({
                            "status": "OK",
                            "untrusted": false,
                        }),
                    ),
                )
            },
            Err(e) => {
                warn!(target: LOG_TARGET, "Block #{block_height} rejected: {e}");
                json_response(
                    StatusCode::OK,
                    &json_rpc_error(req["id"].as_i64(), -5, &format!("Block rejected: {e}")),
                )
            },
        }
    }
}

/// Build a 76-byte XMRig-compatible mining blob for Tari RandomXT.
///
/// Format:
/// ```text
/// | 3 bytes (zeros) | 32 bytes (mining_hash) | 8 bytes (nonce big-endian) | 33 bytes (pow_algo + zeros) |
/// ```
///
/// - Bytes 35..39: high nonce bytes (XMRig writes per-thread extra nonce here, at `reserved_offset`)
/// - Bytes 39..43: low nonce bytes (XMRig iterates this 4-byte field at the standard Monero nonce offset)
pub fn build_tari_mining_blob(mining_hash: &[u8], nonce: u64, pow_algo: u8) -> Vec<u8> {
    let mut blob = vec![0u8; 3];
    blob.extend_from_slice(mining_hash);
    blob.extend_from_slice(&nonce.to_be_bytes());
    blob.push(pow_algo);
    blob.extend_from_slice(&[0u8; 32]);
    blob
}

/// Parse the mining hash and nonce from a Tari mining blob.
///
/// Layout: `[0x00 x3][mining_hash:32][nonce:8][pow_algo:1][reserved:32]`
fn parse_mining_blob(blob: &[u8]) -> Result<([u8; 32], u64), XmrigProxyError> {
    if blob.len() != TARI_MINING_BLOB_SIZE {
        return Err(XmrigProxyError::InvalidRequest(format!(
            "blob length {} does not match expected {TARI_MINING_BLOB_SIZE}",
            blob.len()
        )));
    }

    let mining_hash: [u8; 32] = blob[TARI_HASH_OFFSET..TARI_BLOB_RESERVED_OFFSET as usize]
        .try_into()
        .map_err(|_| XmrigProxyError::InvalidRequest("bad mining hash slice".to_string()))?;

    let nonce_bytes: [u8; TARI_NONCE_SIZE] = blob[TARI_BLOB_RESERVED_OFFSET as usize..TARI_BLOB_RESERVED_OFFSET as usize + TARI_NONCE_SIZE]
        .try_into()
        .map_err(|_| XmrigProxyError::InvalidRequest("bad nonce slice".to_string()))?;
    let nonce = u64::from_be_bytes(nonce_bytes);

    Ok((mining_hash, nonce))
}

fn json_rpc_success(id: Option<i64>, result: Value) -> Value {
    json!({
        "id": id.unwrap_or(-1),
        "jsonrpc": "2.0",
        "result": result,
    })
}

fn json_rpc_error(id: Option<i64>, code: i32, message: &str) -> Value {
    json!({
        "id": id.unwrap_or(-1),
        "jsonrpc": "2.0",
        "error": {
            "code": code,
            "message": message,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- parse_miner_id_from_request (nonce partitioning ID) ---

    #[test]
    fn parse_miner_id_from_request_extra_nonce_takes_priority_over_everything() {
        let address = TariAddress::default();
        let base58 = address.to_base58();
        let req = json!({
            "params": {
                "wallet_address": base58,
                "extra_nonce": "a1b2c3d4e5f60718"
            }
        });
        let dummy_addr = "127.0.0.1:1234".parse::<SocketAddr>().unwrap();
        // extra_nonce should win over wallet_address and peer as miner identity
        assert_eq!(parse_miner_id_from_request(&req, dummy_addr), "a1b2c3d4e5f60718");
    }

    #[test]
    fn parse_miner_id_from_request_extra_nonce_priority_over_peer() {
        let req = json!({
            "params": {
                "extra_nonce": "deadbeef12345678"
            }
        });
        let peer_addr = "192.168.1.5:42000".parse::<SocketAddr>().unwrap();
        assert_eq!(parse_miner_id_from_request(&req, peer_addr), "deadbeef12345678");
    }

    #[test]
    fn parse_miner_id_from_request_returns_peer_addr_when_no_extra_nonce() {
        let req = json!({});
        let peer_addr = "192.168.1.5:42000".parse::<SocketAddr>().unwrap();
        assert_eq!(parse_miner_id_from_request(&req, peer_addr), "192.168.1.5:42000");
    }

    #[test]
    fn parse_miner_id_from_request_generates_auto_id_when_peer_is_zero() {
        let req = json!({});
        let zero_addr = SocketAddr::from(([0, 0, 0, 0], 0));
        let id = parse_miner_id_from_request(&req, zero_addr);
        assert!(id.starts_with("auto_"));
    }

    #[test]
    fn parse_miner_id_from_request_generates_unique_auto_ids() {
        let req = json!({});
        let zero_addr = SocketAddr::from(([0, 0, 0, 0], 0));
        let id1 = parse_miner_id_from_request(&req, zero_addr);
        let id2 = parse_miner_id_from_request(&req, zero_addr);
        assert_ne!(id1, id2);
    }

    // --- parse_registration_id (MinerRegistry deduplication ID) ---

    #[test]
    fn parse_registration_id_returns_ip_plus_wallet_address() {
        let address = TariAddress::default();
        let base58 = address.to_base58();
        let req = json!({
            "params": {
                "wallet_address": base58
            }
        });
        let dummy_addr = "127.0.0.1:1234".parse::<SocketAddr>().unwrap();
        assert_eq!(parse_registration_id(&req, dummy_addr), format!("127.0.0.1:{}", address));
    }

    #[test]
    fn parse_registration_id_returns_ip_default_when_no_wallet() {
        let req = json!({});
        let peer_addr = "192.168.1.5:42000".parse::<SocketAddr>().unwrap();
        assert_eq!(parse_registration_id(&req, peer_addr), "192.168.1.5:default");
    }

    #[test]
    fn parse_registration_id_generates_auto_id_when_peer_is_zero() {
        let req = json!({});
        let zero_addr = SocketAddr::from(([0, 0, 0, 0], 0));
        let id = parse_registration_id(&req, zero_addr);
        assert!(id.starts_with("auto_"));
    }

    #[test]
    fn parse_registration_id_generates_unique_auto_ids() {
        let req = json!({});
        let zero_addr = SocketAddr::from(([0, 0, 0, 0], 0));
        let id1 = parse_registration_id(&req, zero_addr);
        let id2 = parse_registration_id(&req, zero_addr);
        assert_ne!(id1, id2);
    }

    #[test]
    fn parse_registration_id_same_for_different_ports() {
        // Multi-threaded miner: same IP, different ports → same registration ID
        let address = TariAddress::default();
        let base58 = address.to_base58();
        let req = json!({
            "params": {
                "wallet_address": base58
            }
        });
        let addr1 = "10.0.0.1:42000".parse::<SocketAddr>().unwrap();
        let addr2 = "10.0.0.1:43000".parse::<SocketAddr>().unwrap();
        assert_eq!(parse_registration_id(&req, addr1), parse_registration_id(&req, addr2));
    }

    // --- parse_wallet_address_from_request ---

    #[test]
    fn parse_wallet_address_from_request_returns_none_for_empty_request() {
        let req = json!({});
        assert!(parse_wallet_address_from_request(&req).is_none());
    }

    #[test]
    fn parse_wallet_address_from_request_returns_none_for_invalid_address() {
        let req = json!({
            "params": {
                "wallet_address": "not_a_real_address"
            }
        });
        assert!(parse_wallet_address_from_request(&req).is_none());
    }

    #[test]
    fn parse_wallet_address_from_request_parses_valid_address() {
        let address = TariAddress::default();
        let base58 = address.to_base58();
        let req = json!({
            "params": {
                "wallet_address": base58
            }
        });
        let parsed = parse_wallet_address_from_request(&req).unwrap();
        assert_eq!(parsed, address);
    }

    // --- build_tari_mining_blob ---

    #[test]
    fn build_tari_mining_blob_has_correct_length() {
        let hash = [0x42u8; 32];
        let blob = build_tari_mining_blob(&hash, 0, 2);
        assert_eq!(blob.len(), 76);
    }

    #[test]
    fn build_tari_mining_blob_starts_with_three_zero_bytes() {
        let hash = [0x42u8; 32];
        let blob = build_tari_mining_blob(&hash, 0, 2);
        assert_eq!(&blob[0..3], &[0, 0, 0]);
    }

    #[test]
    fn build_tari_mining_blob_contains_hash_at_offset_3() {
        let hash = [0xABu8; 32];
        let blob = build_tari_mining_blob(&hash, 0, 2);
        assert_eq!(&blob[3..35], &hash[..]);
    }

    #[test]
    fn build_tari_mining_blob_encodes_nonce_as_big_endian() {
        let hash = [0u8; 32];
        let nonce: u64 = 0x01_02_03_04_05_06_07_08;
        let blob = build_tari_mining_blob(&hash, nonce, 2);
        assert_eq!(&blob[35..43], &nonce.to_be_bytes());
    }

    #[test]
    fn build_tari_mining_blob_sets_pow_algo_at_offset_43() {
        let hash = [0u8; 32];
        let blob = build_tari_mining_blob(&hash, 0, POW_ALGO_RANDOMXT);
        assert_eq!(blob[43], POW_ALGO_RANDOMXT);
    }

    #[test]
    fn build_tari_mining_blob_trailing_bytes_are_zero() {
        let hash = [0u8; 32];
        let blob = build_tari_mining_blob(&hash, 0, 2);
        assert_eq!(&blob[44..], &[0u8; 32]);
    }

    #[test]
    fn build_tari_mining_blob_nonce_high_bytes_at_reserved_offset() {
        let hash = [0u8; 32];
        // nonce = 0x00000001_00000000 → high 4 bytes = 0x00000001
        let nonce: u64 = 0x00000001_00000000;
        let blob = build_tari_mining_blob(&hash, nonce, 2);
        // TARI_BLOB_RESERVED_OFFSET is 35, bytes 35..39 are the high nonce bytes
        assert_eq!(&blob[35..39], &[0x00, 0x00, 0x00, 0x01]);
    }

    #[test]
    fn parse_mining_blob_roundtrip() {
        let hash = [0xABu8; 32];
        let nonce: u64 = 0xDEADBEEFCAFEBABE;
        let blob = build_tari_mining_blob(&hash, nonce, POW_ALGO_RANDOMXT);
        let (parsed_hash, parsed_nonce) = parse_mining_blob(&blob).unwrap();
        assert_eq!(parsed_hash, hash);
        assert_eq!(parsed_nonce, nonce);
    }

    #[test]
    fn parse_mining_blob_rejects_wrong_length() {
        let blob = vec![0u8; 75]; // too short
        assert!(parse_mining_blob(&blob).is_err());
    }

    // --- json_rpc_success / json_rpc_error ---

    #[test]
    fn json_rpc_success_contains_result_and_jsonrpc_version() {
        let resp = json_rpc_success(Some(1), json!("ok"));
        assert_eq!(resp["jsonrpc"], "2.0");
        assert_eq!(resp["result"], "ok");
        assert_eq!(resp["id"], 1);
    }

    #[test]
    fn json_rpc_success_defaults_id_to_minus_one() {
        let resp = json_rpc_success(None, json!("ok"));
        assert_eq!(resp["id"], -1);
    }

    #[test]
    fn json_rpc_error_contains_error_code_and_message() {
        let resp = json_rpc_error(Some(2), -1, "bad");
        assert_eq!(resp["jsonrpc"], "2.0");
        assert_eq!(resp["error"]["code"], -1);
        assert_eq!(resp["error"]["message"], "bad");
        assert_eq!(resp["id"], 2);
    }
}
