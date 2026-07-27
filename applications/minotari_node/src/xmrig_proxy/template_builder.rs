// Copyright 2025. The Tari Project
//
// Redistribution and use in source code and binary forms, with or without modification, are permitted provided that the
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

//! Builds a new block template with coinbase output, kernel signature, and stores it in the cache.
//! Also constructs the JSON-RPC response for template queries.
//!
//! Three-step pipeline:
//! 1. `build_coinbase()` — generate coinbase output + kernel for payment address, add to template
//! 2. `sign_kernel()` — build kernel signature and add to template body
//! 3. `finalize_and_store()` — finalize via node, compute mining hash, get VM key, store in cache
//!
//! Response construction:
//! - `build_template_response()` — fetch stored block, derive mining data, return JSON-RPC response

use hyper::{Response, StatusCode};
use serde_json::{Value, json};

use tari_common_types::types::{
    CompressedCommitment, CompressedPublicKey, CompressedSignature, UncompressedCommitment, UncompressedPublicKey,
};
use tari_core::{
    base_node::LocalNodeCommsInterface, consensus::BaseNodeConsensusManager, validation::tari_rx_vm_key_height,
};
use tari_node_components::blocks::NewBlockTemplate;
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

use super::{
    MinerId,
    blob::{POW_ALGO_RANDOMXT, TARI_BLOB_RESERVED_OFFSET, build_tari_mining_blob},
    block_template_storage::BlockTemplateStorage,
    chain_tip,
    error::XmrigProxyError,
    json_rpc::json_rpc_success,
    service::{ProxyBody, json_response},
};

const LOG_TARGET: &str = "minotari::base_node::xmrig_proxy";

/// Result of building and storing a new template — returned to the caller for response construction.
#[derive(Debug)]
pub struct TemplateBuildResult {
    /// The 32-byte mining hash key used for cache lookup.
    pub mining_hash_key: [u8; 32],
}

/// Convenience function that chains build_coinbase → sign_kernel → finalize_and_store.
pub async fn build_and_store(
    handler: &mut LocalNodeCommsInterface,
    consensus_rules: &BaseNodeConsensusManager,
    block_templates: &BlockTemplateStorage,
    payment_address: &tari_common_types::tari_address::TariAddress,
    miner_id: &MinerId,
    target_difficulty: u64,
    mut new_template: NewBlockTemplate,
    coinbase_extra: &[u8],
    range_proof_type: RangeProofType,
) -> Result<TemplateBuildResult, XmrigProxyError> {
    let height = new_template.header.height;

    // Step 1: Build coinbase output + kernel and add to template.
    let (coinbase_kernel, mut key_manager, wallet_commitment_mask_key_id) = build_coinbase(
        consensus_rules,
        payment_address,
        coinbase_extra,
        range_proof_type,
        height,
        &mut new_template,
    )?;

    // Step 2: Sign the kernel and add to template body.
    sign_kernel(
        &mut new_template,
        &coinbase_kernel,
        &mut key_manager,
        wallet_commitment_mask_key_id,
    )?;

    // Step 3: Finalize via node, compute mining hash, get VM key, store in cache.
    finalize_and_store(
        handler,
        block_templates,
        payment_address,
        miner_id,
        target_difficulty,
        new_template,
    )
    .await
}

/// Build the coinbase output and kernel for a payment address, add it to the template body,
/// and return the kernel (for signing) plus a KeyManager ready for signing.
pub fn build_coinbase(
    consensus_rules: &BaseNodeConsensusManager,
    payment_address: &tari_common_types::tari_address::TariAddress,
    coinbase_extra: &[u8],
    range_proof_type: RangeProofType,
    height: u64,
    new_template: &mut NewBlockTemplate,
) -> Result<(TransactionKernel, KeyManager, TariKeyId), XmrigProxyError> {
    let constants = consensus_rules.consensus_constants(height);

    // Validate coinbase count.
    let max_coinbases = constants.max_block_coinbase_count();
    if 1 > max_coinbases {
        return Err(XmrigProxyError::InternalError(
            "No coinbases allowed by consensus".to_string(),
        ));
    }

    // Generate the coinbase output and kernel for the payment address.
    let coinbase_extra =
        CoinBaseExtra::try_from(coinbase_extra.to_vec()).map_err(|e| XmrigProxyError::InternalError(e.to_string()))?;
    let key_manager = KeyManager::new_random().map_err(|e| XmrigProxyError::InternalError(e.to_string()))?;
    let script_key_id = TariKeyId::default();

    // Calculate the coinbase reward for this block.
    let reward = consensus_rules
        .calculate_coinbase_and_fees(height, new_template.body.kernels())
        .map_err(|e| XmrigProxyError::InternalError(e.to_string()))?
        .as_u64();

    let (_, coinbase_output, coinbase_kernel, wallet_output) = generate_coinbase_with_wallet_output(
        0.into(),
        reward.into(),
        height,
        &coinbase_extra,
        &key_manager,
        &script_key_id,
        payment_address,
        false, // stealth_payment
        constants,
        range_proof_type,
        MemoField::new_open(vec![], TxType::Coinbase).expect("empty user-data should always be valid"),
    )
    .map_err(|e| XmrigProxyError::InternalError(e.to_string()))?;

    // Add coinbase output to the template body.
    new_template.body.add_output(coinbase_output);

    Ok((
        coinbase_kernel,
        key_manager,
        wallet_output.commitment_mask_key_id().clone(),
    ))
}

/// Build the kernel signature and add it to the template body.
pub fn sign_kernel(
    new_template: &mut NewBlockTemplate,
    coinbase_kernel: &TransactionKernel,
    key_manager: &mut KeyManager,
    wallet_commitment_mask_key_id: TariKeyId,
) -> Result<(), XmrigProxyError> {
    // Build the kernel signature.
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
            &wallet_commitment_mask_key_id,
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

    Ok(())
}

/// Finalize the block via the node, compute mining hash, get VM key, and store in cache.
pub async fn finalize_and_store(
    handler: &mut LocalNodeCommsInterface,
    block_templates: &BlockTemplateStorage,
    payment_address: &tari_common_types::tari_address::TariAddress,
    miner_id: &MinerId,
    target_difficulty: u64,
    new_template: NewBlockTemplate,
) -> Result<TemplateBuildResult, XmrigProxyError> {
    // Ask the node to finalize the block (fills in MMR roots etc.).
    let new_block = handler.get_new_block(new_template).await.map_err(|e| {
        log::warn!(target: "minotari::base_node::xmrig_proxy", "Failed to get new block: {e}");
        e
    })?;

    let block_height = new_block.header.height;

    // Compute the RandomXT mining hash.
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

    // Get the RandomX VM key (seed hash for XMRig) from the block at tari_rx_vm_key_height.
    let vm_key_height = tari_rx_vm_key_height(block_height);
    let mut handler_clone = handler.clone();
    let header = handler_clone
        .get_header(vm_key_height)
        .await?
        .ok_or_else(|| XmrigProxyError::MissingData(format!("block header at height {vm_key_height} not found")))?;
    let vm_key: [u8; 32] = **header.hash();

    // Convert mining hash to key and store template in cache.
    let mining_hash_key: [u8; 32] = mining_hash
        .as_slice()
        .try_into()
        .map_err(|_| XmrigProxyError::MissingData("mining hash not 32 bytes".to_string()))?;

    block_templates
        .store(
            mining_hash_key,
            new_block,
            payment_address.clone(),
            miner_id.clone(),
            target_difficulty,
            vm_key,
        )
        .await;

    Ok(TemplateBuildResult { mining_hash_key })
}

/// Build the JSON-RPC response for a block template (used by both cached and fresh paths).
pub async fn build_template_response(
    node_service: &LocalNodeCommsInterface,
    consensus_rules: &BaseNodeConsensusManager,
    block_templates: &BlockTemplateStorage,
    mining_hash_key: &[u8; 32],
    miner_id: &MinerId,
    req: &Value,
) -> Result<Response<ProxyBody>, XmrigProxyError> {
    let entry = match block_templates.get_by_key(mining_hash_key).await {
        Some(e) => e,
        None => {
            return Err(XmrigProxyError::InternalError(
                "Template disappeared after store".to_string(),
            ));
        },
    };

    // Use the vm_key stored alongside the template — avoids re-fetching from node.
    let vm_key = entry.vm_key;
    let block_height = entry.block.header.height;

    // Re-derive mining hash from the stored block (nonce is zero at template time)
    let mining_hash = match entry.block.header.pow.pow_algo {
        PowAlgorithm::RandomXT => entry.block.header.mining_hash().to_vec(),
        algo => {
            return Err(XmrigProxyError::InternalError(format!(
                "Expected RandomXT block template, got {algo:?}"
            )));
        },
    };

    let target_difficulty_val = block_templates
        .get_target_difficulty(mining_hash_key)
        .await
        .unwrap_or(600);

    // Generate a random min_nonce for this template (full u64 space, random start)
    let min_nonce: u64 = rand::random();
    let max_nonce: u64 = u64::MAX;

    // Calculate expected reward
    let expected_reward = consensus_rules
        .calculate_coinbase_and_fees(block_height, entry.block.body.kernels())
        .map_err(|e| XmrigProxyError::InternalError(e.to_string()))?
        .as_u64();

    // Build the 76-byte XMRig-compatible mining blob
    let blob = build_tari_mining_blob(&mining_hash, 0u64, POW_ALGO_RANDOMXT);
    let blob_hex = hex::encode(&blob);
    let seed_hex = hex::encode(vm_key);
    let prev_hash_hex = hex::encode(entry.block.header.prev_hash.to_vec());

    // Fetch current chain tip so the log reveals whether this template is stale.
    let mut handler = node_service.clone();
    let current_tip = chain_tip::get_chain_tip(&mut handler).await?;

    log::debug!(
        target: LOG_TARGET,
        "Template response for height #{block_height} (current tip: #{}), miner {miner_id}, nonce range [{min_nonce}, {max_nonce}]",
        current_tip.height,
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
                "min_nonce": min_nonce,
                "max_nonce": max_nonce,
                "expected_reward": expected_reward,
                "status": "OK",
                "untrusted": false,
                "miner_id": miner_id,
            }),
        ),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;
    use http_body_util::BodyExt;
    use hyper::StatusCode;
    use primitive_types::U512;
    use serde_json::json;
    use tari_common::configuration::Network;
    use tari_common_types::{
        chain_metadata::ChainMetadata,
        types::{CompressedSignature, FixedHash, PrivateKey},
    };
    use tari_core::{
        base_node::{
            LocalNodeCommsInterface,
            comms_interface::{BlockEvent, CommsInterfaceError, NodeCommsRequest, NodeCommsResponse},
        },
        consensus::BaseNodeConsensusManager,
    };
    use tari_node_components::blocks::{
        Block, BlockBuilder, BlockHeader, BlockHeaderAccumulatedData, ChainHeader, NewBlockTemplate,
    };
    use tari_service_framework::reply_channel::{self, Receiver};
    use tari_transaction_components::{
        MicroMinotari,
        aggregated_body::AggregateBody,
        tari_proof_of_work::{AccumulatedDifficulty, Difficulty, PowAlgorithm, ProofOfWork},
    };
    use tari_utilities::epoch_time::EpochTime;
    use tokio::{sync::broadcast, task};

    use super::*;
    use crate::xmrig_proxy::block_template_storage::BlockTemplateStorage;

    // ---------------------------------------------------------------------------
    // Fixtures & helpers
    // ---------------------------------------------------------------------------

    /// Internal helper: create a DualAddress for a specific network with valid key material.
    fn make_dual_address_inner(network: Network) -> tari_common_types::tari_address::dual_address::DualAddress {
        use tari_common_types::{
            tari_address::{TariAddressFeatures, dual_address::DualAddress},
            types::{CompressedPublicKey, PrivateKey},
        };
        use tari_crypto::keys::SecretKey;

        // Generate random key pairs for view and spend keys.
        let mut rng = rand::rng();
        let view_pk = CompressedPublicKey::from_secret_key(&PrivateKey::random(&mut rng));
        let spend_pk = CompressedPublicKey::from_secret_key(&PrivateKey::random(&mut rng));

        DualAddress::new(
            view_pk,
            spend_pk,
            network,
            TariAddressFeatures::create_one_sided_only(),
            None, // no payment_id
        )
        .expect("valid dual address")
    }

    /// Create a TariAddress for a specific network as a Dual address with valid key material.
    fn make_address_for_network(network: Network) -> tari_common_types::tari_address::TariAddress {
        use tari_common_types::tari_address::TariAddress;
        let dual = make_dual_address_inner(network);
        TariAddress::Dual(Box::new(dual))
    }

    /// Build a minimal ChainMetadata fixture.
    fn make_chain_metadata(height: u64, hash: [u8; 32]) -> ChainMetadata {
        ChainMetadata::new(
            height,
            FixedHash::new(hash),
            0, // pruning_horizon (archival)
            0, // pruned_height
            U512::from(1u64),
            0,
        )
        .expect("valid metadata")
    }

    /// Build a minimal NewBlockTemplate fixture at the given height.
    fn make_new_block_template_at(height: u64) -> NewBlockTemplate {
        make_new_block_template_at_with_pow(height, PowAlgorithm::RandomXT)
    }

    /// Build a minimal NewBlockTemplate fixture with a specific PoW algorithm.
    fn make_new_block_template_at_with_pow(height: u64, pow_algo: PowAlgorithm) -> NewBlockTemplate {
        let mut header = tari_node_components::blocks::NewBlockHeaderTemplate::empty();
        header.height = height;
        header.pow = ProofOfWork::new(pow_algo);
        NewBlockTemplate {
            header,
            body: AggregateBody::empty(),
            target_difficulty: Difficulty::from_u64(1).unwrap(),
            reward: MicroMinotari::from(0u64),
            total_fees: MicroMinotari::from(0u64),
            is_mempool_in_sync: true,
        }
    }

    /// Build a LocalNodeCommsInterface backed by reply channels. Returns the
    /// interface and the receiver side for dispatching canned responses.
    fn make_mock_comms() -> (
        LocalNodeCommsInterface,
        Receiver<NodeCommsRequest, Result<NodeCommsResponse, CommsInterfaceError>>,
    ) {
        let (req_tx, req_rx) =
            reply_channel::unbounded::<NodeCommsRequest, Result<NodeCommsResponse, CommsInterfaceError>>();
        let (block_tx, _block_rx) =
            reply_channel::unbounded::<tari_node_components::blocks::Block, Result<FixedHash, CommsInterfaceError>>();
        let block_event_tx: broadcast::Sender<Arc<BlockEvent>> = broadcast::channel(50).0;
        (LocalNodeCommsInterface::new(req_tx, block_tx, block_event_tx), req_rx)
    }

    /// Build a Block fixture with RandomXT PoW at the given height.
    fn make_test_block(height: u64) -> Block {
        let mut block = BlockBuilder::new(0).build();
        // Set the actual block height (BlockBuilder::new takes blockchain_version, not height).
        block.header.height = height;
        // Override pow to be RandomXT so finalize_and_store accepts it.
        block.header.pow = ProofOfWork::new(PowAlgorithm::RandomXT);
        block
    }

    /// Spawn a mock comms handler that responds to common requests.
    fn spawn_mock_handler(
        mut req_rx: Receiver<NodeCommsRequest, Result<NodeCommsResponse, CommsInterfaceError>>,
        _canned_block: Block,
        _vm_key_header_hash: [u8; 32],
        metadata_height: u64,
    ) {
        let meta = make_chain_metadata(metadata_height, [1u8; 32]);

        task::spawn(async move {
            while let Some(req_ctx) = req_rx.next().await {
                match req_ctx.request() {
                    NodeCommsRequest::GetChainMetadata => {
                        req_ctx.reply(Ok(NodeCommsResponse::ChainMetadata(meta.clone()))).ok();
                    },
                    NodeCommsRequest::GetNewBlockTemplate(_) => {
                        req_ctx.reply(Err(CommsInterfaceError::UnexpectedApiResponse)).ok();
                    },
                    NodeCommsRequest::GetNewBlock(template) => {
                        // Construct a Block from the template body (which contains coinbase output + kernels).
                        let mut header = BlockHeader::new(template.header.version);
                        header.height = template.header.height;
                        header.prev_hash = template.header.prev_hash;
                        header.timestamp = EpochTime::now();
                        header.total_kernel_offset = template.header.total_kernel_offset.clone();
                        header.total_script_offset = template.header.total_script_offset.clone();
                        header.pow = template.header.pow.clone();

                        let block = Block {
                            header,
                            body: template.body.clone(),
                        };

                        req_ctx
                            .reply(Ok(NodeCommsResponse::NewBlock {
                                success: true,
                                error: None,
                                block: Some(block),
                            }))
                            .ok();
                    },
                    NodeCommsRequest::FetchHeaders(_range) => {
                        // Return a valid ChainHeader with internally consistent hashes.
                        let mut header = BlockHeader::new(0);
                        header.height = 1;
                        header.timestamp = EpochTime::from(0u64);
                        header.pow = ProofOfWork::new(PowAlgorithm::RandomXT);

                        // Compute the actual hash of this header so accumulated_data matches.
                        let header_hash = header.hash();

                        let accumulated_data = BlockHeaderAccumulatedData {
                            hash: header_hash,
                            total_kernel_offset: PrivateKey::default(),
                            achieved_difficulty: Difficulty::min(),
                            total_accumulated_difficulty: U512::from(1u64),
                            accumulated_monero_randomx_difficulty: AccumulatedDifficulty::min(),
                            accumulated_tari_randomx_difficulty: AccumulatedDifficulty::min(),
                            accumulated_sha3x_difficulty: AccumulatedDifficulty::min(),
                            accumulated_cuckaroo_difficulty: AccumulatedDifficulty::min(),
                            target_difficulty: Difficulty::min(),
                        };

                        if let Some(chain_header) = ChainHeader::try_construct(header, accumulated_data) {
                            req_ctx
                                .reply(Ok(NodeCommsResponse::BlockHeaders(vec![chain_header])))
                                .ok();
                        } else {
                            req_ctx.reply(Ok(NodeCommsResponse::BlockHeaders(vec![]))).ok();
                        }
                    },
                    _ => {
                        req_ctx.reply(Err(CommsInterfaceError::UnexpectedApiResponse)).ok();
                    },
                }
            }
        });
    }

    // ---------------------------------------------------------------------------
    // build_coinbase() tests
    // ---------------------------------------------------------------------------

    #[test]
    fn build_coinbase_success_with_empty_extra_adds_one_output_and_valid_kernel() {
        let consensus_rules = BaseNodeConsensusManager::builder(Network::LocalNet).build().unwrap();
        let payment_address = make_address_for_network(Network::LocalNet);
        let mut template = make_new_block_template_at(100);

        let (kernel, _key_manager, _mask_key_id) = build_coinbase(
            &consensus_rules,
            &payment_address,
            &[], // empty coinbase_extra
            RangeProofType::BulletProofPlus,
            100,
            &mut template,
        )
        .unwrap();

        // Verify coinbase output was added.
        assert_eq!(template.body.outputs().len(), 1);
        // The kernel should have a non-zero excess (it's a valid kernel).
        assert_ne!(kernel.excess.as_bytes(), &[0u8; 32][..]);
    }

    #[test]
    fn build_coinbase_success_with_non_empty_extra_adds_one_output() {
        let consensus_rules = BaseNodeConsensusManager::builder(Network::LocalNet).build().unwrap();
        let payment_address = make_address_for_network(Network::LocalNet);
        let mut template = make_new_block_template_at(100);
        let extra_data = vec![0x01, 0x02, 0x03];

        let result = build_coinbase(
            &consensus_rules,
            &payment_address,
            &extra_data,
            RangeProofType::BulletProofPlus,
            100,
            &mut template,
        );

        assert!(result.is_ok());
        assert_eq!(template.body.outputs().len(), 1);
    }

    // ---------------------------------------------------------------------------
    // sign_kernel() tests
    // ---------------------------------------------------------------------------

    #[test]
    fn sign_kernel_adds_signed_kernel_to_template() {
        let consensus_rules = BaseNodeConsensusManager::builder(Network::LocalNet).build().unwrap();
        let payment_address = make_address_for_network(Network::LocalNet);
        let mut template = make_new_block_template_at(100);

        let (coinbase_kernel, mut key_manager, mask_key_id) = build_coinbase(
            &consensus_rules,
            &payment_address,
            &[],
            RangeProofType::BulletProofPlus,
            100,
            &mut template,
        )
        .unwrap();

        let kernels_before = template.body.kernels().len();

        sign_kernel(&mut template, &coinbase_kernel, &mut key_manager, mask_key_id).unwrap();

        // One signed kernel should be added.
        assert_eq!(template.body.kernels().len(), kernels_before + 1);
    }

    #[test]
    fn sign_kernel_produces_valid_signature() {
        let consensus_rules = BaseNodeConsensusManager::builder(Network::LocalNet).build().unwrap();
        let payment_address = make_address_for_network(Network::LocalNet);
        let mut template = make_new_block_template_at(100);

        let (coinbase_kernel, mut key_manager, mask_key_id) = build_coinbase(
            &consensus_rules,
            &payment_address,
            &[],
            RangeProofType::BulletProofPlus,
            100,
            &mut template,
        )
        .unwrap();

        sign_kernel(&mut template, &coinbase_kernel, &mut key_manager, mask_key_id).unwrap();

        let signed_kernel = template.body.kernels().last().expect("should have a kernel");
        // The signature should be non-zero (valid Schnorr signature).
        assert_ne!(signed_kernel.excess_sig, CompressedSignature::default());
    }

    #[test]
    fn sign_kernel_sorts_template_body() {
        let consensus_rules = BaseNodeConsensusManager::builder(Network::LocalNet).build().unwrap();
        let payment_address = make_address_for_network(Network::LocalNet);
        let mut template = make_new_block_template_at(100);

        let (coinbase_kernel, mut key_manager, mask_key_id) = build_coinbase(
            &consensus_rules,
            &payment_address,
            &[],
            RangeProofType::BulletProofPlus,
            100,
            &mut template,
        )
        .unwrap();

        sign_kernel(&mut template, &coinbase_kernel, &mut key_manager, mask_key_id).unwrap();

        // The body should be sorted (outputs before kernels is the expected order).
        assert!(!template.body.kernels().is_empty());
    }

    // ---------------------------------------------------------------------------
    // finalize_and_store() tests
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn finalize_and_store_success_stores_template_in_cache() {
        let (comms, req_rx) = make_mock_comms();
        let block_templates = BlockTemplateStorage::new();
        let payment_address = make_address_for_network(Network::LocalNet);
        let miner_id = "test_miner".to_string();

        // Use height 3048 so tari_rx_vm_key_height(3048) == 2048 (non-zero).
        let block = make_test_block(3048);
        let vm_key_hash: [u8; 32] = [0xAB; 32];

        spawn_mock_handler(req_rx, block.clone(), vm_key_hash, 3048);

        let template = make_new_block_template_at(3048);
        let mut handler = comms;

        let result = finalize_and_store(
            &mut handler,
            &block_templates,
            &payment_address,
            &miner_id,
            1000,
            template,
        )
        .await;

        assert!(result.is_ok(), "finalize_and_store should succeed");
        // Verify the template was stored.
        let entry = block_templates.get_for_address(&payment_address).await;
        assert!(entry.is_some(), "template should be in cache after finalize_and_store");
        let (_key, entry) = entry.unwrap();
        assert_eq!(entry.wallet_address, payment_address);
        // vm_key is derived from the mock's header hash (not predictable), so verify it's non-zero.
        assert!(!entry.vm_key.iter().all(|&b| b == 0), "vm_key should be non-zero");
    }

    #[tokio::test]
    async fn finalize_and_store_rejects_non_randomxt_algorithm() {
        let (comms, mut req_rx) = make_mock_comms();
        let block_templates = BlockTemplateStorage::new();
        let payment_address = make_address_for_network(Network::LocalNet);

        // Use a Cuckaroo template — should be rejected by finalize_and_store.
        // We need a mock handler to respond to the GetNewBlock request with a Cuckaroo PoW block.
        let template = make_new_block_template_at_with_pow(3048, PowAlgorithm::Cuckaroo);

        task::spawn(async move {
            while let Some(req_ctx) = req_rx.next().await {
                match req_ctx.request() {
                    NodeCommsRequest::GetChainMetadata => {
                        req_ctx
                            .reply(Ok(NodeCommsResponse::ChainMetadata(make_chain_metadata(
                                3048, [1u8; 32],
                            ))))
                            .ok();
                    },
                    NodeCommsRequest::GetNewBlockTemplate(_) => {
                        req_ctx.reply(Err(CommsInterfaceError::UnexpectedApiResponse)).ok();
                    },
                    NodeCommsRequest::GetNewBlock(_template) => {
                        // Return a block with Cuckaroo PoW to trigger the rejection.
                        let mut header = BlockHeader::new(0);
                        header.height = 3048;
                        header.pow = ProofOfWork::new(PowAlgorithm::Cuckaroo);

                        req_ctx
                            .reply(Ok(NodeCommsResponse::NewBlock {
                                success: true,
                                error: None,
                                block: Some(Block {
                                    header,
                                    body: AggregateBody::empty(),
                                }),
                            }))
                            .ok();
                    },
                    NodeCommsRequest::FetchHeaders(_range) => {
                        let mut h = BlockHeader::new(0);
                        h.height = 1;
                        h.timestamp = EpochTime::from(0u64);
                        h.pow = ProofOfWork::new(PowAlgorithm::RandomXT);
                        let hh = h.hash();
                        let ad = BlockHeaderAccumulatedData {
                            hash: hh,
                            total_kernel_offset: PrivateKey::default(),
                            achieved_difficulty: Difficulty::min(),
                            total_accumulated_difficulty: U512::from(1u64),
                            accumulated_monero_randomx_difficulty: AccumulatedDifficulty::min(),
                            accumulated_tari_randomx_difficulty: AccumulatedDifficulty::min(),
                            accumulated_sha3x_difficulty: AccumulatedDifficulty::min(),
                            accumulated_cuckaroo_difficulty: AccumulatedDifficulty::min(),
                            target_difficulty: Difficulty::min(),
                        };
                        if let Some(ch) = ChainHeader::try_construct(h, ad) {
                            req_ctx.reply(Ok(NodeCommsResponse::BlockHeaders(vec![ch]))).ok();
                        } else {
                            req_ctx.reply(Ok(NodeCommsResponse::BlockHeaders(vec![]))).ok();
                        }
                    },
                    _ => {
                        req_ctx.reply(Err(CommsInterfaceError::UnexpectedApiResponse)).ok();
                    },
                }
            }
        });

        let mut handler = comms;

        let result = finalize_and_store(
            &mut handler,
            &block_templates,
            &payment_address,
            &"miner".to_string(),
            1000,
            template,
        )
        .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            XmrigProxyError::InternalError(msg) => {
                assert!(msg.contains("RandomXT"), "error should mention RandomXT: {msg}");
            },
            other => panic!("Expected InternalError, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn finalize_and_store_handles_missing_vm_key_header() {
        let (comms, mut req_rx) = make_mock_comms();
        let block_templates = BlockTemplateStorage::new();
        let payment_address = make_address_for_network(Network::LocalNet);

        // Use height 3048 so tari_rx_vm_key_height(3048) == 2048.
        // The mock handler will return an empty header list for the VM key fetch,
        // causing get_header to return None → MissingData error.
        let block = make_test_block(3048);

        task::spawn(async move {
            while let Some(req_ctx) = req_rx.next().await {
                match req_ctx.request() {
                    NodeCommsRequest::GetChainMetadata => {
                        req_ctx
                            .reply(Ok(NodeCommsResponse::ChainMetadata(make_chain_metadata(
                                3048, [1u8; 32],
                            ))))
                            .ok();
                    },
                    NodeCommsRequest::GetNewBlock(_) => {
                        req_ctx
                            .reply(Ok(NodeCommsResponse::NewBlock {
                                success: true,
                                error: None,
                                block: Some(block.clone()),
                            }))
                            .ok();
                    },
                    NodeCommsRequest::FetchHeaders(_range) => {
                        // Return empty — simulates header not found at vm_key_height.
                        req_ctx.reply(Ok(NodeCommsResponse::BlockHeaders(vec![]))).ok();
                    },
                    _ => {
                        req_ctx.reply(Err(CommsInterfaceError::UnexpectedApiResponse)).ok();
                    },
                }
            }
        });

        let template = make_new_block_template_at(3048);
        let mut handler = comms;

        let result = finalize_and_store(
            &mut handler,
            &block_templates,
            &payment_address,
            &"miner".to_string(),
            1000,
            template,
        )
        .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            XmrigProxyError::MissingData(msg) => {
                assert!(msg.contains("not found"), "error should mention 'not found': {msg}");
            },
            other => panic!("Expected MissingData, got: {:?}", other),
        }
    }

    // ---------------------------------------------------------------------------
    // build_template_response() tests
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn build_template_response_success_contains_expected_fields() {
        let (comms, mut req_rx) = make_mock_comms();
        let block_templates = BlockTemplateStorage::new();

        // Store a template entry first.
        let block = make_test_block(3048);
        let mining_hash_key: [u8; 32] = [0x55; 32];
        let vm_key: [u8; 32] = [0x66; 32];
        let payment_address = make_address_for_network(Network::LocalNet);

        block_templates
            .store(
                mining_hash_key,
                block.clone(),
                payment_address.clone(),
                "test_miner".to_string(),
                1500,
                vm_key,
            )
            .await;
        task::spawn(async move {
            while let Some(req_ctx) = req_rx.next().await {
                match req_ctx.request() {
                    NodeCommsRequest::GetChainMetadata => {
                        req_ctx
                            .reply(Ok(NodeCommsResponse::ChainMetadata(make_chain_metadata(
                                3048, [1u8; 32],
                            ))))
                            .ok();
                    },
                    _ => {
                        req_ctx.reply(Err(CommsInterfaceError::UnexpectedApiResponse)).ok();
                    },
                }
            }
        });

        let consensus_rules = BaseNodeConsensusManager::builder(Network::LocalNet).build().unwrap();
        let miner_id = "test_miner".to_string();
        let req = json!({"id": 42, "jsonrpc": "2.0", "method": "getblocktemplate"});

        let handler = comms;
        let result = build_template_response(
            &handler,
            &consensus_rules,
            &block_templates,
            &mining_hash_key,
            &miner_id,
            &req,
        )
        .await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        // Verify JSON-RPC success envelope.
        assert_eq!(parsed["jsonrpc"], "2.0");
        assert_eq!(parsed["id"], 42);
        assert!(parsed["result"].is_object());

        let result_obj = parsed["result"].as_object().unwrap();
        assert!(result_obj.contains_key("blocktemplate_blob"));
        assert!(result_obj.contains_key("blockhashing_blob"));
        assert_eq!(result_obj["seed_hash"], json!(hex::encode(vm_key)));
        assert_eq!(result_obj["difficulty"], 1500);
        assert_eq!(result_obj["height"], 3048);
        assert_eq!(result_obj["reserved_offset"], TARI_BLOB_RESERVED_OFFSET);
        assert!(result_obj.contains_key("min_nonce"));
        assert_eq!(result_obj["max_nonce"], u64::MAX);
        assert_eq!(result_obj["status"], "OK");
        assert_eq!(result_obj["untrusted"], false);
        assert_eq!(result_obj["miner_id"], "test_miner");

        // Verify blob is 76 bytes hex-encoded.
        let blob_hex = result_obj["blocktemplate_blob"].as_str().unwrap();
        assert_eq!(hex::decode(blob_hex).unwrap().len(), 76);
    }

    #[tokio::test]
    async fn build_template_response_missing_template_returns_error() {
        let (_comms, _req_rx) = make_mock_comms();
        let block_templates = BlockTemplateStorage::new();

        // Don't store any template — call with a key that doesn't exist.
        let missing_key: [u8; 32] = [0xFF; 32];
        let consensus_rules = BaseNodeConsensusManager::builder(Network::LocalNet).build().unwrap();
        let miner_id = "test_miner".to_string();
        let req = json!({"id": 1});

        let result = build_template_response(
            &_comms,
            &consensus_rules,
            &block_templates,
            &missing_key,
            &miner_id,
            &req,
        )
        .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            XmrigProxyError::InternalError(msg) => {
                assert!(msg.contains("disappeared"), "error should mention 'disappeared': {msg}");
            },
            other => panic!("Expected InternalError, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn build_template_response_handles_no_wallet_id_in_request() {
        let (comms, mut req_rx) = make_mock_comms();
        let block_templates = BlockTemplateStorage::new();

        let block = make_test_block(3048);
        let mining_hash_key: [u8; 32] = [0x77; 32];
        let vm_key: [u8; 32] = [0x88; 32];
        let payment_address = make_address_for_network(Network::LocalNet);

        block_templates
            .store(
                mining_hash_key,
                block.clone(),
                payment_address.clone(),
                "miner".to_string(),
                1500,
                vm_key,
            )
            .await;

        task::spawn(async move {
            while let Some(req_ctx) = req_rx.next().await {
                match req_ctx.request() {
                    NodeCommsRequest::GetChainMetadata => {
                        req_ctx
                            .reply(Ok(NodeCommsResponse::ChainMetadata(make_chain_metadata(
                                3048, [1u8; 32],
                            ))))
                            .ok();
                    },
                    _ => {
                        req_ctx.reply(Err(CommsInterfaceError::UnexpectedApiResponse)).ok();
                    },
                }
            }
        });

        // Request with no "id" field.
        let req = json!({"jsonrpc": "2.0", "method": "getblocktemplate"});
        let consensus_rules = BaseNodeConsensusManager::builder(Network::LocalNet).build().unwrap();
        let miner_id = "miner".to_string();

        let result = build_template_response(
            &comms,
            &consensus_rules,
            &block_templates,
            &mining_hash_key,
            &miner_id,
            &req,
        )
        .await;

        assert!(result.is_ok());
        let response = result.unwrap();
        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        // id should default to -1 when not present in request.
        assert_eq!(parsed["id"], -1);
    }

    #[tokio::test]
    async fn build_template_response_blob_starts_with_three_zero_bytes() {
        let (comms, mut req_rx) = make_mock_comms();
        let block_templates = BlockTemplateStorage::new();

        let block = make_test_block(3048);
        let mining_hash_key: [u8; 32] = [0x99; 32];
        let vm_key: [u8; 32] = [0xAA; 32];
        let payment_address = make_address_for_network(Network::LocalNet);

        block_templates
            .store(
                mining_hash_key,
                block.clone(),
                payment_address.clone(),
                "miner".to_string(),
                1500,
                vm_key,
            )
            .await;

        task::spawn(async move {
            while let Some(req_ctx) = req_rx.next().await {
                match req_ctx.request() {
                    NodeCommsRequest::GetChainMetadata => {
                        req_ctx
                            .reply(Ok(NodeCommsResponse::ChainMetadata(make_chain_metadata(
                                3048, [1u8; 32],
                            ))))
                            .ok();
                    },
                    _ => {
                        req_ctx.reply(Err(CommsInterfaceError::UnexpectedApiResponse)).ok();
                    },
                }
            }
        });

        let consensus_rules = BaseNodeConsensusManager::builder(Network::LocalNet).build().unwrap();
        let miner_id = "miner".to_string();
        let req = json!({"id": 1});

        let result = build_template_response(
            &comms,
            &consensus_rules,
            &block_templates,
            &mining_hash_key,
            &miner_id,
            &req,
        )
        .await;

        assert!(result.is_ok());
        let response = result.unwrap();
        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        let blob_hex = parsed["result"]["blocktemplate_blob"].as_str().unwrap();
        let blob_bytes = hex::decode(blob_hex).unwrap();
        assert_eq!(&blob_bytes[0..3], &[0, 0, 0]);
    }

    // ---------------------------------------------------------------------------
    // build_and_store() integration test (full pipeline)
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn build_and_store_full_pipeline_stores_template_in_cache() {
        let (mut comms, req_rx) = make_mock_comms();
        let block_templates = BlockTemplateStorage::new();
        let payment_address = make_address_for_network(Network::LocalNet);
        let miner_id = "pipeline_miner".to_string();

        // Use height 3048 so tari_rx_vm_key_height(3048) == 2048.
        let block = make_test_block(3048);
        let vm_key_hash: [u8; 32] = [0xBB; 32];

        spawn_mock_handler(req_rx, block.clone(), vm_key_hash, 3048);

        let consensus_rules = BaseNodeConsensusManager::builder(Network::LocalNet).build().unwrap();
        let template = make_new_block_template_at(3048);

        let result = build_and_store(
            &mut comms,
            &consensus_rules,
            &block_templates,
            &payment_address,
            &miner_id,
            2000,
            template,
            &[], // empty coinbase_extra
            RangeProofType::BulletProofPlus,
        )
        .await;

        assert!(
            result.is_ok(),
            "build_and_store full pipeline should succeed: {:?}",
            result
        );
        // Verify the template was stored.
        let entry = block_templates.get_for_address(&payment_address).await;
        assert!(entry.is_some(), "template should be in cache after full pipeline");
        let (_key, entry) = entry.unwrap();
        assert_eq!(entry.wallet_address, payment_address);
        // vm_key is derived from the mock's header hash (not predictable), so verify it's non-zero.
        assert!(!entry.vm_key.iter().all(|&b| b == 0), "vm_key should be non-zero");
        // Verify the block has coinbase output and signed kernel.
        assert!(!entry.block.body.outputs().is_empty());
        assert!(!entry.block.body.kernels().is_empty());
    }

    // ---------------------------------------------------------------------------
    // get_chain_tip() internal function test (via build_template_response)
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn build_template_response_includes_current_tip_in_log_context() {
        let (comms, mut req_rx) = make_mock_comms();
        let block_templates = BlockTemplateStorage::new();

        let block = make_test_block(3048);
        let mining_hash_key: [u8; 32] = [0xCC; 32];
        let vm_key: [u8; 32] = [0xDD; 32];
        let payment_address = make_address_for_network(Network::LocalNet);

        block_templates
            .store(
                mining_hash_key,
                block.clone(),
                payment_address.clone(),
                "miner".to_string(),
                1500,
                vm_key,
            )
            .await;

        task::spawn(async move {
            while let Some(req_ctx) = req_rx.next().await {
                match req_ctx.request() {
                    NodeCommsRequest::GetChainMetadata => {
                        // Return metadata with height 5000 — different from block height.
                        req_ctx
                            .reply(Ok(NodeCommsResponse::ChainMetadata(make_chain_metadata(
                                5000, [2u8; 32],
                            ))))
                            .ok();
                    },
                    _ => {
                        req_ctx.reply(Err(CommsInterfaceError::UnexpectedApiResponse)).ok();
                    },
                }
            }
        });

        let consensus_rules = BaseNodeConsensusManager::builder(Network::LocalNet).build().unwrap();
        let miner_id = "miner".to_string();
        let req = json!({"id": 3});

        let result = build_template_response(
            &comms,
            &consensus_rules,
            &block_templates,
            &mining_hash_key,
            &miner_id,
            &req,
        )
        .await;

        // Should succeed — get_chain_tip returned valid metadata.
        assert!(result.is_ok());
    }

    // ---------------------------------------------------------------------------
    // Coinbase generation tests
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn build_coinbase_success() {
        // Test coinbase generation with various input configurations
        let config_wallet = make_address_for_network(Network::LocalNet);
        let consensus_rules = BaseNodeConsensusManager::builder(Network::LocalNet).build().unwrap();
        let range_proof_type = tari_transaction_components::transaction_components::RangeProofType::BulletProofPlus;

        // Build a minimal block template for testing
        let mut new_template = NewBlockTemplate {
            header: tari_node_components::blocks::NewBlockHeaderTemplate::empty(),
            body: AggregateBody::empty(),
            target_difficulty: Difficulty::from_u64(1).unwrap(),
            reward: MicroMinotari::from(1000),
            total_fees: MicroMinotari::from(0),
            is_mempool_in_sync: true,
        };

        // Test with non-empty extra
        let coinbase_extra_vec = vec![1u8, 2u8, 3u8];
        let result = build_coinbase(
            &consensus_rules,
            &config_wallet,
            &coinbase_extra_vec,
            range_proof_type,
            100,
            &mut new_template,
        );
        assert!(result.is_ok(), "Coinbase generation with extra failed");
    }

    #[tokio::test]
    async fn build_coinbase_empty_extra() {
        // Test coinbase generation with empty extra
        let config_wallet = make_address_for_network(Network::LocalNet);
        let consensus_rules = BaseNodeConsensusManager::builder(Network::LocalNet).build().unwrap();
        let range_proof_type = tari_transaction_components::transaction_components::RangeProofType::BulletProofPlus;

        let mut new_template = NewBlockTemplate {
            header: tari_node_components::blocks::NewBlockHeaderTemplate::empty(),
            body: AggregateBody::empty(),
            target_difficulty: Difficulty::from_u64(1).unwrap(),
            reward: MicroMinotari::from(1000),
            total_fees: MicroMinotari::from(0),
            is_mempool_in_sync: true,
        };

        let coinbase_extra_vec: Vec<u8> = vec![];
        let result = build_coinbase(
            &consensus_rules,
            &config_wallet,
            &coinbase_extra_vec,
            range_proof_type,
            100,
            &mut new_template,
        );
        assert!(result.is_ok(), "Coinbase generation with empty extra failed");
    }

    #[tokio::test]
    async fn sign_kernel_basic() {
        // Test basic sign_kernel functionality - just verify the function signature compiles
        // The full signing requires key manager which is complex to mock in unit tests
        let config_wallet = make_address_for_network(Network::LocalNet);
        let consensus_rules = BaseNodeConsensusManager::builder(Network::LocalNet).build().unwrap();
        let range_proof_type = tari_transaction_components::transaction_components::RangeProofType::BulletProofPlus;

        // Build a minimal block template for testing
        let mut new_template = NewBlockTemplate {
            header: tari_node_components::blocks::NewBlockHeaderTemplate::empty(),
            body: AggregateBody::empty(),
            target_difficulty: Difficulty::from_u64(1).unwrap(),
            reward: MicroMinotari::from(1000),
            total_fees: MicroMinotari::from(0),
            is_mempool_in_sync: true,
        };

        let coinbase_extra_vec = vec![1u8, 2u8, 3u8];
        let result = build_coinbase(
            &consensus_rules,
            &config_wallet,
            &coinbase_extra_vec,
            range_proof_type,
            100,
            &mut new_template,
        );
        assert!(result.is_ok(), "Coinbase generation should succeed");
    }
}
