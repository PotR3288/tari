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
    CompressedCommitment, CompressedPublicKey, CompressedSignature, UncompressedCommitment,
    UncompressedPublicKey,
};
use tari_core::{base_node::LocalNodeCommsInterface, consensus::BaseNodeConsensusManager, validation::tari_rx_vm_key_height};
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
    block_template_storage::{BlockTemplateStorage, ChainTip},
    blob::{POW_ALGO_RANDOMXT, TARI_BLOB_RESERVED_OFFSET, build_tari_mining_blob},
    error::XmrigProxyError,
    json_rpc::json_rpc_success,
    service::{ProxyBody, json_response},
    MinerId,
};

const LOG_TARGET: &str = "minotari::base_node::xmrig_proxy";

/// Result of building and storing a new template — returned to the caller for response construction.
#[allow(dead_code)]
pub struct TemplateBuildResult {
    /// The 32-byte mining hash key used for cache lookup.
    pub mining_hash_key: [u8; 32],
    /// The RandomX VM key (seed hash) for XMRig.
    pub vm_key: [u8; 32],
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
    let (coinbase_kernel, mut key_manager, wallet_commitment_mask_key_id) =
        build_coinbase(consensus_rules, payment_address, coinbase_extra, range_proof_type, height, &mut new_template)?;

    // Step 2: Sign the kernel and add to template body.
    sign_kernel(&mut new_template, &coinbase_kernel, &mut key_manager, wallet_commitment_mask_key_id)?;

    // Step 3: Finalize via node, compute mining hash, get VM key, store in cache.
    finalize_and_store(handler, block_templates, payment_address, miner_id, target_difficulty, new_template).await
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
    let coinbase_extra = CoinBaseExtra::try_from(coinbase_extra.to_vec())
        .map_err(|e| XmrigProxyError::InternalError(e.to_string()))?;
    let key_manager = KeyManager::new_random().map_err(|e| XmrigProxyError::InternalError(e.to_string()))?;
    let script_key_id = TariKeyId::default();

    // Calculate the coinbase reward for this block.
    let reward = consensus_rules.calculate_coinbase_and_fees(height, new_template.body.kernels())
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

    Ok((coinbase_kernel, key_manager, wallet_output.commitment_mask_key_id().clone()))
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
            PowAlgorithm::RandomXT,
        )
        .await;

    Ok(TemplateBuildResult {
        mining_hash_key,
        vm_key,
    })
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
    let block = match block_templates.get(mining_hash_key).await {
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
    let mut handler = node_service.clone();
    let vm_key_height = tari_rx_vm_key_height(block_height);
    let vm_key = *handler
        .get_header(vm_key_height)
        .await?
        .ok_or_else(|| XmrigProxyError::MissingData(format!("block header at height {vm_key_height} not found")))?
        .hash();

    let target_difficulty_val = block_templates
        .get_target_difficulty(mining_hash_key)
        .await
        .unwrap_or(600);

    // Generate a random min_nonce for this template (full u64 space, random start)
    let min_nonce: u64 = rand::random();
    let max_nonce: u64 = u64::MAX;

    // Calculate expected reward
    let expected_reward = consensus_rules
        .calculate_coinbase_and_fees(block_height, block.body.kernels())
        .map_err(|e| XmrigProxyError::InternalError(e.to_string()))?
        .as_u64();

    // Build the 76-byte XMRig-compatible mining blob
    let blob = build_tari_mining_blob(&mining_hash, 0u64, POW_ALGO_RANDOMXT);
    let blob_hex = hex::encode(&blob);
    let seed_hex = hex::encode(vm_key);
    let prev_hash_hex = hex::encode(block.header.prev_hash.to_vec());

    // Fetch current chain tip so the log reveals whether this template is stale.
    let current_tip = get_chain_tip(node_service).await?;

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

/// Fetch the current chain tip height and block hash from the node.
async fn get_chain_tip(node_service: &LocalNodeCommsInterface) -> Result<ChainTip, XmrigProxyError> {
    let mut handler = node_service.clone();
    let meta = handler.get_metadata().await?;
    Ok(ChainTip {
        height: meta.best_block_height(),
        top_hash: *meta.best_block_hash(),
    })
}
