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

use std::net::SocketAddr;

use hyper::{Response, StatusCode, body::Bytes};
use log::{debug, trace, warn};
use serde_json::Value;
use tari_common::configuration::Network;
use tari_common_types::tari_address::TariAddress;
use tari_core::{
    base_node::{LocalNodeCommsInterface, StateMachineHandle},
    consensus::BaseNodeConsensusManager,
};
use tari_transaction_components::tari_proof_of_work::PowAlgorithm;
use tari_transaction_components::transaction_components::RangeProofType;

use super::{
    block_template_storage::BlockTemplateStorage,
    error::XmrigProxyError,
    json_rpc::json_rpc_error,
    miner_registry::MinerRegistry,
    request_parser::{parse_miner_id_from_request, parse_wallet_address_from_request},
    service::{ProxyBody, json_response},
};

const LOG_TARGET: &str = "minotari::base_node::xmrig_proxy";

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
    /// The remote socket address of the miner that opened this connection.
    pub peer_addr: SocketAddr,
}

impl InnerService {
    /// Handle a JSON-RPC request from the miner.
    pub async fn handle(&self, body: Bytes) -> Result<Response<ProxyBody>, XmrigProxyError> {
        let json: Value = serde_json::from_slice(&body)?;
        let method = json.get("method").and_then(Value::as_str).unwrap_or("");
        trace!(target: LOG_TARGET, "Received method: {method}");
        match method {
            "getblocktemplate" => self.handle_get_block_template(&json).await,
            "submitblock" => {
                super::submit_block::handle_submit_block(&json, &self.block_templates, &self.node_service).await
            },
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

    /// Handle GET requests
    pub async fn handle_get(&self, path: &str) -> Result<Response<ProxyBody>, XmrigProxyError> {
        let mut handler = self.node_service.clone();
        super::status_handlers::handle_get(path, &mut handler).await
    }

    #[allow(clippy::too_many_lines)]
    async fn handle_get_block_template(&self, req: &Value) -> Result<Response<ProxyBody>, XmrigProxyError> {
        // 1. Parse miner identity from request (two-layer: extra_nonce > peer_addr for nonce partitioning).
        let miner_id = parse_miner_id_from_request(req, self.peer_addr);
        let requested_wallet_address = parse_wallet_address_from_request(req);
        let payment_address = match &requested_wallet_address {
            Some(addr) if addr.network() == self.network => requested_wallet_address.clone().unwrap(),
            Some(addr) => {
                debug!(
                    target: LOG_TARGET,
                    "Miner at {} provided address network '{}' does not match node network '{}'; falling back to config default (fallback active)",
                    self.peer_addr,
                    addr.network(),
                    self.network
                );
                self.wallet_payment_address.clone()
            },
            None => {
                debug!(
                    target: LOG_TARGET,
                    "Miner at {} provided no wallet address; using config default (fallback active)",
                    self.peer_addr
                );
                self.wallet_payment_address.clone()
            },
        };

        // 2. Register or refresh miner by resolved payment address (dedup by wallet).
        self.miner_registry.get_or_register(&payment_address).await?;

        // 3. Detect chain tip advance — if Tari's state has moved on, evict stale caches.
        let mut handler = self.node_service.clone();
        let (_advanced, current_tip_height) =
            super::chain_tip::check_chain_tip_advance(&mut handler, &self.block_templates).await?;
        let next_height = current_tip_height.saturating_add(1);

        if let Some((cached_key, _cached_entry)) = self.block_templates.get_for_address(&payment_address).await {
            // Add miner to the cached template (no nonce partitioning — random assignment)
            self.block_templates
                .add_miner_to_template(cached_key, miner_id.clone())
                .await;

            debug!(
                target: LOG_TARGET,
                "Cached template hit for miner {}, address {}",
                miner_id,
                payment_address
            );

            // Re-validate after potential eviction — a concurrent request or reorg may have evicted
            // this template between the cache lookup above and now. If template has been evicted, fall through to
            // generate a fresh one instead of returning an error.
            if self.block_templates.get(&cached_key).await.is_none() {
                debug!(target: LOG_TARGET, "Cached template for address {} was evicted between lookup and response build, regenerating", payment_address);
            } else {
                return super::template_builder::build_template_response(
                    &self.node_service,
                    &self.consensus_rules,
                    &self.block_templates,
                    &cached_key,
                    &miner_id,
                    req,
                )
                .await;
            }
        }

        // 4. No cached template — build and store via pipeline
        let constants = self.consensus_rules.consensus_constants(next_height);
        let asking_weight = constants.max_block_transaction_weight();

        let new_template = handler
            .get_new_block_template(PowAlgorithm::RandomXT, asking_weight)
            .await
            .map_err(|e| {
                warn!(target: LOG_TARGET, "Failed to get block template: {e}");
                e
            })?;

        let target_difficulty = new_template.target_difficulty.as_u64();

        let result = super::template_builder::build_and_store(
            &mut handler,
            &self.consensus_rules,
            &self.block_templates,
            &payment_address,
            &miner_id,
            target_difficulty,
            new_template,
            &self.coinbase_extra,
            self.range_proof_type,
        )
        .await?;

        debug!(
            target: LOG_TARGET,
            "New template for miner {}, address {}",
            miner_id,
            payment_address,
        );

        // Build response via shared helper
        super::template_builder::build_template_response(
            &self.node_service,
            &self.consensus_rules,
            &self.block_templates,
            &result.mining_hash_key,
            &miner_id,
            req,
        )
        .await
    }
}
