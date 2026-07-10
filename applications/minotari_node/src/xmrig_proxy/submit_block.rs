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

//! Submit block handling for the XMRig proxy.
//!
//! Validates the submitted blob, extracts mining hash and nonce from it, looks up the
//! corresponding cached template (removing it to prevent duplicate submissions), patches
//! the nonce in the block header, and submits to the base node.

use log::{info, warn};
use hyper::{Response, StatusCode};
use serde_json::{Value, json};
use tari_core::base_node::LocalNodeCommsInterface;

use super::{
    blob::parse_mining_blob,
    error::XmrigProxyError,
    json_rpc::{json_rpc_error, json_rpc_success},
    service::{ProxyBody, json_response},
};

const LOG_TARGET: &str = "minotari::base_node::xmrig_proxy";

/// Handle submitblock — validate blob, look up template, patch nonce, submit to node.
pub async fn handle_submit_block(
    req: &Value,
    block_templates: &super::block_template_storage::BlockTemplateStorage,
    node_service: &LocalNodeCommsInterface,
) -> Result<Response<ProxyBody>, XmrigProxyError> {
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
    let mut block = match block_templates.take(&mining_hash).await {
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
    let mut handler = node_service.clone();
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
