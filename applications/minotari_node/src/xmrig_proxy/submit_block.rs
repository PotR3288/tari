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

use hyper::{Response, StatusCode};
use log::{info, warn};
use serde_json::{Value, json};
use tari_core::base_node::LocalNodeCommsInterface;

// Test helpers use PowAlgorithm locally - not imported at module level

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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;
    use http_body_util::BodyExt;
    use hyper::StatusCode;
    use serde_json::json;
    use tari_common_types::{tari_address::TariAddress, types::FixedHash};
    use tari_core::base_node::{
        LocalNodeCommsInterface,
        comms_interface::{BlockEvent, CommsInterfaceError, NodeCommsRequest, NodeCommsResponse},
    };
    use tari_node_components::blocks::{Block, BlockBuilder};
    use tari_service_framework::reply_channel;
    use tokio::sync::broadcast;

    use super::*;
    use tari_transaction_components::tari_proof_of_work::{PowAlgorithm, ProofOfWork};

    use crate::xmrig_proxy::{
        blob::{POW_ALGO_RANDOMXT, build_tari_mining_blob},
        block_template_storage::BlockTemplateStorage,
    };

    // ---------------------------------------------------------------------------
    // Fixtures & helpers
    // ---------------------------------------------------------------------------

    /// Build a mock `LocalNodeCommsInterface` and spawn a handler task.
    ///
    /// The spawned task receives on the internal block receiver and replies to
    /// each submission with success or failure based on `submit_success`.
    fn make_mock_comms_with_block_response(
        submit_success: bool,
    ) -> (LocalNodeCommsInterface, tokio::task::JoinHandle<()>) {
        let (req_tx, _req_rx) =
            reply_channel::unbounded::<NodeCommsRequest, Result<NodeCommsResponse, CommsInterfaceError>>();
        let (block_tx, mut block_rx) = reply_channel::unbounded::<Block, Result<FixedHash, CommsInterfaceError>>();

        // Spawn handler that responds to submit_block calls.
        let handler = tokio::spawn(async move {
            while let Some(req_ctx) = block_rx.next().await {
                if submit_success {
                    req_ctx.reply(Ok(FixedHash::default())).ok();
                } else {
                    req_ctx
                        .reply(Err(CommsInterfaceError::ApiError("block invalid".into())))
                        .ok();
                }
            }
        });

        let block_event_tx: broadcast::Sender<Arc<BlockEvent>> = broadcast::channel(50).0;
        let comms = LocalNodeCommsInterface::new(req_tx, block_tx, block_event_tx);
        (comms, handler)
    }

    /// Build a valid 76-byte Tari mining blob with the given hash and nonce.
    fn make_valid_blob(mining_hash: &[u8], nonce: u64) -> Vec<u8> {
        build_tari_mining_blob(mining_hash, nonce, POW_ALGO_RANDOMXT)
    }

    /// Build a minimal Block fixture at the given height with RandomXT PoW.
    fn make_test_block(height: u64) -> Block {
        let mut block = BlockBuilder::new(0).build();
        block.header.height = height;
        block.header.pow = ProofOfWork::new(PowAlgorithm::RandomXT);
        block
    }

    /// Store a template in `BlockTemplateStorage` and return its mining hash key.
    async fn store_template(storage: &BlockTemplateStorage, height: u64) -> [u8; 32] {
        let mining_hash = [0xABu8; 32];
        let block = make_test_block(height);
        storage
            .store(
                mining_hash,
                block,
                TariAddress::default(),
                "test-miner".to_string(),
                100,
                [0x42u8; 32],
            )
            .await;
        mining_hash
    }

    /// Build a submitblock JSON-RPC request with the given blob hex.
    fn make_submit_request(blob_hex: &str) -> Value {
        json!({
            "jsonrpc": "2.0",
            "method": "submitblock",
            "params": [blob_hex],
            "id": 42,
        })
    }

    // ---------------------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn happy_path_block_accepted() {
        let mining_hash = [0xABu8; 32];
        let blob = make_valid_blob(&mining_hash, 12345);
        let blob_hex = hex::encode(&blob);

        // Mock comms: accept block submission.
        let (comms, _handler) = make_mock_comms_with_block_response(true);

        // Store template so take() finds it.
        let storage = BlockTemplateStorage::new();
        store_template(&storage, 42).await;

        let req = make_submit_request(&blob_hex);
        let result = handle_submit_block(&req, &storage, &comms).await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(parsed["error"], json!(null));
        assert_eq!(parsed["result"]["status"], "OK");
        assert_eq!(parsed["id"], 42);
    }

    #[tokio::test]
    async fn missing_params_array_returns_error() {
        let storage = BlockTemplateStorage::new();
        let (comms, _handler) = make_mock_comms_with_block_response(true);

        // Request with no params array.
        let req = json!({
            "jsonrpc": "2.0",
            "method": "submitblock",
            "id": 1,
        });

        let result = handle_submit_block(&req, &storage, &comms).await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(parsed["error"]["code"], -32602);
        assert_eq!(parsed["error"]["message"], "params must be an array");
    }

    #[tokio::test]
    async fn invalid_hex_blob_returns_error() {
        let storage = BlockTemplateStorage::new();
        let (comms, _handler) = make_mock_comms_with_block_response(true);

        // Non-hex string.
        let req = json!({
            "jsonrpc": "2.0",
            "method": "submitblock",
            "params": ["not_valid_hex!!!!"],
            "id": 3,
        });

        let result = handle_submit_block(&req, &storage, &comms).await;

        assert!(result.is_err());
        // hex::decode returns a standard library error → InvalidRequest variant.
        assert!(matches!(result.unwrap_err(), XmrigProxyError::InvalidRequest(_)));
    }

    #[tokio::test]
    async fn wrong_blob_length_returns_error() {
        let storage = BlockTemplateStorage::new();
        let (comms, _handler) = make_mock_comms_with_block_response(true);

        // Valid hex but only 70 bytes (not the expected 76).
        let short_blob = vec![0u8; 70];
        let req = json!({
            "jsonrpc": "2.0",
            "method": "submitblock",
            "params": [hex::encode(&short_blob)],
            "id": 5,
        });

        let result = handle_submit_block(&req, &storage, &comms).await;

        assert!(result.is_err());
        // parse_mining_blob returns InvalidRequest for wrong length.
        let err = result.unwrap_err();
        match err {
            XmrigProxyError::InvalidRequest(msg) => {
                assert!(msg.contains("70") || msg.contains("does not match expected"));
            },
            other => panic!("expected InvalidRequest, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn block_rejected_by_node_returns_error() {
        let mining_hash = [0xABu8; 32];
        let blob = make_valid_blob(&mining_hash, 54321);
        let blob_hex = hex::encode(&blob);

        // Mock comms: reject block submission.
        let (comms, _handler) = make_mock_comms_with_block_response(false);

        let storage = BlockTemplateStorage::new();
        store_template(&storage, 10).await;

        let req = make_submit_request(&blob_hex);
        let result = handle_submit_block(&req, &storage, &comms).await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(parsed["error"]["code"], -5);
        assert!(parsed["error"]["message"].as_str().unwrap().contains("rejected"));
    }

    #[tokio::test]
    async fn template_evicted_before_submit_returns_not_found() {
        let mining_hash = [0xEFu8; 32];
        let blob = make_valid_blob(&mining_hash, 777);
        let blob_hex = hex::encode(&blob);

        let storage = BlockTemplateStorage::new();
        // Store a template with a different hash, then take our mining_hash (which doesn't exist).
        store_template(&storage, 5).await;
        storage.take(&mining_hash).await;

        let (comms, _handler) = make_mock_comms_with_block_response(true);

        let req = make_submit_request(&blob_hex);
        let result = handle_submit_block(&req, &storage, &comms).await;

        assert!(result.is_ok());
        let response = result.unwrap();
        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: Value = serde_json::from_slice(&body_bytes).unwrap();
        // Template was already taken → not found.
        assert_eq!(parsed["error"]["code"], -1);
    }

    #[tokio::test]
    async fn nonce_patched_in_block_before_submit() {
        use std::sync::{Arc, Mutex};

        let mining_hash = [0xCDu8; 32];
        let expected_nonce: u64 = 0xDEADBEEFCAFEBABE;
        let blob = make_valid_blob(&mining_hash, expected_nonce);
        let blob_hex = hex::encode(&blob);

        // Shared state to capture the submitted block's nonce.
        let captured_nonce = Arc::new(Mutex::new(None::<u64>));
        let captured_nonce_clone = captured_nonce.clone();

        // Build mock comms where we intercept the block submission.
        let (req_tx, _req_rx) =
            reply_channel::unbounded::<NodeCommsRequest, Result<NodeCommsResponse, CommsInterfaceError>>();

        let (block_tx, mut block_rx) = reply_channel::unbounded::<Block, Result<FixedHash, CommsInterfaceError>>();

        // Use a oneshot: the handler sends once it's about to wait on block_rx.next().
        // The test awaits this signal before proceeding.
        let (handler_ready_tx, handler_ready_rx) = tokio::sync::oneshot::channel::<()>();

        // Store template BEFORE spawning handler — ensures template is ready when we submit.
        let storage = BlockTemplateStorage::new();
        // Use the same mining_hash as the blob so take() finds it.
        storage
            .store(
                mining_hash,
                make_test_block(99),
                TariAddress::default(),
                "test-miner".to_string(),
                100,
                [0x42u8; 32],
            )
            .await;

        tokio::spawn(async move {
            // Signal that we're about to wait on block_rx, then wait.
            let _ = handler_ready_tx.send(());
            if let Some(req_ctx) = block_rx.next().await {
                let block = req_ctx.request().clone();
                *captured_nonce_clone.lock().unwrap() = Some(block.header.nonce);
                req_ctx.reply(Ok(FixedHash::default())).ok();
            }
        });

        // Wait until the handler is actually listening on block_rx.
        let _ = handler_ready_rx.await;

        let block_event_tx: broadcast::Sender<Arc<BlockEvent>> = broadcast::channel(50).0;
        let comms = LocalNodeCommsInterface::new(req_tx, block_tx, block_event_tx);

        // Handler is already listening — submit immediately.
        let req = make_submit_request(&blob_hex);
        handle_submit_block(&req, &storage, &comms).await.unwrap();

        // Verify the nonce was patched in the submitted block.
        let nonce = captured_nonce.lock().unwrap().take();
        assert_eq!(nonce, Some(expected_nonce));
    }
}
