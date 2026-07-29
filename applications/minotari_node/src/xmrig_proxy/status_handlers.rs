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

//! GET endpoint handlers for the XMRig proxy — height, hash, and info endpoints.
//!
//! These are lightweight read-only queries that return chain tip information to mining
//! software (some miners use these endpoints to report status or display network info).

use hyper::{Response, StatusCode};
use log::{debug, warn};
use serde_json::{Value, json};
use tari_core::base_node::LocalNodeCommsInterface;

use super::{error::XmrigProxyError, service::ProxyBody};

const LOG_TARGET: &str = "minotari::base_node::xmrig_proxy::status_handlers";

/// Handle GET /get_height, /getinfo, /getheight requests (some mining software uses these).
pub async fn handle_get(
    path: &str,
    handler: &mut LocalNodeCommsInterface,
) -> Result<Response<ProxyBody>, XmrigProxyError> {
    debug!(target: LOG_TARGET, "GET request for path: {}", path);
    
    let result = match path {
        "/get_height" | "/getblockcount" => get_height(&json!({}), handler).await,
        "/getheight" => get_height_hash(handler).await,
        "/getinfo" | "/get_info" => get_info(handler).await,
        _ => {
            debug!(target: LOG_TARGET, "Unknown path: {}", path);
            super::service::json_response(StatusCode::NOT_FOUND, &json!({"error": "Not found"}))
        },
    };
    
    match &result {
        Ok(_) => {
            debug!(target: LOG_TARGET, "GET {} succeeded", path);
        },
        Err(e) => {
            warn!(
                target: LOG_TARGET,
                "GET {} failed: {}", path, e
            );
        },
    }
    
    Ok(result?)
}

/// Handle GET /get_height and /getblockcount — returns block count.
async fn get_height(
    req: &Value,
    handler: &mut LocalNodeCommsInterface,
) -> Result<Response<ProxyBody>, XmrigProxyError> {
    let tip = super::chain_tip::get_chain_tip(handler).await?;
    super::service::json_response(
        StatusCode::OK,
        &super::json_rpc::json_rpc_success(
            Some(req.get("id").and_then(Value::as_i64).unwrap_or(-1)),
            json!({ "count": tip.height, "status": "OK" }),
        ),
    )
}

/// Handle GET /getheight — returns height and hash.
async fn get_height_hash(handler: &mut LocalNodeCommsInterface) -> Result<Response<ProxyBody>, XmrigProxyError> {
    let tip = super::chain_tip::get_chain_tip(handler).await?;
    super::service::json_response(
        StatusCode::OK,
        &json!({
            "height": tip.height,
            "hash": format!("{}", tip.top_hash),
            "status": "OK",
        }),
    )
}

/// Handle GET /getinfo — returns top block hash and height.
async fn get_info(handler: &mut LocalNodeCommsInterface) -> Result<Response<ProxyBody>, XmrigProxyError> {
    let tip = super::chain_tip::get_chain_tip(handler).await?;
    super::service::json_response(
        StatusCode::OK,
        &json!({
            "top_block_hash": format!("{}", tip.top_hash),
            "height": tip.height,
            "status": "OK",
        }),
    )
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use http_body_util::BodyExt;
    use serde_json::Value;

    use super::*;
    use tari_core::base_node::comms_interface::{BlockEvent, CommsInterfaceError, NodeCommsRequest, NodeCommsResponse};
    use tari_service_framework::reply_channel;

    // ---------------------------------------------------------------------------
    // Path dispatch — known paths (with mock comms)
    // ---------------------------------------------------------------------------

    /// Build a LocalNodeCommsInterface backed by reply channels. Returns the
    /// interface and the receiver side for dispatching canned responses.
    fn make_mock_comms() -> (
        LocalNodeCommsInterface,
        reply_channel::Receiver<NodeCommsRequest, Result<NodeCommsResponse, CommsInterfaceError>>,
    ) {
        let (req_tx, req_rx) =
            reply_channel::unbounded::<NodeCommsRequest, Result<NodeCommsResponse, CommsInterfaceError>>();
        let (block_tx, _block_rx) = reply_channel::unbounded::<
            tari_node_components::blocks::Block,
            Result<tari_common_types::types::FixedHash, CommsInterfaceError>,
        >();
        let block_event_tx: tokio::sync::broadcast::Sender<std::sync::Arc<BlockEvent>> =
            tokio::sync::broadcast::channel(50).0;

        (LocalNodeCommsInterface::new(req_tx, block_tx, block_event_tx), req_rx)
    }

    fn make_chain_metadata(height: u64, hash_byte: u8) -> tari_common_types::chain_metadata::ChainMetadata {
        use primitive_types::U512;
        tari_common_types::chain_metadata::ChainMetadata::new(
            height,
            tari_common_types::types::FixedHash::new([hash_byte; 32]),
            0,
            0,
            U512::from(1u64),
            0,
        )
        .expect("valid metadata")
    }

    /// Dispatch mock requests to respond to chain metadata queries.
    async fn dispatch_mock_requests(
        mut rx: reply_channel::Receiver<NodeCommsRequest, Result<NodeCommsResponse, CommsInterfaceError>>,
        height: u64,
        hash_byte: u8,
    ) {
        while let Some(req_ctx) = rx.next().await {
            match req_ctx.request() {
                NodeCommsRequest::GetChainMetadata => {
                    let metadata = make_chain_metadata(height, hash_byte);
                    let _ = req_ctx.reply(Ok(NodeCommsResponse::ChainMetadata(metadata)));
                },
                _ => {},
            }
        }
    }

    // ---------------------------------------------------------------------------
    // Path equivalence tests
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn get_height_and_getblockcount_return_same_structure() {
        let (comms_a, rx_a) = make_mock_comms();
        tokio::task::spawn(dispatch_mock_requests(rx_a, 8888, 0x88));

        let result_a = handle_get("/get_height", &mut comms_a.clone()).await;
        assert!(result_a.is_ok());

        let (comms_b, rx_b) = make_mock_comms();
        tokio::task::spawn(dispatch_mock_requests(rx_b, 8888, 0x88));

        let result_b = handle_get("/getblockcount", &mut comms_b.clone()).await;
        assert!(result_b.is_ok());

        // Both should return json_rpc_success envelopes with count field
        let body_a = result_a.unwrap().into_body().collect().await.unwrap().to_bytes();
        let parsed_a: Value = serde_json::from_slice(&body_a).unwrap();

        let body_b = result_b.unwrap().into_body().collect().await.unwrap().to_bytes();
        let parsed_b: Value = serde_json::from_slice(&body_b).unwrap();

        assert_eq!(parsed_a["result"]["count"], parsed_b["result"]["count"]);
    }

    #[tokio::test]
    async fn getinfo_and_get_info_return_same_structure() {
        let (comms_a, rx_a) = make_mock_comms();
        tokio::task::spawn(dispatch_mock_requests(rx_a, 5555, 0x55));

        let result_a = handle_get("/getinfo", &mut comms_a.clone()).await;
        assert!(result_a.is_ok());

        let (comms_b, rx_b) = make_mock_comms();
        tokio::task::spawn(dispatch_mock_requests(rx_b, 5555, 0x55));

        let result_b = handle_get("/get_info", &mut comms_b.clone()).await;
        assert!(result_b.is_ok());

        // Both should return flat objects with top_block_hash and height
        let body_a = result_a.unwrap().into_body().collect().await.unwrap().to_bytes();
        let parsed_a: Value = serde_json::from_slice(&body_a).unwrap();

        let body_b = result_b.unwrap().into_body().collect().await.unwrap().to_bytes();
        let parsed_b: Value = serde_json::from_slice(&body_b).unwrap();

        assert_eq!(parsed_a["height"], parsed_b["height"]);
    }

    // ---------------------------------------------------------------------------
    // Hash propagation tests
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn get_height_hash_returns_non_empty_hash() {
        let (comms, rx) = make_mock_comms();
        tokio::task::spawn(dispatch_mock_requests(rx, 100, 0xAB));

        let result = handle_get("/getheight", &mut comms.clone()).await;
        assert!(result.is_ok());
        let response = result.unwrap();
        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: Value = serde_json::from_slice(&body_bytes).unwrap();

        let hash = parsed["hash"].as_str().expect("hash should be a string");
        assert!(!hash.is_empty(), "hash should not be empty");
    }

    #[tokio::test]
    async fn get_info_returns_non_empty_top_block_hash() {
        let (comms, rx) = make_mock_comms();
        tokio::task::spawn(dispatch_mock_requests(rx, 100, 0xCD));

        let result = handle_get("/getinfo", &mut comms.clone()).await;
        assert!(result.is_ok());
        let response = result.unwrap();
        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: Value = serde_json::from_slice(&body_bytes).unwrap();

        let hash = parsed["top_block_hash"]
            .as_str()
            .expect("top_block_hash should be a string");
        assert!(!hash.is_empty(), "top_block_hash should not be empty");
    }

    // ---------------------------------------------------------------------------
    // Status field consistency tests
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn get_height_includes_status_ok() {
        let (comms, rx) = make_mock_comms();
        tokio::task::spawn(dispatch_mock_requests(rx, 1, 0x01));

        let result = handle_get("/get_height", &mut comms.clone()).await;
        assert!(result.is_ok());
        let response = result.unwrap();
        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: Value = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(parsed["result"]["status"], "OK");
    }

    // ---------------------------------------------------------------------------
    // Path dispatch tests
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn unknown_path_returns_404() {
        let (comms, _rx) = make_mock_comms();

        let result = handle_get("/unknown_path", &mut comms.clone()).await;
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: Value = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(parsed["error"], "Not found");
    }

    // ---------------------------------------------------------------------------
    // get_height / getblockcount tests
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn get_height_returns_correct_count() {
        let (comms, rx) = make_mock_comms();
        tokio::task::spawn(dispatch_mock_requests(rx, 9999, 0x99));

        let result = handle_get("/get_height", &mut comms.clone()).await;
        assert!(result.is_ok());

        let response = result.unwrap();
        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: Value = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(parsed["result"]["count"], 9999);
    }

    #[tokio::test]
    async fn getblockcount_returns_correct_count() {
        let (comms, rx) = make_mock_comms();
        tokio::task::spawn(dispatch_mock_requests(rx, 7777, 0x77));

        let result = handle_get("/getblockcount", &mut comms.clone()).await;
        assert!(result.is_ok());

        let response = result.unwrap();
        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: Value = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(parsed["result"]["count"], 7777);
    }

    // ---------------------------------------------------------------------------
    // getheight tests
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn getheight_returns_height_and_hash() {
        let (comms, rx) = make_mock_comms();
        tokio::task::spawn(dispatch_mock_requests(rx, 3333, 0x33));

        let result = handle_get("/getheight", &mut comms.clone()).await;
        assert!(result.is_ok());

        let response = result.unwrap();
        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: Value = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(parsed["height"], 3333);
        assert_eq!(
            parsed["hash"],
            "3333333333333333333333333333333333333333333333333333333333333333"
        );
        assert_eq!(parsed["status"], "OK");
    }

    // ---------------------------------------------------------------------------
    // getinfo tests
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn getinfo_returns_top_block_hash_and_height() {
        let (comms, rx) = make_mock_comms();
        tokio::task::spawn(dispatch_mock_requests(rx, 4444, 0x44));

        let result = handle_get("/getinfo", &mut comms.clone()).await;
        assert!(result.is_ok());

        let response = result.unwrap();
        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: Value = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(
            parsed["top_block_hash"],
            "4444444444444444444444444444444444444444444444444444444444444444"
        );
        assert_eq!(parsed["height"], 4444);
        assert_eq!(parsed["status"], "OK");
    }

    #[tokio::test]
    async fn get_info_returns_top_block_hash_and_height() {
        let (comms, rx) = make_mock_comms();
        tokio::task::spawn(dispatch_mock_requests(rx, 5555, 0x55));

        let result = handle_get("/get_info", &mut comms.clone()).await;
        assert!(result.is_ok());

        let response = result.unwrap();
        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: Value = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(
            parsed["top_block_hash"],
            "5555555555555555555555555555555555555555555555555555555555555555"
        );
        assert_eq!(parsed["height"], 5555);
        assert_eq!(parsed["status"], "OK");
    }

    // ---------------------------------------------------------------------------
    // Chain tip error handling tests
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn get_height_propagates_comms_error() {
        let (comms, mut rx) = make_mock_comms();
        tokio::task::spawn(async move {
            if let Some(req_ctx) = rx.next().await {
                let _ = req_ctx.reply(Err(CommsInterfaceError::UnexpectedApiResponse));
            }
        });

        let result = handle_get("/get_height", &mut comms.clone()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn getinfo_propagates_comms_error() {
        let (comms, mut rx) = make_mock_comms();
        tokio::task::spawn(async move {
            if let Some(req_ctx) = rx.next().await {
                let _ = req_ctx.reply(Err(CommsInterfaceError::UnexpectedApiResponse));
            }
        });

        let result = handle_get("/getinfo", &mut comms.clone()).await;
        assert!(result.is_err());
    }

    // ---------------------------------------------------------------------------
    // Response structure validation tests
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn get_height_response_has_json_rpc_structure() {
        let (comms, rx) = make_mock_comms();
        tokio::task::spawn(dispatch_mock_requests(rx, 100, 0x66));

        let result = handle_get("/get_height", &mut comms.clone()).await;
        assert!(result.is_ok());

        let response = result.unwrap();
        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: Value = serde_json::from_slice(&body_bytes).unwrap();

        // Verify JSON-RPC structure
        assert!(parsed.is_object());
        assert_eq!(parsed.get("jsonrpc"), Some(&Value::String("2.0".to_string())));
        assert_eq!(parsed["result"]["count"], 100);
    }

    #[tokio::test]
    async fn getinfo_response_has_correct_fields() {
        let (comms, rx) = make_mock_comms();
        tokio::task::spawn(dispatch_mock_requests(rx, 200, 0x77));

        let result = handle_get("/getinfo", &mut comms.clone()).await;
        assert!(result.is_ok());

        let response = result.unwrap();
        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: Value = serde_json::from_slice(&body_bytes).unwrap();

        // Verify all expected fields are present
        assert!(parsed.get("top_block_hash").is_some());
        assert!(parsed.get("height").is_some());
        assert!(parsed.get("status").is_some());

        // Verify types
        assert!(parsed["top_block_hash"].is_string());
        assert!(parsed["height"].is_number());
        assert!(parsed["status"].is_string());
    }
}
