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
use serde_json::{Value, json};
use tari_core::base_node::LocalNodeCommsInterface;

use super::{
    error::XmrigProxyError,
    service::ProxyBody,
};

/// Handle GET /get_height, /getinfo, /getheight requests (some mining software uses these).
pub async fn handle_get(
    path: &str,
    handler: &mut LocalNodeCommsInterface,
) -> Result<Response<ProxyBody>, XmrigProxyError> {
    match path {
        "/get_height" | "/getblockcount" => get_height(&json!({}), handler).await,
        "/getheight" => get_height_hash(handler).await,
        "/getinfo" | "/get_info" => get_info(handler).await,
        _ => super::service::json_response(StatusCode::NOT_FOUND, &json!({"error": "Not found"})),
    }
}

/// Handle GET /get_height and /getblockcount — returns block count.
async fn get_height(req: &Value, handler: &mut LocalNodeCommsInterface) -> Result<Response<ProxyBody>, XmrigProxyError> {
    let tip = super::chain_tip::get_chain_tip(handler).await?;
    super::service::json_response(
        StatusCode::OK,
        &super::json_rpc::json_rpc_success(
            req["id"].get("id").map(|v| v.as_i64()).unwrap_or_default(),
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
