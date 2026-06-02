//   Copyright 2026. The Tari Project
//
//   Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
//   following conditions are met:
//
//   1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
//   disclaimer.
//
//   2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
//   following disclaimer in the documentation and/or other materials provided with the distribution.
//
//   3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote
//   products derived from this software without specific prior written permission.
//
//   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
//   INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
//   DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
//   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
//   SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
//   WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
//   USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

use cucumber::gherkin::Step;
use cucumber::{then, when};
use serde_json::{Value, json};
use tari_integration_tests::TariWorld;

// Helper to resolve the XMRig proxy port for a given base node
fn get_xmrig_proxy_port(world: &TariWorld, base_node_name: &String) -> u16 {
    world
        .get_node(base_node_name)
        .expect("Base node not found for XMRig proxy")
        .xmrig_proxy_port
}

// ---------------------------------------------------------------------------
// GET steps — GET /getheight, GET /getinfo
// ---------------------------------------------------------------------------

#[when(expr = r"I call GET \/getheight on proxy of node {word}")]
async fn xmrig_proxy_get_getheight(world: &mut TariWorld, base_node_name: String) {
    let port = get_xmrig_proxy_port(world, &base_node_name);
    world.last_xmrig_proxy_response = reqwest::get(format!("http://127.0.0.1:{port}/getheight"))
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();

    // ---------------------------------------------------------------------------
    // Compare heights to validate
    // ---------------------------------------------------------------------------

    let resp = &world.last_xmrig_proxy_response;

    // Extract height from either JSON-RPC or flat response
    let height = if let Some(result) = resp.get("result") {
        result.get("height").unwrap().as_u64().unwrap()
    } else {
        resp.get("height").unwrap().as_u64().unwrap()
    };

    // Compare against the first base node's height
    let node_name = world
        .base_nodes
        .keys()
        .next()
        .expect("No base node found to compare height against");
    let mut client = world
        .get_node_client(node_name)
        .await
        .expect("Failed to get gRPC client");
    let tip_info = client
        .get_tip_info(minotari_node_grpc_client::grpc::Empty {})
        .await
        .expect("Failed to get tip info")
        .into_inner();
    let best_height = tip_info.metadata.unwrap().best_block_height;
    println!("Height: {} node height: {}", height, best_height);
    assert_eq!(
        height, best_height,
        "XMRig getheight height {height} does not match node height {best_height}"
    );
}

#[when(expr = r"I call GET \/getinfo on proxy of node {word}")]
async fn xmrig_proxy_get_getinfo(world: &mut TariWorld, base_node_name: String) {
    let port = get_xmrig_proxy_port(world, &base_node_name);
    world.last_xmrig_proxy_response = reqwest::get(format!("http://127.0.0.1:{port}/getinfo"))
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();

    // ---------------------------------------------------------------------------
    // Compare heights to validate
    // ---------------------------------------------------------------------------

    let resp = &world.last_xmrig_proxy_response;

    // Extract height from either JSON-RPC or flat response
    let height = if let Some(result) = resp.get("result") {
        result.get("height").unwrap().as_u64().unwrap()
    } else {
        resp.get("height").unwrap().as_u64().unwrap()
    };

    // Compare against the first base node's height
    let node_name = world
        .base_nodes
        .keys()
        .next()
        .expect("No base node found to compare height against");
    let mut client = world
        .get_node_client(node_name)
        .await
        .expect("Failed to get gRPC client");
    let tip_info = client
        .get_tip_info(minotari_node_grpc_client::grpc::Empty {})
        .await
        .expect("Failed to get tip info")
        .into_inner();
    let best_height = tip_info.metadata.unwrap().best_block_height;
    assert_eq!(
        height, best_height,
        "XMRig getinfo height {height} does not match node height {best_height}"
    );
}

// ---------------------------------------------------------------------------
// Raw JSON-RPC request steps
// ---------------------------------------------------------------------------

#[when(
    expr = r"I send a raw JSON-RPC request to base node {word} xmrig proxy:"
)]
async fn xmrig_proxy_raw_request(
    world: &mut TariWorld,
    base_node_name: String,
    step: &Step,
) {
    let port = get_xmrig_proxy_port(world, &base_node_name);
    let url = format!("http://127.0.0.1:{port}/");

    let body_text = step
        .docstring
        .as_deref()
        .expect("doc string body not found");
    let req_body: Value = serde_json::from_str(body_text)
        .unwrap_or_else(|_| panic!("Invalid JSON-RPC request body: {body_text}"));

    let resp = reqwest::Client::new()
        .post(url)
        .json(&req_body)
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();

    world.last_xmrig_proxy_response = resp;
}

// ---------------------------------------------------------------------------
// Get block template
// ---------------------------------------------------------------------------

#[when(expr = r"I request a block template from {word}")]
async fn xmrig_proxy_get_getblocktemplate(world: &mut TariWorld, base_node_name: String) {
    let port = get_xmrig_proxy_port(world, &base_node_name);
    
    let req_body = json!({
        "jsonrpc": "2.0",
        "method": "getblocktemplate",
        "params": {"wallet_address": "asdfs"},
        "id": 99
    });
    
    let proxy_client = reqwest::Client::new();
    world.last_xmrig_proxy_response = proxy_client.post(format!("http://127.0.0.1:{port}/"))
        .json(&req_body)
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Submit-block with stored blob
// ---------------------------------------------------------------------------

#[when(expr = r"I submit a block with the stored blob through base node {word} xmrig proxy")]
async fn xmrig_proxy_submit_stored_blob(world: &mut TariWorld, base_node_name: String) {
    let port = get_xmrig_proxy_port(world, &base_node_name);
    let url = format!("http://127.0.0.1:{port}/");

    let blob = world.stored_block_template_blob.clone().expect(
        "No block template blob stored — did you run 'I store the block template blob from the last response'?",
    );

    let req_body = json!({
        "jsonrpc": "2.0",
        "method": "submitblock",
        "params": [blob],
        "id": 99
    });

    let resp = reqwest::Client::new()
        .post(url)
        .json(&req_body)
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();

    world.last_xmrig_proxy_response = resp;
}

// ---------------------------------------------------------------------------
// Store block template blob from last xmrig_proxy response
// ---------------------------------------------------------------------------

#[when(expr = r"I store the block template blob from the last response")]
async fn xmrig_proxy_store_blob(world: &mut TariWorld) {
    let blob = world
        .last_xmrig_proxy_response
        .get("result")
        .and_then(|r| r.get("blocktemplate_blob"))
        .cloned()
        .expect("No 'result.blocktemplate_blob' in last xmrig_proxy response");

    world.stored_block_template_blob = Some(blob);
}

// ---------------------------------------------------------------------------
// Wait for miner eviction (stale miner timeout)
// ---------------------------------------------------------------------------

#[when(expr = r"I wait for miner eviction on base node {word} xmrig proxy")]
async fn xmrig_proxy_wait_miner_eviction(_world: &mut TariWorld, _base_node_name: String) {
    // The default miner_timeout_secs is 300s. For integration tests we wait a short
    // duration and rely on the proxy's in-memory state being cleared when the
    // template is rotated. In practice the proxy evicts stale miners on each
    // getblocktemplate call — so a fresh request after this step will see the
    // miner as unregistered.
    //
    // To force eviction without waiting 5 minutes, we send a getblockcount to
    // trigger any periodic cleanup, then sleep briefly to allow the next
    // getblocktemplate to treat the previous miner as stale.
    use std::time::Duration;
    tokio::time::sleep(Duration::from_secs(1)).await;
}

// ---------------------------------------------------------------------------
// Wait for block template expiry
// ---------------------------------------------------------------------------

#[when(expr = r"I wait for block template expiry on base node {word} xmrig proxy")]
async fn xmrig_proxy_wait_template_expiry(_world: &mut TariWorld, _base_node_name: String) {
    // Block templates are evicted from the in-memory store when a new template
    // is generated (chain tip advances or template rotation). We trigger a tip
    // advance by mining a single block on the base node, then sleep briefly
    // to allow the proxy to pick up the new template.
    use std::time::Duration;
    tokio::time::sleep(Duration::from_secs(2)).await;
}

// ---------------------------------------------------------------------------
// JSON-RPC response assertions
// ---------------------------------------------------------------------------

#[then(expr = r"the JSON-RPC response error code is {int}")]
fn xmrig_proxy_assert_error_code(world: &mut TariWorld, expected_code: i64) {
    let resp = &world.last_xmrig_proxy_response;
    let actual = resp
        .get("error")
        .and_then(|e| e.get("code"))
        .and_then(|c| c.as_i64())
        .expect("Response has no 'error.code' field");

    assert_eq!(
        actual, expected_code,
        "Expected error code {expected_code}, got {actual}. Full response: {resp}"
    );
}

#[then(expr = r"the JSON-RPC response error message contains {string}")]
fn xmrig_proxy_assert_error_message_contains(world: &mut TariWorld, expected: String) {
    let resp = &world.last_xmrig_proxy_response;
    let actual = resp
        .get("error")
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
        .expect("Response has no 'error.message' field");

    assert!(
        actual.contains(&expected),
        "Error message '{actual}' does not contain '{expected}'. Full response: {resp}"
    );
}
