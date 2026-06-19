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
use hex;
use serde_json::{Value, json};
use tari_common_types::{
    tari_address::TariAddress,
    types::{CompressedPublicKey, PrivateKey},
};
use tari_crypto::keys::SecretKey;
use tari_integration_tests::{miner::mine_blocks_without_wallet, TariWorld};

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

    // Use the world's default payment address (a valid LocalNet TariAddress) so the proxy
    // resolves miner identity from it rather than falling back to peer socket address.
    let req_body = json!({
        "jsonrpc": "2.0",
        "method": "getblocktemplate",
        "params": {"wallet_address": world.default_payment_address.to_base58()},
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
// Submit-block with stored blob patched by a specific nonce value
// ---------------------------------------------------------------------------

/// Patch the stored block template blob with a given nonce and submit it.
/// The Tari mining blob is 76 bytes: [3 zero][mining_hash:32][nonce:8 big-endian][pow_algo:1][reserved:32].
/// Nonce occupies bytes 35..43 (big-endian). This enables testing the happy-path
/// submitblock flow — get a template, patch in a nonce, submit immediately before eviction.
#[when(expr = r"I submit the stored blob with nonce {int} through base node {word} xmrig proxy")]
async fn xmrig_proxy_submit_stored_blob_with_nonce(world: &mut TariWorld, nonce: u64, base_node_name: String) {
    const TARI_BLOB_RESERVED_OFFSET: usize = 35;
    const TARI_NONCE_SIZE: usize = 8;

    let port = get_xmrig_proxy_port(world, &base_node_name);
    let url = format!("http://127.0.0.1:{port}/");

    // Retrieve the original blob hex from storage
    let blob_value = world.stored_block_template_blob.clone().expect(
        "No block template blob stored — did you run 'I store the block template blob from the last response'?",
    );
    let blob_hex = blob_value.as_str().expect("stored_block_template_blob is not a string");

    // Decode, patch nonce at offset 35 (big-endian), re-encode
    let mut blob = hex::decode(blob_hex).expect("Failed to decode stored block template blob hex");
    assert!(blob.len() >= TARI_BLOB_RESERVED_OFFSET + TARI_NONCE_SIZE,
        "Blob too short ({}) to contain nonce at offset {}", blob.len(), TARI_BLOB_RESERVED_OFFSET);

    let nonce_bytes = nonce.to_be_bytes();
    for (i, b) in nonce_bytes.iter().enumerate() {
        blob[TARI_BLOB_RESERVED_OFFSET + i] = *b;
    }

    let patched_blob_hex = hex::encode(&blob);

    let req_body = json!({
        "jsonrpc": "2.0",
        "method": "submitblock",
        "params": [patched_blob_hex],
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
// Submit-block with stored blob (original — resubmits unmodified blob)
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
async fn xmrig_proxy_wait_miner_eviction(world: &mut TariWorld, _base_node_name: String) {
    // The proxy evicts stale miners and all cached templates on each getblocktemplate call
    // (chain tip detection). Send a fresh request to trigger eviction of the previous miner.
    let port = get_xmrig_proxy_port(world, &_base_node_name);

    // Use the world's default payment address so the proxy resolves miner identity from it
    // rather than falling back to peer socket address.
    let req_body = json!({
        "jsonrpc": "2.0",
        "method": "getblocktemplate",
        "params": {"wallet_address": world.default_payment_address.to_base58()},
        "id": 99
    });

    world.last_xmrig_proxy_response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/"))
        .json(&req_body)
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Wait for block template expiry
// ---------------------------------------------------------------------------

#[when(expr = r"I wait for block template expiry on base node {word} xmrig proxy")]
async fn xmrig_proxy_wait_template_expiry(world: &mut TariWorld, _base_node_name: String) {
    // Block templates are evicted from the in-memory store when a new template
    // is generated (chain tip advances or template rotation). We trigger a tip
    // advance by mining 3 blocks on the base node, then make a getblocktemplate
    // call to force the proxy to detect the new chain tip and evict cached templates.
    use std::time::Duration;

    let mut client = world
        .get_node_client(&_base_node_name)
        .await
        .expect("Couldn't get the node client to mine with");
    let script_key_id = &world.script_key_id().await;
    mine_blocks_without_wallet(
        &mut client,
        3, // num_blocks
        0, // weight (default)
        &world.key_manager,
        script_key_id,
        &world.default_payment_address.clone(),
        false,
        &world.consensus_manager.clone(),
    )
    .await;

    // Make a getblocktemplate call to trigger chain tip detection + eviction.
    let port = get_xmrig_proxy_port(world, &_base_node_name);

    // Use the world's default payment address so the proxy resolves miner identity from it
    // rather than falling back to peer socket address.
    let req_body = json!({
        "jsonrpc": "2.0",
        "method": "getblocktemplate",
        "params": {"wallet_address": world.default_payment_address.to_base58()},
        "id": 99
    });

    world.last_xmrig_proxy_response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/"))
        .json(&req_body)
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();

    // Give the proxy time to process the eviction before submitblock runs.
    tokio::time::sleep(Duration::from_secs(1)).await;
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

// ---------------------------------------------------------------------------
// Nonce partitioning integration test steps
// ---------------------------------------------------------------------------

/// Send a getblocktemplate request with a specific wallet address as the miner identifier.
/// Each call creates its own reqwest::Client to ensure a distinct TCP connection (unique peer_addr).
#[when(expr = r"I request a block template from {word} with miner ID {string}")]
async fn xmrig_proxy_get_template_with_miner_id(
    world: &mut TariWorld,
    _base_node_name: String,
    wallet_address: String,
) {
    let port = get_xmrig_proxy_port(world, &_base_node_name);

    // Use a fresh client per call to ensure distinct peer_addr (TCP connection).
    // This guarantees each miner gets a unique identity even if wallet_address
    // doesn't parse as a valid TariAddress.
    // Send extra_nonce so the proxy resolves miner ID from it (Layer 1) instead of
    // falling back to peer socket address (Layer 2), which would pollute the
    // partitioner with stale IP:port entries.
    let req_body = json!({
        "jsonrpc": "2.0",
        "method": "getblocktemplate",
        "params": {
            "wallet_address": wallet_address,
            "extra_nonce": wallet_address
        },
        "id": 99
    });

    let resp = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/"))
        .json(&req_body)
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();

    world.last_xmrig_proxy_response = resp;
}

/// Extract and store ALL nonce ranges from the last getblocktemplate response.
/// The proxy now returns `miner_nonce_ranges` in the response which contains
/// every miner's current range — this keeps the test state consistent with what
/// the partitioner actually has after repartitioning.
#[when(expr = r"I store the nonce range for miner {string}")]
async fn xmrig_proxy_store_nonce_range(world: &mut TariWorld, miner_id: String) {
    let resp = &world.last_xmrig_proxy_response;

    // First extract the requesting miner's own range (for backwards compatibility)
    let nonce_range = resp
        .get("result")
        .and_then(|r| r.get("nonce_range"))
        .expect("Response has no 'result.nonce_range' field");

    let start = nonce_range
        .get("start")
        .and_then(Value::as_u64)
        .expect("'nonce_range.start' must be a number");
    let end = nonce_range
        .get("end")
        .and_then(Value::as_u64)
        .expect("'nonce_range.end' must be a number");

    world.miner_nonce_ranges.insert(miner_id.clone(), (start, end));

    // Also extract all OTHER miners' ranges from the response to keep test state consistent.
    // Skip the requesting miner since we already extracted it above.
    if let Some(all_ranges) = resp.get("result").and_then(|r| r.get("miner_nonce_ranges")) {
        if let Some(arr) = all_ranges.as_array() {
            for entry in arr {
                if let (Some(mid), Some(nr)) = (
                    entry.get("miner_id").and_then(Value::as_str),
                    entry.get("nonce_range"),
                ) {
                    // Skip the requesting miner — already extracted above
                    if mid == &miner_id {
                        continue;
                    }
                    // Skip peer socket address miner IDs (contain ':') — these are not test miners.
                    // The proxy uses IP:port as a fallback miner ID when no extra_nonce is provided,
                    // and those entries may persist in the partitioner until eviction.
                    if mid.contains(':') {
                        continue;
                    }
                    let s = nr.get("start").and_then(Value::as_u64);
                    let e = nr.get("end").and_then(Value::as_u64);
                    if let (Some(s), Some(e)) = (s, e) {
                        world.miner_nonce_ranges.insert(mid.to_string(), (s, e));
                    }
                }
            }
        }
    }
}

/// Assert that all stored nonce ranges are non-overlapping.
#[then(expr = r"the nonce ranges are non-overlapping")]
fn xmrig_proxy_assert_non_overlapping(world: &mut TariWorld) {
    let ranges: Vec<(String, u64, u64)> = world
        .miner_nonce_ranges
        .iter()
        .map(|(id, &(start, end))| (id.clone(), start, end))
        .collect();

    for i in 0..ranges.len() {
        for j in (i + 1)..ranges.len() {
            let (_, s1, e1) = &ranges[i];
            let (_, s2, e2) = &ranges[j];
            assert!(
                *e1 <= *s2 || *e2 <= *s1,
                "Ranges for {} ({}) and {} ({}) overlap",
                ranges[i].0,
                format!("{}..{}", s1, e1),
                ranges[j].0,
                format!("{}..{}", s2, e2)
            );
        }
    }
}

/// Assert that a specific miner's range is valid (start < u64::MAX, end == u64::MAX).
#[then(expr = r"miner {string} has a valid nonce range")]
fn xmrig_proxy_assert_valid_range(world: &mut TariWorld, miner_id: String) {
    let (start, end) = world
        .miner_nonce_ranges
        .get(&miner_id)
        .copied()
        .unwrap_or_else(|| panic!("No nonce range stored for miner '{miner_id}'"));

    assert!(
        start < u64::MAX,
        "Miner '{}' range start is {}, expected < {}",
        miner_id, start, u64::MAX
    );
    assert_eq!(
        end, u64::MAX,
        "Miner '{}' range end is {}, expected {}",
        miner_id, end, u64::MAX
    );
}

/// Assert that all stored miners have distinct nonce starts.
#[then(expr = r"the miners have distinct nonce starts")]
fn xmrig_proxy_assert_distinct_starts(world: &mut TariWorld) {
    let starts: Vec<u64> = world.miner_nonce_ranges.values().map(|&(start, _)| start).collect();
    let unique_count = starts.iter().collect::<std::collections::HashSet<_>>().len();
    assert!(
        unique_count == starts.len(),
        "Expected {} distinct nonce starts, found {} (duplicates detected)",
        starts.len(),
        unique_count
    );
}

/// Assert that N miners each have approximately equal-sized ranges.
#[then(expr = r"each of the {int} miners has an equal split")]
fn xmrig_proxy_assert_equal_split(world: &mut TariWorld, count: usize) {
    assert_eq!(
        world.miner_nonce_ranges.len(),
        count,
        "Expected {} miners in nonce ranges, got {}",
        count,
        world.miner_nonce_ranges.len()
    );

    let total_space = u64::MAX; // full 64-bit space
    let expected_size = total_space / count as u64;

    for (miner_id, &(start, end)) in world.miner_nonce_ranges.iter() {
        let size = end - start;
        assert_eq!(
            size, expected_size,
            "Miner '{}' range size is {} (expected ~{}), range: {}..{}",
            miner_id, size, expected_size, start, end
        );
    }
}

/// Assert that a submitted nonce falls within the miner's assigned range.
#[then(expr = r"nonce {int} is in miner {string}'s range")]
fn xmrig_proxy_assert_nonce_in_range(world: &mut TariWorld, nonce: u64, miner_id: String) {
    let (start, end) = world
        .miner_nonce_ranges
        .get(&miner_id)
        .copied()
        .unwrap_or_else(|| panic!("No nonce range stored for miner '{miner_id}'"));

    assert!(
        nonce >= start && nonce < end,
        "Nonce {} is not in range {}..{} for miner '{}'",
        nonce, start, end, miner_id
    );
}

/// Assert that a submitted nonce falls outside the miner's assigned range.
#[then(expr = r"nonce {int} is out of miner {string}'s range")]
fn xmrig_proxy_assert_nonce_out_of_range(world: &mut TariWorld, nonce: u64, miner_id: String) {
    let (start, end) = world
        .miner_nonce_ranges
        .get(&miner_id)
        .copied()
        .unwrap_or_else(|| panic!("No nonce range stored for miner '{miner_id}'"));

    assert!(
        nonce < start || nonce >= end,
        "Nonce {} IS in range {}..{} for miner '{}' (expected out of range)",
        nonce, start, end, miner_id
    );
}

// ===========================================================================
// Phase 1 P0: Miner Registration steps (A1–A4)
// ===========================================================================

/// Assert that the JSON-RPC response status is "OK".
#[then(expr = r"the JSON-RPC response status is OK")]
fn xmrig_proxy_assert_status_ok(world: &mut TariWorld) {
    let resp = &world.last_xmrig_proxy_response;
    let status = resp
        .get("result")
        .and_then(|r| r.get("status"))
        .and_then(Value::as_str)
        .expect("Response has no 'result.status' field");

    assert_eq!(
        status, "OK",
        "Expected status OK, got '{}'. Full response: {resp}",
        status
    );
}

/// Assert that the response contains a specific dotted-path field.
/// E.g., "nonce_range.start" checks resp.result.nonce_range.start exists.
#[then(expr = r#"the response contains field "{string}""#)]
fn xmrig_proxy_assert_response_contains_field(world: &mut TariWorld, path: String) {
    let parts: Vec<&str> = path.split('.').collect();

    // Walk the JSON tree starting from result
    let mut current = world.last_xmrig_proxy_response.get("result");
    for part in &parts {
        match current {
            Some(obj) => current = obj.get(*part),
            None => break,
        }
    }

    assert!(
        current.is_some(),
        "Response does not contain field '{}'. Full response: {}",
        path,
        world.last_xmrig_proxy_response
    );
}

// ===========================================================================
// Phase 1 P0: Payment Address steps (C1–C3) — variant with explicit wallet addr
// ===========================================================================

/// Send a getblocktemplate request with a specific miner ID and an explicit
/// wallet address. Each call creates its own reqwest::Client to ensure a
/// distinct TCP connection (unique peer_addr).
#[when(expr = r"I request a block template from {word} with miner ID {string} using wallet address {string}")]
async fn xmrig_proxy_get_template_with_miner_id_and_wallet(
    world: &mut TariWorld,
    _base_node_name: String,
    _miner_id: String,
    wallet_address: String,
) {
    let port = get_xmrig_proxy_port(world, &_base_node_name);

    // Use a fresh client per call to ensure distinct peer_addr (TCP connection).
    let req_body = json!({
        "jsonrpc": "2.0",
        "method": "getblocktemplate",
        "params": {"wallet_address": wallet_address},
        "id": 99
    });

    let resp = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/"))
        .json(&req_body)
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();

    world.last_xmrig_proxy_response = resp;
}

// ===========================================================================
// Phase 1 P0: Template Caching steps (B1–B3) — storage & assertion helpers
// ===========================================================================

/// Store the `height` field from the last response under a named key.
#[when(expr = r#"I store the response height as {string}"#)]
async fn xmrig_proxy_store_response_height(world: &mut TariWorld, name: String) {
    let height = world
        .last_xmrig_proxy_response
        .get("result")
        .and_then(|r| r.get("height"))
        .and_then(Value::as_u64)
        .expect("'result.height' must be a number");

    world.stored_values.insert(name, height.to_string());
}

/// Assert that the current response height matches a previously stored value.
#[then(expr = r#"the response height matches stored value {string}"#)]
fn xmrig_proxy_assert_height_matches(world: &mut TariWorld, name: String) {
    let expected = world
        .stored_values
        .get(&name)
        .cloned()
        .unwrap_or_else(|| panic!("No stored value for key '{}'", name));

    let actual = world
        .last_xmrig_proxy_response
        .get("result")
        .and_then(|r| r.get("height"))
        .and_then(Value::as_u64)
        .expect("'result.height' must be a number");

    assert_eq!(
        actual.to_string(),
        expected,
        "Expected height {}, got {}. Full response: {}",
        expected,
        actual,
        world.last_xmrig_proxy_response
    );
}

/// Store the `prev_hash` field from the last response under a named key.
#[when(expr = r#"I store the response prev_hash as {string}"#)]
async fn xmrig_proxy_store_response_prev_hash(world: &mut TariWorld, name: String) {
    let prev_hash = world
        .last_xmrig_proxy_response
        .get("result")
        .and_then(|r| r.get("prev_hash"))
        .and_then(Value::as_str)
        .expect("'result.prev_hash' must be a string");

    world.stored_values.insert(name, prev_hash.to_string());
}

/// Assert that the current response's prev_hash differs from a stored value.
#[then(expr = r#"the stored value {string} is different from current prev_hash"#)]
fn xmrig_proxy_assert_prev_hash_changed(world: &mut TariWorld, name: String) {
    let expected = world
        .stored_values
        .get(&name)
        .cloned()
        .unwrap_or_else(|| panic!("No stored value for key '{}'", name));

    let actual = world
        .last_xmrig_proxy_response
        .get("result")
        .and_then(|r| r.get("prev_hash"))
        .and_then(Value::as_str)
        .expect("'result.prev_hash' must be a string");

    assert_ne!(
        actual,
        &expected,
        "Expected prev_hash to differ from '{}', but got '{}'. Full response: {}",
        expected,
        actual,
        world.last_xmrig_proxy_response
    );
}

/// Assert that the current response height is greater than a stored value.
#[then(expr = r#"the response height is greater than stored value {string}"#)]
fn xmrig_proxy_assert_height_greater(world: &mut TariWorld, name: String) {
    let expected = world
        .stored_values
        .get(&name)
        .cloned()
        .unwrap_or_else(|| panic!("No stored value for key '{}'", name));

    let actual = world
        .last_xmrig_proxy_response
        .get("result")
        .and_then(|r| r.get("height"))
        .and_then(Value::as_u64)
        .expect("'result.height' must be a number");

    let expected_num: u64 = expected.parse().unwrap_or_else(|_| {
        panic!("Stored value '{}' is not a valid number", name)
    });

    assert!(
        actual > expected_num,
        "Expected height {} to be greater than stored {}, but got {}. Full response: {}",
        actual,
        expected_num,
        actual,
        world.last_xmrig_proxy_response
    );
}

// ===========================================================================
// Phase 2 P1: Max miners cap step (A5) — generates valid LocalNet TariAddresses
// ===========================================================================

/// Send a getblocktemplate request with a randomly generated LocalNet TariAddress.
/// Each call creates its own reqwest::Client to ensure distinct peer_addr, and
/// generates a unique TariAddress so each connection gets a distinct registration_id.
#[when(expr = r"I request a block template from {word} with a random wallet address")]
async fn xmrig_proxy_get_template_with_random_wallet(
    world: &mut TariWorld,
    base_node_name: String,
) {
    let port = get_xmrig_proxy_port(world, &base_node_name);

    // Generate a unique LocalNet TariAddress for this request.
    // Each call produces a distinct address so registration_ids don't collide.
    let pk = PrivateKey::random(&mut rand::rng());
    let cpk = CompressedPublicKey::from_secret_key(&pk);
    let addr: TariAddress = tari_common_types::tari_address::TariAddress::new_dual_address_with_default_features(
        cpk.clone(),
        cpk,
        tari_common::configuration::Network::LocalNet,
    )
    .expect("should create valid LocalNet address");

    let req_body = json!({
        "jsonrpc": "2.0",
        "method": "getblocktemplate",
        "params": {"wallet_address": addr.to_base58()},
        "id": 99
    });

    // Fresh client per call → distinct peer_addr (TCP connection)
    let resp = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/"))
        .json(&req_body)
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();

    world.last_xmrig_proxy_response = resp;
}

// ===========================================================================
// Phase 3: Multi-Miner Identity steps (E1, E2) — nonce range comparison
// ===========================================================================

/// Store only the requesting miner's own nonce range from `result.nonce_range`,
/// without processing the full `miner_nonce_ranges` array. This avoids overwriting
/// previously stored ranges when comparing idempotency across multiple requests.
#[when(expr = r"I store my own nonce range as {string}")]
async fn xmrig_proxy_store_own_nonce_range(world: &mut TariWorld, name: String) {
    let resp = &world.last_xmrig_proxy_response;

    let nonce_range = resp
        .get("result")
        .and_then(|r| r.get("nonce_range"))
        .expect("Response has no 'result.nonce_range' field");

    let start = nonce_range
        .get("start")
        .and_then(Value::as_u64)
        .expect("'nonce_range.start' must be a number");
    let end = nonce_range
        .get("end")
        .and_then(Value::as_u64)
        .expect("'nonce_range.end' must be a number");

    world.miner_nonce_ranges.insert(name, (start, end));
}

/// Assert that two named entries in miner_nonce_ranges have identical start/end values.
#[then(expr = r"the stored nonce ranges are identical for {word} and {word}")]
fn xmrig_proxy_assert_nonce_ranges_identical(world: &mut TariWorld, name_a: String, name_b: String) {
    let (start_a, end_a) = world
        .miner_nonce_ranges
        .get(&name_a)
        .copied()
        .unwrap_or_else(|| panic!("No nonce range stored for key '{}'", name_a));

    let (start_b, end_b) = world
        .miner_nonce_ranges
        .get(&name_b)
        .copied()
        .unwrap_or_else(|| panic!("No nonce range stored for key '{}'", name_b));

    assert_eq!(
        (start_a, end_a),
        (start_b, end_b),
        "Nonce ranges differ: '{}' is {}..{} but '{}' is {}..{}",
        name_a, start_a, end_a, name_b, start_b, end_b
    );
}

/// Send a getblocktemplate request with a specific miner ID, wallet address, and
/// extra_nonce. Each call creates its own reqwest::Client to ensure distinct peer_addr.
/// This is used for E1 where two connections share the same wallet_address but have
/// different extra_nonces — verifying they resolve to distinct miners.
#[when(expr = r"I request a block template from {word} with miner ID {string} using wallet address {string} and extra_nonce {string}")]
async fn xmrig_proxy_get_template_with_miner_id_wallet_and_extra_nonce(
    world: &mut TariWorld,
    _base_node_name: String,
    _miner_id: String,
    wallet_address: String,
    extra_nonce: String,
) {
    let port = get_xmrig_proxy_port(world, &_base_node_name);

    // Fresh client per call → distinct peer_addr (TCP connection).
    let req_body = json!({
        "jsonrpc": "2.0",
        "method": "getblocktemplate",
        "params": {
            "wallet_address": wallet_address,
            "extra_nonce": extra_nonce
        },
        "id": 99
    });

    let resp = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/"))
        .json(&req_body)
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();

    world.last_xmrig_proxy_response = resp;
}


