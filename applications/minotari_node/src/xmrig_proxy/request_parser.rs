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

//! Request parsing helpers for XMRig JSON-RPC requests.
//!
//! Two-layer miner identity resolution:
//! 1. `params.extra_nonce` — per-connection random nonce sent by XMRig (Tari fork).
//!    Guarantees unique miner IDs even when multiple instances share the same wallet.
//! 2. `peer_addr` — fall back to the remote socket address (IP:port) of the TCP connection.

use std::{net::SocketAddr, str::FromStr, sync::atomic::{AtomicU64, Ordering}};

use log::debug;
use serde_json::Value;
use tari_common_types::tari_address::TariAddress;

use super::MinerId;

/// Extract miner identity from a JSON-RPC request and peer address.
///
/// Used for nonce partitioning — each connection gets a unique ID so threads
/// don't search overlapping nonce ranges.
pub fn parse_miner_id_from_request(req: &Value, peer_addr: SocketAddr) -> MinerId {
    // Layer 1: per-connection extra_nonce from XMRig (Tari fork).
    if let Some(extra_nonce) = req
        .get("params")
        .and_then(|p| p.get("extra_nonce"))
        .and_then(Value::as_str)
    {
        return extra_nonce.to_string();
    }

    // Layer 2: peer socket address (IP:port) — distinguishes miners behind NAT
    if peer_addr != SocketAddr::from(([0, 0, 0, 0], 0)) {
        return peer_addr.to_string();
    }

    // Layer 3: auto-generated unique ID — monotonic counter guarantees uniqueness
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

/// Extract an optional wallet address override from a JSON-RPC request.
pub fn parse_wallet_address_from_request(req: &Value) -> Option<TariAddress> {
    let address_str = req
        .get("params")
        .and_then(|p| p.get("wallet_address"))
        .and_then(|v| v.as_str())?;
    TariAddress::from_str(address_str).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
}
