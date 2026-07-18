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

//! JSON-RPC 2.0 response builders for XMRig proxy responses.

use serde_json::{Value, json};

/// Build a successful JSON-RPC 2.0 response envelope.
pub fn json_rpc_success(id: Option<i64>, result: Value) -> Value {
    json!({
        "id": id.unwrap_or(-1),
        "jsonrpc": "2.0",
        "result": result,
    })
}

/// Build an error JSON-RPC 2.0 response envelope.
pub fn json_rpc_error(id: Option<i64>, code: i32, message: &str) -> Value {
    json!({
        "id": id.unwrap_or(-1),
        "jsonrpc": "2.0",
        "error": {
            "code": code,
            "message": message,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Success envelope has all required fields, correct ID, and exactly three top-level keys.
    #[test]
    fn json_rpc_success_envelope_has_all_fields() {
        let resp = json_rpc_success(Some(42), json!({"block_height": 12345}));
        assert_eq!(resp["id"], 42);
        assert_eq!(resp["jsonrpc"], "2.0");
        assert_eq!(resp["result"]["block_height"], 12345);
        assert!(resp.get("error").is_none());
        assert_eq!(resp.as_object().unwrap().len(), 3);

        // Null result is accepted with no error field.
        let resp_null = json_rpc_success(Some(0), Value::Null);
        assert_eq!(resp_null["result"], Value::Null);
        assert!(resp_null.get("error").is_none());

        // ID defaults to -1 when not provided; preserves various result types.
        let resp_no_id = json_rpc_success(None, json!("ok"));
        assert_eq!(resp_no_id["id"], -1);
        assert_eq!(resp_no_id["result"], "ok");

        let arr_resp = json_rpc_success(Some(2), Value::Array(vec![json!(1)]));
        assert_eq!(arr_resp["result"][0], 1);
    }

    /// Error envelope has all required fields, correct ID, and no result field.
    #[test]
    fn json_rpc_error_envelope_has_all_fields() {
        let resp = json_rpc_error(Some(42), -32600, "Invalid Request");
        assert_eq!(resp["id"], 42);
        assert_eq!(resp["jsonrpc"], "2.0");
        assert_eq!(resp["error"]["code"], -32600);
        assert_eq!(resp["error"]["message"], "Invalid Request");
        assert!(resp.get("result").is_none());

        // ID defaults to -1 when not provided; preserves special characters and unicode.
        let resp_no_id = json_rpc_error(None, -32000, "Max miners reached");
        assert_eq!(resp_no_id["id"], -1);
        assert_eq!(resp_no_id["error"]["code"], -32000);

        let msg_with_quotes = "Error: \"invalid\" address (network mismatch)";
        let resp_special = json_rpc_error(Some(1), -32000, msg_with_quotes);
        assert_eq!(resp_special["error"]["message"], msg_with_quotes);
    }

    /// Success and error envelopes are mutually exclusive; ID and code are JSON numbers.
    #[test]
    fn success_and_error_envelopes_are_mutually_exclusive() {
        let success = json_rpc_success(Some(1), json!({}));
        let error = json_rpc_error(Some(2), -32600, "error");

        assert!(success.get("result").is_some());
        assert!(success.get("error").is_none());
        assert!(error.get("error").is_some());
        assert!(error.get("result").is_none());

        // ID and error code are JSON numbers, not strings. Zero values are valid.
        let resp = json_rpc_error(Some(0), 0, "OK");
        assert!(resp["id"].is_number());
        assert!(resp["error"]["code"].is_number());
        assert_eq!(resp["id"], 0);
        assert_eq!(resp["error"]["code"], 0);
    }
}
