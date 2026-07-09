# Copyright 2022 The Tari Project
# SPDX-License-Identifier: BSD-3-Clause

@xmrig-proxy @base-node
Feature: XMRig Proxy JSON-RPC Error Handling

    Background:
        Given I have a seed node NODE


    @slow
    Scenario: 4_SubmitBlock_ExpiredTemplate
        # Get a template, wait for it to expire, then submit.
        # Given I have a merge mining proxy "proxy" connected to "base_node" with default config
        When I request a block template from NODE
        And I store the block template blob from the last response
        And I wait for block template expiry on base node NODE xmrig proxy
        When I submit a block with the stored blob through base node NODE xmrig proxy
        Then the JSON-RPC response error code is -1
        And the JSON-RPC response error message contains "not found"

    @slow
    Scenario: 5_SubmitBlock_InvalidHexBlob
        # Submitblock requires params[0] to be a valid hex string. Non-hex
        # characters trigger InvalidRequest which the service layer maps to -32603.
        When I send a raw JSON-RPC request to base node NODE xmrig proxy:
          """
          {
            "jsonrpc": "2.0",
            "method": "submitblock",
            "params": ["not-valid-hex!!!"],
            "id": 1
          }
          """
        Then the JSON-RPC response error code is -32603

    # -----------------------------------------------------------------------
    # Scenario E1: Unknown method returns -32601 (JSON-RPC spec)
    # Verifies that an unrecognized JSON-RPC method returns the standard
    # "Method not found" error with code -32601. This is a direct response
    # from inner.rs dispatch, NOT mapped through the service layer.
    # -----------------------------------------------------------------------
    Scenario: 6_UnknownMethod_ReturnsMinus32601
        When I send a raw JSON-RPC request to base node NODE xmrig proxy:
          """
          {
            "jsonrpc": "2.0",
            "method": "unknown_method_xyz",
            "params": {},
            "id": 7
          }
          """
        Then the JSON-RPC response error code is -32601
        And the JSON-RPC response error message contains "Method not found"

    # -----------------------------------------------------------------------
    # Scenario E2: submitblock with non-array params returns -32602
    # The handler checks req["params"].as_array() — if it's a string/object,
    # returns -32602 "params must be an array". Direct response from inner.rs.
    # -----------------------------------------------------------------------
    Scenario: 7_SubmitBlock_NonArrayParams_ReturnsMinus32602
        When I send a raw JSON-RPC request to base node NODE xmrig proxy:
          """
          {
            "jsonrpc": "2.0",
            "method": "submitblock",
            "params": {"not": "an_array"},
            "id": 8
          }
          """
        Then the JSON-RPC response error code is -32602
        And the JSON-RPC response error message contains "params must be an array"

    # -----------------------------------------------------------------------
    # Scenario E3: submitblock with non-string params[0] returns -32602
    # The handler checks params.first().and_then(Value::as_str) — if it's a
    # number/null/array, returns -32602 "params[0] must be a hex string".
    # -----------------------------------------------------------------------
    Scenario: 8_SubmitBlock_NonStringParams0_ReturnsMinus32602
        When I send a raw JSON-RPC request to base node NODE xmrig proxy:
          """
          {
            "jsonrpc": "2.0",
            "method": "submitblock",
            "params": [12345],
            "id": 9
          }
          """
        Then the JSON-RPC response error code is -32602
        And the JSON-RPC response error message contains "params[0] must be a hex string"

    # -----------------------------------------------------------------------
    # Scenario E4: submitblock with null params[0] returns -32602
    # Same as E3 but with null instead of a number.
    # -----------------------------------------------------------------------
    Scenario: 9_SubmitBlock_NullParams0_ReturnsMinus32602
        When I send a raw JSON-RPC request to base node NODE xmrig proxy:
          """
          {
            "jsonrpc": "2.0",
            "method": "submitblock",
            "params": [null],
            "id": 10
          }
          """
        Then the JSON-RPC response error code is -32602
        And the JSON-RPC response error message contains "params[0] must be a hex string"

    # -----------------------------------------------------------------------
    # Scenario E5: Malformed JSON body returns -32603 (service layer)
    # The service layer tries serde_json::from_slice on the raw body. If it
    # fails, it returns InvalidRequest mapped to -32603 via the error handler.
    # -----------------------------------------------------------------------
    Scenario: 10_MalformedJSONBody_ReturnsMinus32603
        When I send a malformed JSON-RPC request to base node NODE xmrig proxy:
          """
          {jsonrpc: "2.0", method: broken!!!}
          """
        Then the JSON-RPC response error code is -32603

    # -----------------------------------------------------------------------
    # Scenario E6: GET /unknown_path returns 404 Not Found
    # The handle_get() method matches known paths; anything else returns
    # StatusCode::NOT_FOUND with {"error": "Not found"}.
    # -----------------------------------------------------------------------
    Scenario: 11_GET_UnknownPath_Returns404
        When I call GET /nonexistent_path on proxy of node NODE
        Then the HTTP response status is 404
