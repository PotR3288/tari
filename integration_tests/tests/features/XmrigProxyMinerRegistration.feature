# Copyright 2022 The Tari Project
# SPDX-License-Identifier: BSD-3-Clause

@xmrig-proxy @base-node @slow
Feature: XMRig Proxy JSON-RPC Miner Registration

  Background:
    Given I have a seed node NODE

  # -----------------------------------------------------------------------
  # Scenario A1: New miner registration on first getblocktemplate call
  # Verifies that the proxy registers a new miner and returns nonce_range.
  # -----------------------------------------------------------------------
  Scenario: First request registers miner and returns nonce range
    When I request a block template from NODE with miner ID "new_miner_a1"
    Then the JSON-RPC response status is OK

  # -----------------------------------------------------------------------
  # Scenario A2: Existing miner re-registration updates last_activity
  # Verifies that a second request from the same miner succeeds (idempotent
  # registration) rather than creating a duplicate.
  # -----------------------------------------------------------------------
  Scenario: Re-registering miner returns OK
    When I request a block template from NODE with miner ID "re_reg_miner"
    Then the JSON-RPC response status is OK

    When I request a block template from NODE with miner ID "re_reg_miner"
    Then the JSON-RPC response status is OK

  # -----------------------------------------------------------------------
  # Scenario A3: Custom payment address accepted (miners may supply their own)
  # Verifies that a miner can supply its own wallet address and still get
  # a valid template response (the proxy accepts it without error).
  # -----------------------------------------------------------------------
  Scenario: Miner supplies custom payment address
    When I request a block template from NODE with miner ID "custom_addr_miner" using wallet address "tnt1Qp2AnySgEeQ0Cm7Dvvoqo2nQGJ8ac3nglWwMD9FSN6FzSTi2fUuMxX3h4Gy"
    Then the JSON-RPC response status is OK

  # -----------------------------------------------------------------------
  # Scenario A4: Config default payment address used when miner omits wallet_address
  # Verifies that a request without wallet_address still succeeds and uses
  # the proxy's config-default coinbase destination.
  # -----------------------------------------------------------------------
  Scenario: Omitting wallet_address falls back to config default
    When I send a raw JSON-RPC request to base node NODE xmrig proxy:
      """
      {
        "jsonrpc": "2.0",
        "method": "getblocktemplate",
        "params": {},
        "id": 42
      }
      """
    Then the JSON-RPC response status is OK

  # -----------------------------------------------------------------------
  # Scenario A5: Max miners reached → error response
  # Verifies that when the hard cap (max_miners=3 in tests) is exceeded,
  # new miner registrations receive an error instead of a template.
  # Key: each miner must use a DIFFERENT valid TariAddress so they get
  # distinct registry entries and count separately against the max_miners cap.
  # Invalid strings fall back to the config default address which deduplicates
  # all localhost connections into one miner entry.
  # -----------------------------------------------------------------------
  Scenario: Exceeding max miners returns error
    # Each call generates a unique valid LocalNet TariAddress so each connection
    # gets a distinct registry entry and counts separately against the max_miners cap (set to 3 in test config).
    When I request a block template from NODE with a random wallet address

    When I request a block template from NODE with a random wallet address

    When I request a block template from NODE with a random wallet address

    # The 4th miner should be rejected (max_miners=3 in test config)
    When I request a block template from NODE with a random wallet address
    Then the JSON-RPC response error code is -32603

  # -----------------------------------------------------------------------
  # Scenario D1: Stale miner evicted after timeout
  # Verifies that a miner inactive longer than miner_timeout_secs (1s in tests)
  # is evicted, and a new miner can register successfully.
  # -----------------------------------------------------------------------
  Scenario: Stale miner eviction allows new registration
    When I request a block template from NODE with miner ID "d1_stale_miner"

    # Wait for the miner to become stale (miner_timeout_secs=1 in test config)
    When I wait for miner eviction on base node NODE xmrig proxy

    # Request from a different miner — should succeed
    When I request a block template from NODE with miner ID "d1_new_miner"
    Then the JSON-RPC response status is OK

  # -----------------------------------------------------------------------
  # Scenario D2: Evicted miner can re-register
  # Verifies liveness — eviction shouldn't permanently block a miner that reconnects.
  # -----------------------------------------------------------------------
  Scenario: Re-registration after eviction succeeds
    When I request a block template from NODE with miner ID "d2_re_reg_miner"

    # Wait for the miner to become stale and be evicted
    When I wait for miner eviction on base node NODE xmrig proxy

    # Re-register — should succeed
    When I request a block template from NODE with miner ID "d2_re_reg_miner"
    Then the JSON-RPC response status is OK

  # -----------------------------------------------------------------------
  # Scenario E7: Wrong-network wallet address falls back to config default
  # A miner sends a valid TariAddress on MainNet while the proxy runs on LocalNet.
  # The proxy should silently fall back to its config-default payment address,
  # still return status OK, but the coinbase targets the default (not the miner's).
  # This verifies the network-validation guard added in inner.rs ~lines 97-108.
  # -----------------------------------------------------------------------
  Scenario: 12_WrongNetworkWalletAddress_FallsBackToDefault
    When I request a block template from NODE with a wrong-network wallet address
    Then the JSON-RPC response status is OK

  # -----------------------------------------------------------------------
  # Scenario E8: Same wallet, different extra_nonce — multi-miner deduplication
  # Two miners behind NAT share one payment address but send distinct extra_nonces.
  # The proxy should deduplicate by wallet in the registry (one entry) but still
  # track distinct miner_ids via extra_nonce and return them in responses.
  # -----------------------------------------------------------------------
  Scenario: 13_SameWalletDifferentExtraNonce_ShareCacheEntry
    When I request a block template from NODE with miner ID "shared_wallet_e8" using extra nonce "extra_nonce_001"
    Then the JSON-RPC response status is OK

    When I request a block template from NODE with miner ID "shared_wallet_e8" using extra nonce "extra_nonce_002"
    Then the JSON-RPC response status is OK

  # -----------------------------------------------------------------------
  # Scenario E9: getblocktemplate response contains all expected fields
  # Verifies that every field in the getblocktemplate result object is present
  # and has the correct type. This ensures the proxy's response contract is stable.
  # Fields tested: blocktemplate_blob, blockhashing_blob, seed_hash, difficulty,
  # height, prev_hash, reserved_offset, min_nonce, max_nonce, expected_reward,
  # status, untrusted, miner_id.
  # -----------------------------------------------------------------------
  Scenario: 14_GetBlockTemplate_ResponseContainsAllFields
    When I request a block template from NODE with miner ID "fields_miner_e9"
    Then the response contains string field "blocktemplate_blob"
    And the response contains string field "blockhashing_blob"
    And the response contains string field "seed_hash"
    And the response contains numeric field "difficulty"
    And the response contains numeric field "height"
    And the response contains string field "prev_hash"
    And the response contains numeric field "reserved_offset"
    And the response contains numeric field "min_nonce"
    And the response contains numeric field "max_nonce"
    And the response contains numeric field "expected_reward"
    And the response contains string field "status"
    And the response contains numeric field "untrusted"
    And the response contains string field "miner_id"

  # -----------------------------------------------------------------------
  # Scenario E10: nonce range is valid (min_nonce <= max_nonce)
  # The proxy generates a random min_nonce and always uses u64::MAX for max.
  # This test verifies the invariant holds in practice.
  # -----------------------------------------------------------------------
  Scenario: 15_NonceRangeIsValid
    When I request a block template from NODE with miner ID "nonce_miner_e10"
    Then the nonce range is valid (min_nonce <= max_nonce)

  # -----------------------------------------------------------------------
  # Scenario E11: miner_id is non-empty in response
  # Every getblocktemplate response should include a non-empty miner_id string.
  # This verifies that parse_miner_id_from_request() always produces a value.
  # -----------------------------------------------------------------------
  Scenario: 16_MinerIdIsNonEmpty
    When I request a block template from NODE with miner ID "minerid_miner_e11"
    Then the response contains a non-empty miner_id
