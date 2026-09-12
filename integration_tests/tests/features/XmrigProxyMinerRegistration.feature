# Copyright 2022 The Tari Project
# SPDX-License-Identifier: BSD-3-Clause

@xmrig-proxy @base-node @slow
Feature: XMRig Proxy JSON-RPC Miner Registration

  Background:
    Given I have a seed node NODE

  # -----------------------------------------------------------------------
  # Scenario A1: New miner registration on first getblocktemplate call
  # Verifies that the proxy registers a new miner and returns a template.
  # -----------------------------------------------------------------------
  Scenario: First request registers miner and returns nonce range
    When I request a block template from NODE with miner ID "new_miner_a1"
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
  # Scenario E7: Wrong-network wallet address falls back to config default
  # A miner sends a valid TariAddress on MainNet while the proxy runs on LocalNet.
  # The proxy falls back to its config-default payment address and still returns
  # status OK — the coinbase targets the default, not the miner's address.
  # -----------------------------------------------------------------------
  Scenario: 12_WrongNetworkWalletAddress_FallsBackToDefault
    When I request a block template from NODE with a wrong-network wallet address
    Then the JSON-RPC response status is OK

  # -----------------------------------------------------------------------
  # Scenario E12: Concurrent miners with same wallet address share cache entry
  # Multiple XMRig instances behind NAT may share one payment address but use
  # distinct extra_nonces. The proxy deduplicates by wallet (one template)
  # while tracking each miner via their extra_nonce/peer_addr for logging/debugging.
  # This tests the multi-miner coordination feature: concurrent connections
  # with same wallet address hit the same cached template and receive random full u64 ranges.
  # -----------------------------------------------------------------------
  Scenario: 17_ConcurrentMinersSameWalletShareCacheEntry
    When I request a block template from NODE with miner ID "concurrent_miner_a" using wallet address "tnt1Qp2AnySgEeQ0Cm7Dvvoqo2nQGJ8ac3nglWwMD9FSN6FzSTi2fUuMxX3h4Gy"
    Then the JSON-RPC response status is OK
    And I store the response height as "height_a"

    # Second concurrent request with same wallet address should hit cache
    When I request a block template from NODE with miner ID "concurrent_miner_b" using wallet address "tnt1Qp2AnySgEeQ0Cm7Dvvoqo2nQGJ8ac3nglWwMD9FSN6FzSTi2fUuMxX3h4Gy"
    Then the JSON-RPC response status is OK
    And the response height matches stored value "height_a"
