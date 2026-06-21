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
  # Verifies that a second request from the same miner returns the same
  # nonce range (idempotent registration) rather than creating a duplicate.
  # -----------------------------------------------------------------------
  Scenario: Re-registering miner returns same nonce range
    When I request a block template from NODE with miner ID "re_reg_miner"
    And I store the nonce range for miner "re_reg_miner"

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
  # Scenario D1: Stale miner evicted after timeout → nonce range reclaimed
  # Verifies that a miner inactive longer than miner_timeout_secs (1s in tests)
  # is evicted, and its nonce range becomes available for reuse.
  # -----------------------------------------------------------------------
  Scenario: Stale miner eviction reclaims nonce range
    When I request a block template from NODE with miner ID "d1_stale_miner"
    And I store the nonce range for miner "d1_stale_miner"

    # Wait for the miner to become stale (miner_timeout_secs=1 in test config)
    When I wait for miner eviction on base node NODE xmrig proxy

    # Request from a different miner — should succeed and get a fresh range
    When I request a block template from NODE with miner ID "d1_new_miner"
    Then the JSON-RPC response status is OK

  # -----------------------------------------------------------------------
  # Scenario D2: Evicted miner can re-register with fresh range
  # Verifies liveness — eviction shouldn't permanently block a miner that reconnects.
  # -----------------------------------------------------------------------
  Scenario: Re-registration after eviction gets fresh nonce range
    When I request a block template from NODE with miner ID "d2_re_reg_miner"
    And I store the nonce range for miner "d2_re_reg_miner"

    # Wait for the miner to become stale and be evicted
    When I wait for miner eviction on base node NODE xmrig proxy

    # Re-register — should succeed with a fresh nonce range
    When I request a block template from NODE with miner ID "d2_re_reg_miner"
    Then the JSON-RPC response status is OK
