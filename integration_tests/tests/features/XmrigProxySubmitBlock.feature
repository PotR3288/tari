# Copyright 2022 The Tari Project
# SPDX-License-Identifier: BSD-3-Clause

@xmrig-proxy @base-node @slow
Feature: XMRig Proxy SubmitBlock Happy Path

  Background:
    Given I have a seed node NODE

  # -----------------------------------------------------------------------
  # Scenario S1: Successful block submission via proxy
  # Get a template from the proxy, patch in a nonce value, and submit it
  # immediately (before chain tip advance or template eviction). On LocalNet
  # difficulty=1 so any hash is valid — the base node accepts the block.
  # This exercises the full write path: getblocktemplate → cache store →
  # submitblock → cache take → nonce patching → base node submission.
  # -----------------------------------------------------------------------
  Scenario: Valid blob with patched nonce is accepted
    When I request a block template from NODE with miner ID "submit_miner_s1"
    And I store the block template blob from the last response
    When I submit the stored blob with nonce 4294967295 through base node NODE xmrig proxy
    Then the JSON-RPC response status is OK

  # -----------------------------------------------------------------------
  # Scenario S2: Duplicate submission rejected
  # After a successful submitblock the template is removed from the cache via
  # take(), so a second submission of the same blob is rejected with error
  # code -1 and "already submitted" in the message.
  # -----------------------------------------------------------------------
  Scenario: Duplicate submission is rejected
    When I request a block template from NODE with miner ID "submit_miner_s2"
    And I store the block template blob from the last response
    When I submit the stored blob with nonce 0 through base node NODE xmrig proxy
    Then the JSON-RPC response status is OK

    When I submit a block with the stored blob through base node NODE xmrig proxy
    Then the JSON-RPC response error code is -1
    And the JSON-RPC response error message contains "already submitted"
