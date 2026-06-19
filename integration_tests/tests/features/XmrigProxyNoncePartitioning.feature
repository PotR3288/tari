# Copyright 2026. The Tari Project
# SPDX-License-Identifier: BSD-3-Clause

@xmrig-proxy @base-node
Feature: XMRig Proxy Nonce Partitioning (Random Start)

  Background:
    Given I have a seed node NODE

  # -----------------------------------------------------------------------
  # Scenario 1: Solo miner receives a valid nonce range (random start..u64::MAX)
  # -----------------------------------------------------------------------
  Scenario: Solo miner gets random-start nonce range
    When I request a block template from NODE with miner ID "m1"
    And I store the nonce range for miner "m1"
    Then miner "m1" has a valid nonce range

  # -----------------------------------------------------------------------
  # Scenario 2: Multiple miners each get distinct random-start ranges
  #              (ranges overlap by design — collision probability is negligible)
  # -----------------------------------------------------------------------
  Scenario: Multiple miners get distinct random-start ranges
    When I request a block template from NODE with miner ID "m1"
    And I store the nonce range for miner "m1"
    When I request a block template from NODE with miner ID "m2"
    And I store the nonce range for miner "m2"
    Then the miners have distinct nonce starts

  # -----------------------------------------------------------------------
  # Scenario 3: Idempotency — same miner always gets the same start nonce
  #              regardless of how many other miners are present.
  # -----------------------------------------------------------------------
  Scenario: Same miner always returns the same range (idempotent)
    When I request a block template from NODE with miner ID "m1"
    And I store my own nonce range as "m1_first"
    When I request a block template from NODE with miner ID "m2"
    And I request a block template from NODE with miner ID "m3"
    When I request a block template from NODE with miner ID "m1"
    And I store my own nonce range as "m1_second"
    Then the stored nonce ranges are identical for m1_first and m1_second

  # -----------------------------------------------------------------------
  # Scenario 4: Nonce validation — any nonce >= start is in range
  # -----------------------------------------------------------------------
  Scenario: Nonce at u64::MAX is in owner's range
    When I request a block template from NODE with miner ID "m1"
    And I store the nonce range for miner "m1"

    # u64::MAX is always in range (start..u64::MAX)
    Then nonce 18446744073709551615 is in miner "m1"'s range
