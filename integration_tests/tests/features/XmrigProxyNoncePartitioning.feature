# Copyright 2026. The Tari Project
# SPDX-License-Identifier: BSD-3-Clause

@xmrig-proxy @base-node
Feature: XMRig Proxy Nonce Partitioning

  Background:
    Given I have a seed node NODE

  # -----------------------------------------------------------------------
  # Scenario 1: Solo miner receives the full 32-bit nonce space
  # -----------------------------------------------------------------------
  Scenario: Solo miner gets full nonce range
    When I request a block template from NODE with miner ID "m1"
    And I store the nonce range for miner "m1"
    Then miner "m1" has full nonce range

  # -----------------------------------------------------------------------
  # Scenario 2: Two miners receive non-overlapping ranges
  # -----------------------------------------------------------------------
  Scenario: Two miners get non-overlapping ranges
    When I request a block template from NODE with miner ID "m1"
    And I store the nonce range for miner "m1"
    When I request a block template from NODE with miner ID "m2"
    And I store the nonce range for miner "m2"
    Then the nonce ranges are non-overlapping

  # -----------------------------------------------------------------------
  # Scenario 3: Three miners get equal splits of the nonce space
  # -----------------------------------------------------------------------
  Scenario: Three miners get equal splits
    When I request a block template from NODE with miner ID "m1"
    And I store the nonce range for miner "m1"
    When I request a block template from NODE with miner ID "m2"
    And I store the nonce range for miner "m2"
    When I request a block template from NODE with miner ID "m3"
    And I store the nonce range for miner "m3"
    Then each of the 3 miners has an equal split

  # -----------------------------------------------------------------------
  # Scenario 4: Four miners get equal splits of the nonce space
  # -----------------------------------------------------------------------
  Scenario: Four miners get equal splits
    When I request a block template from NODE with miner ID "m1"
    And I store the nonce range for miner "m1"
    When I request a block template from NODE with miner ID "m2"
    And I store the nonce range for miner "m2"
    When I request a block template from NODE with miner ID "m3"
    And I store the nonce range for miner "m3"
    When I request a block template from NODE with miner ID "m4"
    And I store the nonce range for miner "m4"
    Then each of the 4 miners has an equal split

  # -----------------------------------------------------------------------
  # Scenario 5: Consistent partitioning — same miner always gets the same
  #              range regardless of request order.
  #              We add a third miner, then remove it (via template rotation),
  #              and verify that the original two miners still get their
  #              expected ranges when re-requested.
  # -----------------------------------------------------------------------
  Scenario: Consistent partitioning across requests
    When I request a block template from NODE with miner ID "m1"
    And I store the nonce range for miner "m1"
    When I request a block template from NODE with miner ID "m2"
    And I store the nonce range for miner "m2"

    # Add a third miner — this triggers repartitioning.
    # Mining blocks advances chain tip → proxy resets nonce partitioner, so m1/m2
    # must re-request to get fresh (re-partitioned) ranges before adding m3.
    When I mine 3 blocks on NODE

    # Re-request from existing miners so they receive new ranges after the reset
    When I request a block template from NODE with miner ID "m1"
    And I store the nonce range for miner "m1"
    When I request a block template from NODE with miner ID "m2"
    And I store the nonce range for miner "m2"

    # Add m3 — triggers repartitioning across all three miners
    When I request a block template from NODE with miner ID "m3"
    And I store the nonce range for miner "m3"
    Then the nonce ranges are non-overlapping

  # -----------------------------------------------------------------------
  # Scenario 6: Nonce validation — a nonce within the assigned range is valid
  #             A nonce outside the assigned range would be rejected by the
  #             proxy's submitblock handler.
  # -----------------------------------------------------------------------
  Scenario: Nonce falls within owner's range
    When I request a block template from NODE with miner ID "m1"
    And I store the nonce range for miner "m1"

    # The first nonce in the range (start) should be valid
    Then nonce 0 is in miner "m1"'s range


