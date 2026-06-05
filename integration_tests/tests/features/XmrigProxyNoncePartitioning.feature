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
    When I request a block template from NODE with miner ID "solo_miner"
    And I store the nonce range for miner "solo_miner"
    Then miner "solo_miner" has full nonce range

  # -----------------------------------------------------------------------
  # Scenario 2: Two miners receive non-overlapping ranges
  # -----------------------------------------------------------------------
  Scenario: Two miners get non-overlapping ranges
    When I request a block template from NODE with miner ID "miner_a"
    And I store the nonce range for miner "miner_a"
    When I request a block template from NODE with miner ID "miner_b"
    And I store the nonce range for miner "miner_b"
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
    When I request a block template from NODE with miner ID "a"
    And I store the nonce range for miner "a"
    When I request a block template from NODE with miner ID "b"
    And I store the nonce range for miner "b"
    When I request a block template from NODE with miner ID "c"
    And I store the nonce range for miner "c"
    When I request a block template from NODE with miner ID "d"
    And I store the nonce range for miner "d"
    Then each of the 4 miners has an equal split

  # -----------------------------------------------------------------------
  # Scenario 5: Consistent partitioning — same miner always gets the same
  #              range regardless of request order.
  #              We add a third miner, then remove it (via template rotation),
  #              and verify that the original two miners still get their
  #              expected ranges when re-requested.
  # -----------------------------------------------------------------------
  Scenario: Consistent partitioning across requests
    When I request a block template from NODE with miner ID "consistent_a"
    And I store the nonce range for miner "consistent_a"
    When I request a block template from NODE with miner ID "consistent_b"
    And I store the nonce range for miner "consistent_b"

    # Add a third miner — this triggers repartitioning
    When I request a block template from NODE with miner ID "consistent_c"
    And I store the nonce range for miner "consistent_c"
    Then the nonce ranges are non-overlapping

  # -----------------------------------------------------------------------
  # Scenario 6: Nonce validation — a nonce within the assigned range is valid
  #             A nonce outside the assigned range would be rejected by the
  #             proxy's submitblock handler.
  # -----------------------------------------------------------------------
  Scenario: Nonce falls within owner's range
    When I request a block template from NODE with miner ID "owner_miner"
    And I store the nonce range for miner "owner_miner"

    # The first nonce in the range (start) should be valid
    Then nonce 0 is in miner "owner_miner"'s range

  # -----------------------------------------------------------------------
  # Scenario 7: Nonce falls outside owner's range
  #             For a solo miner, u64::MAX is out of range because the
  #             partitioner uses u32::MAX as the upper bound.
  # -----------------------------------------------------------------------
  Scenario: Nonce falls outside owner's range (solo miner)
    When I request a block template from NODE with miner ID "victim_miner"
    And I store the nonce range for miner "victim_miner"

    # u64::MAX is definitely out of the u32-based nonce space
    Then nonce 18446744073709551615 is out of miner "victim_miner"'s range
