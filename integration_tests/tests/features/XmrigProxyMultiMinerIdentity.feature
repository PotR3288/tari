# Copyright 2022 The Tari Project
# SPDX-License-Identifier: BSD-3-Clause

@xmrig-proxy @base-node @slow
Feature: XMRig Proxy Multi-Miner Identity Resolution

  Background:
    Given I have a seed node NODE

  # -----------------------------------------------------------------------
  # Scenario E1: Two connections with same wallet_address but different
  #              extra_nonce → two distinct miners with separate nonce ranges.
  # This is the core multi-miner feature — XMRig instances sharing one payout
  # address should each get their own nonce search space. With random-start
  # nonces, collision probability is negligible (~10^-7 per block) so duplicate
  # work is effectively zero. The proxy resolves miner identity from extra_nonce
  # (Layer 1) before wallet_address, so different extra_nonces produce distinct
  # identities with independent starting nonces.
  # -----------------------------------------------------------------------
  Scenario: Same wallet address with different extra_nonces gets separate ranges
    # Connection 1: same wallet, extra_nonce "e1_miner_a"
    When I request a block template from NODE with miner ID "e1_miner_a" using wallet address "tnt1Qp2AnySgEeQ0Cm7Dvvoqo2nQGJ8ac3nglWwMD9FSN6FzSTi2fUuMxX3h4Gy" and extra_nonce "e1_miner_a"
    And I store the nonce range for miner "e1_miner_a"

    # Connection 2: same wallet, different extra_nonce "e1_miner_b"
    When I request a block template from NODE with miner ID "e1_miner_b" using wallet address "tnt1Qp2AnySgEeQ0Cm7Dvvoqo2nQGJ8ac3nglWwMD9FSN6FzSTi2fUuMxX3h4Gy" and extra_nonce "e1_miner_b"
    And I store the nonce range for miner "e1_miner_b"

    # Verify they got distinct starting nonces (ranges overlap by design, but starts differ)
    Then the miners have distinct nonce starts

  # -----------------------------------------------------------------------
  # Scenario E2: Same miner identity requests template twice → cached template,
  #              not re-partitioned. The nonce range should be identical on both
  #              calls — same start and end values. This verifies idempotency of
  #              the partitioner: a known miner's allocation is stable across
  #              repeated getblocktemplate requests (no double-counting).
  # -----------------------------------------------------------------------
  Scenario: Same identity twice returns cached template with unchanged nonce range
    When I request a block template from NODE with miner ID "e2_repeat_miner" using wallet address "tnt1Qp2AnySgEeQ0Cm7Dvvoqo2nQGJ8ac3nglWwMD9FSN6FzSTi2fUuMxX3h4Gy" and extra_nonce "e2_repeat_miner"
    And I store my own nonce range as "e2_first_call"

    # Second request from same identity — should hit cache, not re-partition
    When I request a block template from NODE with miner ID "e2_repeat_miner" using wallet address "tnt1Qp2AnySgEeQ0Cm7Dvvoqo2nQGJ8ac3nglWwMD9FSN6FzSTi2fUuMxX3h4Gy" and extra_nonce "e2_repeat_miner"
    And I store my own nonce range as "e2_second_call"

    # Verify both calls got identical ranges (idempotent partitioning)
    Then the stored nonce ranges are identical for e2_first_call and e2_second_call
