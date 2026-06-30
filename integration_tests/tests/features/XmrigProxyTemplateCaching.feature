# Copyright 2022 The Tari Project
# SPDX-License-Identifier: BSD-3-Clause

@xmrig-proxy @base-node @slow
Feature: XMRig Proxy Template Caching & Chain Tip Tracking

  Background:
    Given I have a seed node NODE

  # -----------------------------------------------------------------------
  # Scenario B1: Cached template returned on repeat request from same miner
  # The first getblocktemplate call generates a new template. A second call
  # from the same miner should hit the cache and return the same height/prev_hash
  # without generating a fresh block.
  # -----------------------------------------------------------------------
  Scenario: Repeat request returns cached template with same height
    When I request a block template from NODE with miner ID "cache_miner_b1"
    And I store the response height as "first_height"

    When I request a block template from NODE with miner ID "cache_miner_b1"
    Then the response height matches stored value "first_height"

  # -----------------------------------------------------------------------
  # Scenario B2: Chain tip advance evicts stale templates
  # Mining a new RandomXT block advances the chain tip. The proxy detects this via
  # update_chain_tip() + is_advanced_by(), which triggers evict_for_algorithm(RandomXT).
  # A subsequent getblocktemplate should return a different height/prev_hash.
  # -----------------------------------------------------------------------
  Scenario: Chain tip advance invalidates cached template
    When I request a block template from NODE with miner ID "evict_miner_b2"
    And I store the response prev_hash as "original_prev_hash"

    When I mine 3 RandomXT blocks on NODE
    When I request a block template from NODE with miner ID "evict_miner_b2"
    Then the stored value "original_prev_hash" is different from current prev_hash

  # -----------------------------------------------------------------------
  # Scenario B3: New template generated after chain tip advance
  # After eviction, a fresh getblocktemplate call should produce a new template
  # with an updated height (one greater than before).
  # -----------------------------------------------------------------------
  Scenario: Fresh template has updated height after chain tip advance
    When I request a block template from NODE with miner ID "fresh_miner_b3"
    And I store the response height as "height_before_tip"

    When I mine 3 RandomXT blocks on NODE
    And I request a block template from NODE with miner ID "fresh_miner_b3"
    Then the response height is greater than stored value "height_before_tip"
