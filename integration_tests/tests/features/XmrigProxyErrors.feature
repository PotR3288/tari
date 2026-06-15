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
