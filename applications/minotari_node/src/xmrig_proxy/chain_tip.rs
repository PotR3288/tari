// Copyright 2025. The Tari Project
//
// Redistribution and use in source code and binary forms, with or without modification, are permitted provided that the
// following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
// disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
// following disclaimer in the documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote
// products derived from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
// USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

//! Chain tip tracking and stale template eviction for the XMRig proxy.
//!
//! Provides a single entry point to fetch the current chain tip from the node service
//! and detect whether Tari's state has advanced since the last check. When the chain
//! advances, cached block templates become stale because their `prev_hash` no longer
//! matches the new parent — miners hashing on them waste effort until rejection triggers
//! regeneration.

use log::debug;
use tari_core::base_node::LocalNodeCommsInterface;

use super::{
    block_template_storage::{BlockTemplateStorage, ChainTip},
    error::XmrigProxyError,
};

const LOG_TARGET: &str = "minotari::base_node::xmrig_proxy";

/// Fetch the current chain tip height and block hash from the node service.
pub async fn get_chain_tip(handler: &mut LocalNodeCommsInterface) -> Result<ChainTip, XmrigProxyError> {
    let meta = handler.get_metadata().await?;
    Ok(ChainTip {
        height: meta.best_block_height(),
        top_hash: *meta.best_block_hash(),
    })
}

/// Check whether the chain has advanced and evict stale templates if so.
///
/// Returns `true` if a tip advance was detected (templates were evicted), along with
/// the current chain tip height for use by callers who need it (e.g., computing the next
/// block height). This avoids a redundant metadata fetch — the caller can reuse the
/// height instead of calling `get_metadata()` again.
///
/// When Tari's chain tip advances, ALL cached templates become stale because they reference
/// the old `prev_hash`. We must evict all templates regardless of algorithm.
pub async fn check_chain_tip_advance(
    handler: &mut LocalNodeCommsInterface,
    block_templates: &BlockTemplateStorage,
) -> Result<(bool, u64), XmrigProxyError> {
    let current_tip = get_chain_tip(handler).await?;
    let advanced = block_templates.update_chain_tip(current_tip).await;
    if advanced {
        debug!(target: LOG_TARGET, "Chain tip advanced to height #{} (hash {}), evicting all cached templates", current_tip.height, current_tip.top_hash);
        block_templates.evict_all().await;
    }
    Ok((advanced, current_tip.height))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;
    use primitive_types::U512;
    use tari_common_types::{chain_metadata::ChainMetadata, tari_address::TariAddress, types::FixedHash};
    use tari_core::base_node::{
        LocalNodeCommsInterface,
        comms_interface::{BlockEvent, CommsInterfaceError, NodeCommsRequest, NodeCommsResponse},
    };
    use tari_node_components::blocks::BlockBuilder;
    use tari_service_framework::reply_channel;
    use tari_utilities::ByteArray;
    use tokio::sync::broadcast;

    use super::*;

    /// Build a ChainMetadata fixture at the given height and hash.
    fn make_metadata(height: u64, hash: [u8; 32]) -> ChainMetadata {
        ChainMetadata::new(
            height,
            FixedHash::new(hash),
            0, // pruning_horizon (archival)
            0, // pruned_height
            U512::from(1u64),
            0,
        )
        .expect("valid metadata")
    }

    /// Build a LocalNodeCommsInterface backed by reply channels. Returns the
    /// interface and the receiver side for dispatching canned responses.
    fn make_mock_comms() -> (
        LocalNodeCommsInterface,
        reply_channel::Receiver<NodeCommsRequest, Result<NodeCommsResponse, CommsInterfaceError>>,
    ) {
        let (req_tx, req_rx) =
            reply_channel::unbounded::<NodeCommsRequest, Result<NodeCommsResponse, CommsInterfaceError>>();
        let (block_tx, _block_rx) =
            reply_channel::unbounded::<tari_node_components::blocks::Block, Result<FixedHash, CommsInterfaceError>>();
        let block_event_tx: broadcast::Sender<Arc<BlockEvent>> = broadcast::channel(50).0;
        (LocalNodeCommsInterface::new(req_tx, block_tx, block_event_tx), req_rx)
    }

    // ---------------------------------------------------------------------------
    // get_chain_tip() tests
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn get_chain_tip_returns_correct_height_and_hash() {
        let (mut comms, mut req_rx) = make_mock_comms();

        let expected_height: u64 = 12345;
        let expected_hash: [u8; 32] = [0xABu8; 32];
        let metadata = make_metadata(expected_height, expected_hash);

        // Spawn a task to respond to the GetChainMetadata request.
        tokio::spawn(async move {
            if let Some(req_ctx) = req_rx.next().await {
                match req_ctx.request() {
                    NodeCommsRequest::GetChainMetadata => {
                        req_ctx
                            .reply(Ok(NodeCommsResponse::ChainMetadata(metadata.clone())))
                            .ok();
                    },
                    _ => {},
                }
            }
        });

        let tip = get_chain_tip(&mut comms).await.unwrap();
        assert_eq!(tip.height, expected_height);
        assert_eq!(*tip.top_hash.as_bytes(), expected_hash);
    }

    #[tokio::test]
    async fn get_chain_tip_propagates_comms_error() {
        let (mut comms, mut req_rx) = make_mock_comms();

        // Spawn a task that returns an error for GetChainMetadata.
        tokio::spawn(async move {
            if let Some(req_ctx) = req_rx.next().await {
                match req_ctx.request() {
                    NodeCommsRequest::GetChainMetadata => {
                        req_ctx.reply(Err(CommsInterfaceError::UnexpectedApiResponse)).ok();
                    },
                    _ => {},
                }
            }
        });

        let result = get_chain_tip(&mut comms).await;
        assert!(result.is_err());
    }

    // ---------------------------------------------------------------------------
    // check_chain_tip_advance() tests
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn check_chain_tip_advance_no_advance_returns_false() {
        let (mut comms, mut req_rx) = make_mock_comms();
        let storage = BlockTemplateStorage::new();

        let height: u64 = 100;
        let hash: [u8; 32] = [0x01u8; 32];
        let metadata = make_metadata(height, hash);

        // Single task handles all requests in a loop.
        tokio::spawn(async move {
            while let Some(req_ctx) = req_rx.next().await {
                if matches!(req_ctx.request(), NodeCommsRequest::GetChainMetadata) {
                    req_ctx
                        .reply(Ok(NodeCommsResponse::ChainMetadata(metadata.clone())))
                        .ok();
                }
            }
        });

        // First call: initializes the stored tip, returns false.
        let (advanced, current_height) = check_chain_tip_advance(&mut comms, &storage).await.unwrap();
        assert!(!advanced);
        assert_eq!(current_height, height);

        // Second call with same tip: no advance detected.
        let (advanced, _) = check_chain_tip_advance(&mut comms, &storage).await.unwrap();
        assert!(!advanced);
    }

    #[tokio::test]
    async fn check_chain_tip_advance_height_increase_returns_true() {
        let (mut comms, mut req_rx) = make_mock_comms();
        let storage = BlockTemplateStorage::new();

        // Store a template so eviction has something to evict.
        let key = [0x42u8; 32];
        storage
            .store(
                key,
                BlockBuilder::new(1).build(),
                TariAddress::default(),
                "miner".to_string(),
                1,
                [0x42u8; 32],
            )
            .await;

        let initial_metadata = make_metadata(100, [0x01u8; 32]);
        let advanced_metadata = make_metadata(101, [0x02u8; 32]);

        // Single task handles all requests in a loop.
        tokio::spawn(async move {
            while let Some(req_ctx) = req_rx.next().await {
                if matches!(req_ctx.request(), NodeCommsRequest::GetChainMetadata) {
                    // First request returns initial tip, second returns advanced tip.
                    static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
                    let count = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let meta = if count == 0 {
                        initial_metadata.clone()
                    } else {
                        advanced_metadata.clone()
                    };
                    req_ctx.reply(Ok(NodeCommsResponse::ChainMetadata(meta))).ok();
                }
            }
        });

        // Initialize the stored tip.
        check_chain_tip_advance(&mut comms, &storage).await.unwrap();

        // Advance to a new height.
        let (advanced, current_height) = check_chain_tip_advance(&mut comms, &storage).await.unwrap();
        assert!(advanced);
        assert_eq!(current_height, 101);

        // Verify templates were evicted.
        assert!(storage.get(&key).await.is_none());
    }

    #[tokio::test]
    async fn check_chain_tip_advance_reorg_same_height_returns_true() {
        let (mut comms, mut req_rx) = make_mock_comms();
        let storage = BlockTemplateStorage::new();

        // Store a template so eviction has something to evict.
        let key = [0x42u8; 32];
        storage
            .store(
                key,
                BlockBuilder::new(1).build(),
                TariAddress::default(),
                "miner".to_string(),
                1,
                [0x42u8; 32],
            )
            .await;

        let initial_metadata = make_metadata(100, [0x01u8; 32]);
        let reorg_metadata = make_metadata(100, [0xFFu8; 32]);

        // Single task handles all requests in a loop.
        tokio::spawn(async move {
            while let Some(req_ctx) = req_rx.next().await {
                if matches!(req_ctx.request(), NodeCommsRequest::GetChainMetadata) {
                    static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
                    let count = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let meta = if count == 0 {
                        initial_metadata.clone()
                    } else {
                        reorg_metadata.clone()
                    };
                    req_ctx.reply(Ok(NodeCommsResponse::ChainMetadata(meta))).ok();
                }
            }
        });

        // Initialize the stored tip.
        check_chain_tip_advance(&mut comms, &storage).await.unwrap();

        // Reorg: same height but different hash.
        let (advanced, current_height) = check_chain_tip_advance(&mut comms, &storage).await.unwrap();
        assert!(advanced);
        assert_eq!(current_height, 100); // same height

        // Verify templates were evicted despite no height change.
        assert!(storage.get(&key).await.is_none());
    }

    #[tokio::test]
    async fn check_chain_tip_advance_propagates_comms_error() {
        let (mut comms, mut req_rx) = make_mock_comms();
        let storage = BlockTemplateStorage::new();

        // Spawn a task that returns an error.
        tokio::spawn(async move {
            if let Some(req_ctx) = req_rx.next().await {
                match req_ctx.request() {
                    NodeCommsRequest::GetChainMetadata => {
                        req_ctx.reply(Err(CommsInterfaceError::UnexpectedApiResponse)).ok();
                    },
                    _ => {},
                }
            }
        });

        let result = check_chain_tip_advance(&mut comms, &storage).await;
        assert!(result.is_err());
    }
}
