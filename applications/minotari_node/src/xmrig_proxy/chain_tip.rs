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
pub async fn check_chain_tip_advance(
    handler: &mut LocalNodeCommsInterface,
    block_templates: &BlockTemplateStorage,
) -> Result<(bool, u64), XmrigProxyError> {
    let current_tip = get_chain_tip(handler).await?;
    let advanced = block_templates.update_chain_tip(current_tip).await;
    if advanced {
        debug!(target: LOG_TARGET, "Chain tip advanced to height #{} (hash {}), evicting RandomXT templates", current_tip.height, current_tip.top_hash);
        block_templates
            .evict_for_algorithm(tari_transaction_components::tari_proof_of_work::PowAlgorithm::RandomXT)
            .await;
    }
    Ok((advanced, current_tip.height))
}
