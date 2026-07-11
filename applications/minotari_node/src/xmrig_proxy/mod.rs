// Copyright 2025. The Tari Project
//
// Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
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

mod blob;
mod block_template_storage;
mod chain_tip;
mod error;
mod inner;
mod json_rpc;
mod miner_registry;
mod request_parser;
mod service;
mod status_handlers;
mod submit_block;
mod template_builder;

/// Miner identity key — the `extra_nonce` hex string sent by XMRig.
pub(crate) type MinerId = String;

use std::time::Duration;

use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use log::{error, info, trace};
use tari_common::configuration::Network;
use tari_common_types::tari_address::TariAddress;
use tari_comms::{multiaddr::Multiaddr, utils::multiaddr::multiaddr_to_socketaddr};
use tari_core::{
    base_node::{LocalNodeCommsInterface, StateMachineHandle},
    consensus::BaseNodeConsensusManager,
};
use tari_shutdown::ShutdownSignal;
use tari_transaction_components::transaction_components::RangeProofType;
use tokio::net::TcpListener;

use self::{
    block_template_storage::BlockTemplateStorage,
    inner::InnerService,
    miner_registry::{MinerRegistry, MinerRegistryConfig},
    service::XmrigProxyService,
};

const LOG_TARGET: &str = "minotari::base_node::xmrig_proxy";
const CLEANUP_INTERVAL_SECS: u64 = 10 * 60;

/// Start the XMRig-compatible JSON-RPC proxy server embedded in the base node.
///
/// The proxy accepts connections from XMRig (configured with `"coin": "tari"`, `"daemon": true`)
/// and uses `LocalNodeCommsInterface` directly to fetch block templates and submit mined blocks,
/// bypassing the gRPC server entirely.
///
/// # Arguments
/// * `node_service` - direct handle to the base node's comms interface
/// * `consensus_rules` - consensus manager for block template construction
/// * `state_machine` - state machine handle (available for future sync-check use)
/// * `listener_address` - the address on which this proxy listens for XMRig connections
/// * `wallet_payment_address` - where mining rewards are sent
/// * `network` - the node's network, used to validate miner-provided payment addresses
/// * `coinbase_extra` - optional extra data in the coinbase
/// * `range_proof_type` - range proof type for coinbase outputs
/// * `max_miners` - maximum concurrent miners allowed in the registry
/// * `miner_timeout_secs` - seconds of inactivity before a miner is evicted
/// * `shutdown` - shutdown signal from the base node
pub async fn run_xmrig_proxy(
    node_service: LocalNodeCommsInterface,
    consensus_rules: BaseNodeConsensusManager,
    state_machine: StateMachineHandle,
    listener_address: Multiaddr,
    wallet_payment_address: TariAddress,
    network: Network,
    coinbase_extra: Vec<u8>,
    range_proof_type: RangeProofType,
    max_miners: usize,
    miner_timeout_secs: u64,
    shutdown: ShutdownSignal,
) -> Result<(), anyhow::Error> {
    let listen_addr = multiaddr_to_socketaddr(&listener_address)?;
    let block_templates = BlockTemplateStorage::new();

    // Create shared miner registry before spawning cleanup tasks
    let miner_registry = MinerRegistry::new(MinerRegistryConfig {
        max_miners,
        miner_timeout_secs,
    });

    // Periodic cleanup of stale miners
    let cleanup_registry = miner_registry.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(CLEANUP_INTERVAL_SECS));
        loop {
            interval.tick().await;
            // Evict stale miners from the registry.
            cleanup_registry.evict_stale().await;
        }
    });

    let service = XmrigProxyService::new(InnerService {
        node_service,
        consensus_rules,
        state_machine,
        block_templates,
        miner_registry,
        wallet_payment_address,
        network,
        coinbase_extra,
        range_proof_type,
        // Placeholder — overwritten per-connection with the real remote address
        peer_addr: "0.0.0.0:0".parse().unwrap(),
    });

    match TcpListener::bind(listen_addr).await {
        Ok(listener) => {
            info!(target: LOG_TARGET, "XMRig proxy listening on {listen_addr}");
            println!(
                "XMRig proxy listening on {listen_addr}. Configure XMRig with: \"coin\": \"tari\", \"url\": \
                 \"{listen_addr}\", \"daemon\": true"
            );

            let mut shutdown = shutdown;
            loop {
                tokio::select! {
                    _ = &mut shutdown => {
                        info!(target: LOG_TARGET, "Shutdown signal received, stopping XMRig proxy");
                        break;
                    }
                    result = listener.accept() => {
                        match result {
                            Ok((tcp, addr)) => {
                                trace!(target: LOG_TARGET, "XMRig proxy: new connection from {addr}");
                                // Clone the inner service and set peer_addr to the real remote address
                                let mut inner = service.inner.clone();
                                inner.peer_addr = addr;
                                let svc = XmrigProxyService::new(inner);
                                let io = TokioIo::new(tcp);
                                tokio::task::spawn(async move {
                                    if let Err(e) = http1::Builder::new().serve_connection(io, &svc).await {
                                        error!(target: LOG_TARGET, "XMRig proxy connection error: {e}");
                                    }
                                });
                            },
                            Err(e) => {
                                error!(target: LOG_TARGET, "XMRig proxy accept error: {e}");
                            },
                        }
                    }
                }
            }
            Ok(())
        },
        Err(e) => {
            error!(
                target: LOG_TARGET,
                "Cannot bind XMRig proxy to '{listen_addr}': {e}. XMRig solo mining will not be available."
            );
            Err(e.into())
        },
    }
}
