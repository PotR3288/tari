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

//! Mining blob construction and parsing for Tari RandomXT.
//!
//! The 76-byte mining blob layout:
//! ```text
//! | 3 bytes (zeros) | 32 bytes (mining_hash) | 8 bytes (nonce big-endian) | 33 bytes (pow_algo + zeros) |
//! ```
//!
//! - Bytes 0..3:   zero padding
//! - Bytes 3..35:  mining hash (32 bytes)
//! - Bytes 35..39: high nonce bytes (XMRig writes per-thread extra nonce here, at `reserved_offset`)
//! - Bytes 39..43: low nonce bytes (standard Monero nonce offset)
//! - Byte 43:      pow_algo byte value
//! - Bytes 44..76: zero padding

/// The byte offset in the 76-byte mining blob where the extra nonce starts.
/// XMRig writes a per-thread extra nonce here so mining threads don't duplicate work.
/// This corresponds to the high 4 bytes of the u64 nonce field.
pub const TARI_BLOB_RESERVED_OFFSET: u32 = 35;

/// Offset where the mining hash starts in the blob (after 3 zero padding bytes).
const TARI_HASH_OFFSET: usize = 3;

/// Size of the nonce field in bytes.
const TARI_NONCE_SIZE: usize = 8;

/// The total size of the Tari mining blob in bytes.
const TARI_MINING_BLOB_SIZE: usize = 76;

/// The pow_algo byte value for RandomXT (= 2).
pub const POW_ALGO_RANDOMXT: u8 = 2;

use log::warn;
use crate::xmrig_proxy::error::XmrigProxyError;

const LOG_TARGET: &str = "minotari::base_node::xmrig_proxy::blob";

/// Build a 76-byte XMRig-compatible mining blob for Tari RandomXT.
pub fn build_tari_mining_blob(mining_hash: &[u8], nonce: u64, pow_algo: u8) -> Vec<u8> {
    let mut blob = vec![0u8; 3];
    blob.extend_from_slice(mining_hash);
    blob.extend_from_slice(&nonce.to_be_bytes());
    blob.push(pow_algo);
    blob.extend_from_slice(&[0u8; 32]);
    blob
}

/// Parse the mining hash and nonce from a Tari mining blob.
pub fn parse_mining_blob(blob: &[u8]) -> Result<([u8; 32], u64), XmrigProxyError> {
    if blob.len() != TARI_MINING_BLOB_SIZE {
        warn!(
            target: LOG_TARGET,
            "Invalid blob length: {} (expected {TARI_MINING_BLOB_SIZE})",
            blob.len()
        );
        return Err(XmrigProxyError::InvalidRequest(format!(
            "blob length {} does not match expected {TARI_MINING_BLOB_SIZE}",
            blob.len()
        )));
    }

    let mining_hash_bytes: [u8; 32] = blob[TARI_HASH_OFFSET..TARI_BLOB_RESERVED_OFFSET as usize]
        .try_into()
        .map_err(|_| XmrigProxyError::InvalidRequest("bad mining hash slice".to_string()))?;

    let nonce_bytes: [u8; TARI_NONCE_SIZE] = blob
        [TARI_BLOB_RESERVED_OFFSET as usize..TARI_BLOB_RESERVED_OFFSET as usize + TARI_NONCE_SIZE]
        .try_into()
        .map_err(|_| XmrigProxyError::InvalidRequest("bad nonce slice".to_string()))?;

    let nonce = u64::from_be_bytes(nonce_bytes);

    let pow_algo_byte = blob[TARI_BLOB_RESERVED_OFFSET as usize + TARI_NONCE_SIZE];
    if pow_algo_byte != POW_ALGO_RANDOMXT {
        warn!(
            target: LOG_TARGET,
            "Invalid pow_algo byte: {pow_algo_byte} (expected {POW_ALGO_RANDOMXT} for RandomXT)"
        );
        return Err(XmrigProxyError::InvalidRequest(format!(
            "invalid pow_algo byte: {pow_algo_byte} (expected {POW_ALGO_RANDOMXT} for RandomXT)"
        )));
    }

    Ok((mining_hash_bytes, nonce))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Blob has correct length (76 bytes) and zero prefix.
    #[test]
    fn build_tari_mining_blob_has_correct_length_and_zero_prefix() {
        let hash = [0x42u8; 32];
        let blob = build_tari_mining_blob(&hash, 0, POW_ALGO_RANDOMXT);
        assert_eq!(blob.len(), TARI_MINING_BLOB_SIZE);
        assert_eq!(&blob[0..3], &[0, 0, 0]);
    }

    /// Full round-trip: build a blob and parse it back to verify hash + nonce.
    #[test]
    fn parse_mining_blob_roundtrip() {
        let hash = [0xABu8; 32];
        let nonce: u64 = 0xDEADBEEFCAFEBABE;
        let blob = build_tari_mining_blob(&hash, nonce, POW_ALGO_RANDOMXT);

        // Verify the pow_algo byte is set correctly.
        assert_eq!(blob[43], POW_ALGO_RANDOMXT);

        let (parsed_hash, parsed_nonce) = parse_mining_blob(&blob).unwrap();
        assert_eq!(parsed_hash, hash);
        assert_eq!(parsed_nonce, nonce);
    }

    /// Parse rejects blobs with wrong length.
    #[test]
    fn parse_mining_blob_rejects_wrong_length() {
        let blob = vec![0u8; 75]; // too short
        assert!(parse_mining_blob(&blob).is_err());

        let blob = vec![0u8; 77]; // too long
        assert!(parse_mining_blob(&blob).is_err());
    }

    /// Parse rejects blobs with invalid pow_algo byte.
    #[test]
    fn parse_mining_blob_rejects_invalid_pow_algo() {
        let hash = [0x42u8; 32];
        let mut blob = build_tari_mining_blob(&hash, 0, POW_ALGO_RANDOMXT);

        // Corrupt the pow_algo byte
        blob[43] = 99; // Invalid algorithm

        assert!(parse_mining_blob(&blob).is_err());
    }
}
