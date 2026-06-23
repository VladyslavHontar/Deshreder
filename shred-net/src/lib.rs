//! `shred-net` — the shared P2P "complete lane" for Colibri and Lumen.
//!
//! This crate owns the Phase-0-proven machinery for participating in Solana's
//! shred network as an unstaked node and reconstructing complete blocks:
//!
//!   - [`wire`]        — repair-socket codec: ping-pong responder + the modern
//!                       signed repair request encoders (the bytes that
//!                       unlocked the repair firehose in Phase 0).
//!   - `gossip`        — (next) join gossip, advertise ContactInfo, score peers.
//!   - `repair`        — (next) the repair driver: decide & dispatch
//!                       Window/HighestWindow/Orphan requests per slot.
//!   - `reconstruct`   — (next) agave `Blockstore`-backed slot reconstruction:
//!                       FEC + completed-data-sets + repair-until-`is_full`.
//!
//! The eventual public surface is a `ShredNet` that, given a config + a peer
//! source, drives repair until each targeted slot reaches `is_full()` and emits
//! the complete block. Both consumers wrap it: Colibri's complete lane and
//! Lumen's `P2pRepairSource`.

pub mod wire;
