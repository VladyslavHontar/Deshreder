//! Pure Solana shred assembler: raw packets in, block components out.
//!
//! ```text
//! packet ──▶ Deshredder::push ──▶ Event::Entries      (each batch, as soon as present)
//!                              ──▶ Event::Footer       (bank hash + producer clock)
//!                              ──▶ Event::SlotComplete
//! ```
//!
//! It also reports what it lacks — `received`, `last_index`, `missing` — so
//! a repair driver can decide what to fetch. It never fetches anything itself:
//! peers, requests, timeouts and retries live with the caller. Assembles;
//! reports gaps; does not fill them.
//!
//! No I/O, no clock, no logging, no rocksdb: the library depends on
//! `solana-entry` for the `BlockComponent` codec and `reed-solomon-erasure`
//! for recovering lost data shreds from coding shreds. Signature checks are
//! the caller's job — feed only leader-verified packets.

mod assembler;
mod wire;

pub use assembler::{Assembler as Deshredder, Event};
pub use wire::merkle_root;
pub use solana_entry::entry::Entry;
pub use solana_hash::Hash;
