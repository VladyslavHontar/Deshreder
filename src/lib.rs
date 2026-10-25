//! Pure Solana shred assembler: raw packets in, block components out.
//!
//! ```text
//! packet ──▶ Deshredder::push ──▶ Event::Entries      (whole entries as soon as the shreds
//!                                                      from their batch's start are in;
//!                                                      the rest when the batch is whole)
//!                              ──▶ Event::Transactions (those of an entry not yet whole, one
//!                                                      by one as their bytes come)
//!                              ──▶ Event::BatchEnd     (a batch went out whole)
//!                              ──▶ Event::Footer       (bank hash + producer clock)
//!                              ──▶ Event::SlotComplete
//! ```
//!
//! It also reports what it lacks — `received`, `last_index`, `missing`, and
//! `needed`, only as many of a FEC set's missing shreds as its recovery still
//! lacks — so a repair driver can decide what to fetch. A FEC set is one
//! Merkle root and the sets of a block tile it: a slot whose shreds
//! contradict that holds two versions of its block, and is given up whole
//! (`Event::Equivocated`). It never fetches anything itself:
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
pub use solana_transaction::versioned::VersionedTransaction;
