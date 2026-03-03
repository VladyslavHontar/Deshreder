# Deshredder

Pure Solana shred assembler library. Takes raw UDP shred packets, reassembles them into complete entries and transactions using FEC recovery. Zero I/O, zero async — just feed bytes in, get entries out.

Used by [Colibri](https://github.com/VladyslavHontar/Colibri) as the shred assembly engine.

## How it works

1. Raw shred bytes come in via `push_raw()`
2. Shreds are deduplicated and grouped by slot
3. When enough shreds arrive for a slot, Reed-Solomon FEC recovery reconstructs any missing pieces
4. Complete entries (containing transactions) are returned to the caller

## API

```rust
use deshredder::{Deshredder, SlotEntries};

let mut deshredder = Deshredder::new(
    3000,  // evict incomplete slots after 3s
    200,   // dedup history: 200 slots
);

// Feed a raw UDP shred packet
if let Some(slot_entries) = deshredder.push_raw(&udp_bytes) {
    println!("slot {} — {} entries", slot_entries.slot, slot_entries.entries.len());

    for entry in &slot_entries.entries {
        for tx in &entry.transactions {
            println!("  tx: {}", tx.signatures[0]);
        }
    }
}

// Call periodically to free memory from incomplete slots
deshredder.evict_expired();
```

### `SlotEntries`

```rust
pub struct SlotEntries {
    pub slot: u64,                          // Solana slot number
    pub entries: Vec<solana_entry::Entry>,   // decoded entries with transactions
    pub entries_bytes: Vec<u8>,             // bincode-serialized entries (ready for gRPC)
}
```

### Methods

| Method | Description |
|--------|-------------|
| `new(slot_timeout_ms, max_tracked_slots)` | Create a new assembler |
| `push_raw(&mut self, bytes: &[u8]) -> Option<SlotEntries>` | Feed one raw shred packet, returns assembled entries when a slot completes |
| `evict_expired(&mut self) -> usize` | Remove stale incomplete slots, returns count evicted |
| `active_slot_count(&self) -> usize` | Number of slots currently buffering shreds |
| `tracked_slot_count(&self) -> usize` | Number of slots in dedup history |

## Build

```bash
cargo build -p deshredder
```

## gRPC subscriber example

A working example that subscribes to a [Colibri](https://github.com/VladyslavHontar/Colibri) gRPC transaction stream:

```bash
cargo run --example subscribe -- --url http://127.0.0.1:8888
```
