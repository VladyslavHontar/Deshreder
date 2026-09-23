//! Wire layout of agave v4.3.0 Merkle shreds — the only thing this crate
//! knows about the outside world.
//!
//! Every offset and size here is taken from `ledger/src/shred.rs` and
//! `ledger/src/shred/merkle.rs` at tag v4.3.0, not remembered:
//!
//! ```text
//! packet[0..64)    signature
//! packet[64]       variant: high nibble 0x6/0x7 = code, 0x9/0xb = data;
//!                  bit 0x10 of the nibble = resigned; low nibble = proof_size
//! packet[65..73)   slot            u64 LE
//! packet[73..77)   index           u32 LE
//! packet[77..79)   version         u16 LE
//! packet[79..83)   fec_set_index   u32 LE        ← end of the common header
//! data:  [83..85) parent_offset  [85] flags  [86..88) size u16 LE
//!        `size` is ABSOLUTE: common header + data header + data, so the
//!        entry bytes are packet[88..size]  (merkle.rs `get_data`)
//! code:  [83..85) num_data  [85..87) num_code  [87..89) position, all u16 LE
//! ```
//!
//! Erasure coding covers a `shard` of identical length in every shred of a
//! FEC set (`merkle.rs` `erasure_shard_offsets` / `capacity`):
//!
//! ```text
//! shard_len          = 1107 − 20·proof_size − 64·resigned
//! data shard         = packet[64 .. 64 + shard_len]   (headers included)
//! code shard         = packet[89 .. 89 + shard_len]
//! ```
//!
//! A recovered data shard therefore carries its own common + data headers at
//! shard-relative offsets (packet offset − 64), which is what lets a shard
//! that Reed–Solomon just produced be validated with the same parser as a
//! packet that came off the wire.

pub const DATA_PAYLOAD: usize = 1203; // ShredData::SIZE_OF_PAYLOAD
pub const CODE_PAYLOAD: usize = 1228; // ShredCode::SIZE_OF_PAYLOAD
const SIGNATURE: usize = 64;
pub const DATA_HEADERS: usize = 88; // signature + common (19) + data header (5)
const CODE_HEADERS: usize = 89; // signature + common (19) + coding header (6)
const MERKLE_ROOT: usize = 32;
const PROOF_ENTRY: usize = 20;

const FLAG_DATA_COMPLETE: u8 = 0b0100_0000;
const FLAG_LAST_IN_SLOT: u8 = 0b1100_0000; // both bits set

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Data { flags: u8, size: u16 },
    Code { num_data: u16, num_code: u16, position: u16 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub slot: u64,
    pub index: u32,
    pub fec_set_index: u32,
    pub proof_size: u8,
    pub resigned: bool,
    pub kind: Kind,
}

impl Header {
    #[inline]
    pub fn is_data(&self) -> bool {
        matches!(self.kind, Kind::Data { .. })
    }
    #[inline]
    pub fn data_complete(&self) -> bool {
        matches!(self.kind, Kind::Data { flags, .. } if flags & FLAG_DATA_COMPLETE != 0)
    }
    #[inline]
    pub fn last_in_slot(&self) -> bool {
        matches!(self.kind, Kind::Data { flags, .. } if flags & FLAG_LAST_IN_SLOT == FLAG_LAST_IN_SLOT)
    }
    /// Length of the erasure-coded shard for this shred's variant.
    #[inline]
    pub fn shard_len(&self) -> usize {
        shard_len(self.proof_size, self.resigned)
    }
}

#[inline]
pub fn shard_len(proof_size: u8, resigned: bool) -> usize {
    CODE_PAYLOAD - CODE_HEADERS - MERKLE_ROOT
        - usize::from(proof_size) * PROOF_ENTRY
        - if resigned { SIGNATURE } else { 0 }
}

#[inline]
fn u16_at(b: &[u8], i: usize) -> u16 {
    u16::from_le_bytes([b[i], b[i + 1]])
}
#[inline]
fn u32_at(b: &[u8], i: usize) -> u32 {
    u32::from_le_bytes([b[i], b[i + 1], b[i + 2], b[i + 3]])
}

/// Parse the headers starting at `base` (64 for a packet, 0 for a shard).
/// Returns `None` for legacy variants, truncated input and any size or
/// position that agave's own `sanitize` would reject.
fn parse_at(b: &[u8], base: usize) -> Option<Header> {
    let variant = *b.get(base)?;
    let proof_size = variant & 0x0F;
    let (is_data, resigned) = match variant & 0xF0 {
        0x60 => (false, false),
        0x70 => (false, true),
        0x90 => (true, false),
        0xb0 => (true, true),
        _ => return None, // legacy (0xa5 / 0x5a) or garbage
    };
    let shard_len = shard_len(proof_size, resigned);
    if shard_len < DATA_HEADERS - SIGNATURE {
        return None; // proof_size too large for the payload
    }
    let common_end = base + 19;
    let kind = if is_data {
        b.get(common_end + 4)?; // need [83..88) relative to the packet
        let flags = b[common_end + 2];
        let size = u16_at(b, common_end + 3);
        // Absolute end of the data, in packet coordinates. Valid iff it lies
        // within [headers, headers + capacity]; capacity = shard_len − 24.
        let s = usize::from(size);
        let max = DATA_HEADERS + shard_len - (DATA_HEADERS - SIGNATURE);
        if s < DATA_HEADERS || s > max {
            return None;
        }
        Kind::Data { flags, size }
    } else {
        b.get(common_end + 5)?; // need [83..89)
        let num_data = u16_at(b, common_end);
        let num_code = u16_at(b, common_end + 2);
        let position = u16_at(b, common_end + 4);
        if num_data == 0 || num_code == 0 || position >= num_code {
            return None;
        }
        Kind::Code { num_data, num_code, position }
    };
    Some(Header {
        slot: u64::from_le_bytes(b[base + 1..base + 9].try_into().unwrap()),
        index: u32_at(b, base + 9),
        fec_set_index: u32_at(b, base + 15),
        proof_size,
        resigned,
        kind,
    })
}

/// Parse a packet as it arrives from turbine or repair. Repair appends a
/// nonce, so the packet may be longer than the payload; it is never shorter.
pub fn parse_packet(packet: &[u8]) -> Option<Header> {
    let h = parse_at(packet, SIGNATURE)?;
    let need = if h.is_data() { DATA_PAYLOAD } else { CODE_PAYLOAD };
    (packet.len() >= need).then_some(h)
}

/// Parse a data shard (headers at offset 0), e.g. one Reed–Solomon produced.
pub fn parse_data_shard(shard: &[u8]) -> Option<Header> {
    let h = parse_at(shard, 0)?;
    (h.is_data() && shard.len() == h.shard_len()).then_some(h)
}

/// The erasure-coded slice of a packet.
#[inline]
pub fn shard<'a>(packet: &'a [u8], h: &Header) -> &'a [u8] {
    let start = if h.is_data() { SIGNATURE } else { CODE_HEADERS };
    &packet[start..start + h.shard_len()]
}

/// The ledger bytes inside a data shard: `shard[24 .. size − 64]`.
#[inline]
pub fn data_in_shard<'a>(shard: &'a [u8], h: &Header) -> &'a [u8] {
    let Kind::Data { size, .. } = h.kind else { return &[] };
    &shard[DATA_HEADERS - SIGNATURE..usize::from(size) - SIGNATURE]
}
