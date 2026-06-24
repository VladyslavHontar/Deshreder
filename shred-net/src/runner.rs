//! `ShredNet` — the runnable wiring that turns the crate's pieces into a live
//! complete-lane: join gossip, receive turbine shreds, drive repair until each
//! targeted slot is `is_full`, and emit the reconstructed block to a sink.
//!
//! Threading (all `std::thread`, no imposed async runtime — both Colibri and
//! Lumen embed this):
//!   - gossip threads (owned by `GossipService`)
//!   - TVU thread     — recv turbine shreds → Blockstore + fast-lane sink hook
//!   - repair thread  — ping-pong responder + per-slot repair driver + emit
//!
//! Acceptance is a live mainnet run (like Phase 0), not unit tests: the socket
//! I/O and peer dynamics can only be exercised against the real cluster.

use {
    crate::{
        gossip::{self, GossipConfig},
        reconstruct::Reconstructor,
        repair::{next_repair_action, RepairAction},
        wire,
    },
    solana_entry::entry::Entry,
    solana_keypair::Keypair,
    std::{
        collections::{HashMap, VecDeque},
        net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc, Mutex,
        },
        thread::{sleep, JoinHandle},
        time::{Duration, Instant},
    },
};

/// Receives reconstructed blocks (and, optionally, the raw fast-lane shred feed).
pub trait BlockSink: Send + Sync + 'static {
    /// A targeted slot reached `is_full()`; `entries` is the complete block.
    fn on_complete_block(&self, slot: u64, entries: Vec<Entry>);
    /// Every raw shred as it arrives (turbine + repair). Default: ignore.
    /// Colibri's fast lane uses this; Lumen's complete lane can skip it.
    fn on_raw_shred(&self, _bytes: &[u8]) {}
}

/// Configuration for a `ShredNet` instance.
pub struct ShredNetConfig {
    pub advertise_ip: IpAddr,
    pub gossip_port:  u16,
    pub tvu_port:     u16,
    pub repair_port:  u16,
    pub shred_version: Option<u16>,
    pub entrypoints:  Vec<String>,
    /// Purge slots below `tip - keep_window` to bound the ephemeral store.
    pub keep_window:  u64,
    /// How many slots to repair in parallel. Throughput is response-bound, so a
    /// wider window only helps if there's enough peer bandwidth to fill it.
    pub target_window: usize,
    /// How many distinct peers to spread repair requests across. This is the
    /// primary throughput lever: each peer rate-limits us independently, so the
    /// aggregate shred-serve rate scales ~linearly with peer count.
    pub top_peers: usize,
}

impl Default for ShredNetConfig {
    fn default() -> Self {
        Self {
            advertise_ip: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            gossip_port: 8000,
            tvu_port: 8200,
            repair_port: 8210,
            shred_version: None,
            entrypoints: Vec::new(),
            keep_window: 8_000,
            target_window: 64,
            top_peers: 64,
        }
    }
}

/// Per-targeted-slot driver bookkeeping (the have/last_index/is_full state lives
/// in the Blockstore via `Reconstructor`; only repair *policy* state is here).
struct DriverState {
    last_repair:    Instant,
    repair_rounds:  u32,
    highest_probed: bool,
    deadline:       Instant,
}

impl DriverState {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            last_repair: now - Duration::from_secs(1),
            repair_rounds: 0,
            highest_probed: false,
            deadline: now + Duration::from_secs(30),
        }
    }
}

/// A live shred-network participant + repair-driven reconstructor.
pub struct ShredNet {
    exit:         Arc<AtomicBool>,
    targets:      Arc<Mutex<VecDeque<u64>>>,
    stakes:       Arc<Mutex<HashMap<[u8; 32], u64>>>,
    observed_tip: Arc<AtomicU64>,
    handles:      Vec<JoinHandle<()>>,
    // Kept alive so gossip threads keep running; never read directly.
    _gossip:      solana_gossip::gossip_service::GossipService,
}

impl ShredNet {
    /// Join gossip and start the TVU + repair threads. Returns immediately; the
    /// node runs in the background until [`ShredNet::shutdown`].
    pub fn start(
        cfg: ShredNetConfig,
        keypair: Arc<Keypair>,
        sink: Arc<dyn BlockSink>,
    ) -> anyhow::Result<Self> {
        let exit = Arc::new(AtomicBool::new(false));
        let node = gossip::join(
            &GossipConfig {
                advertise_ip: cfg.advertise_ip,
                gossip_port: cfg.gossip_port,
                tvu_port: cfg.tvu_port,
                shred_version: cfg.shred_version,
                entrypoints: cfg.entrypoints.clone(),
            },
            keypair.clone(),
            exit.clone(),
        )?;

        let reconstructor = Arc::new(Reconstructor::new()?);
        let targets: Arc<Mutex<VecDeque<u64>>> = Arc::new(Mutex::new(VecDeque::new()));
        let stakes: Arc<Mutex<HashMap<[u8; 32], u64>>> = Arc::new(Mutex::new(HashMap::new()));
        let observed_tip = Arc::new(AtomicU64::new(0));

        let mut handles = Vec::new();
        handles.push(spawn_tvu(
            cfg.tvu_port,
            exit.clone(),
            reconstructor.clone(),
            sink.clone(),
            observed_tip.clone(),
        )?);
        handles.push(spawn_repair(
            &cfg,
            keypair,
            exit.clone(),
            node.cluster_info.clone(),
            reconstructor,
            sink,
            targets.clone(),
            stakes.clone(),
            observed_tip.clone(),
        )?);

        Ok(Self {
            exit,
            targets,
            stakes,
            observed_tip,
            handles,
            _gossip: node.gossip_service,
        })
    }

    /// Enqueue slots to repair-until-complete. Idempotent; duplicates are
    /// coalesced by the driver.
    pub fn request_slots(&self, slots: impl IntoIterator<Item = u64>) {
        let mut q = self.targets.lock().unwrap_or_else(|e| e.into_inner());
        q.extend(slots);
    }

    /// Update the stake table used to weight repair-peer selection (node pubkey
    /// → activated stake). Lumen feeds this from LMDB; empty ⇒ recency-only.
    pub fn set_stakes(&self, map: HashMap<[u8; 32], u64>) {
        *self.stakes.lock().unwrap_or_else(|e| e.into_inner()) = map;
    }

    /// Highest slot observed on the TVU socket so far (0 until the first shred).
    pub fn observed_tip(&self) -> u64 {
        self.observed_tip.load(Ordering::Relaxed)
    }

    /// Signal shutdown and join the worker threads.
    pub fn shutdown(self) {
        self.exit.store(true, Ordering::SeqCst);
        for h in self.handles {
            let _ = h.join();
        }
    }
}

/// Extract a shred's slot from raw bytes (LE u64 at offset 65), gated on the
/// shred-variant byte at offset 64 — same recognizer as `wire::parse_inbound`.
fn slot_of(buf: &[u8]) -> Option<u64> {
    if buf.len() < 88 {
        return None;
    }
    let variant = buf[64];
    let is_shred = matches!(variant & 0xF0, 0x80 | 0x90 | 0xB0) || variant == 0xA5;
    if !is_shred {
        return None;
    }
    Some(u64::from_le_bytes(buf[65..73].try_into().ok()?))
}

fn spawn_tvu(
    tvu_port: u16,
    exit: Arc<AtomicBool>,
    reconstructor: Arc<Reconstructor>,
    sink: Arc<dyn BlockSink>,
    observed_tip: Arc<AtomicU64>,
) -> anyhow::Result<JoinHandle<()>> {
    let bind_ip = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
    let tvu_socket = UdpSocket::bind(SocketAddr::new(bind_ip, tvu_port))?;
    tvu_socket.set_read_timeout(Some(Duration::from_millis(10)))?;
    set_recv_buffer(&tvu_socket, 32 * 1024 * 1024);

    Ok(std::thread::spawn(move || {
        let mut buf = [0u8; 1280];
        loop {
            if exit.load(Ordering::Relaxed) {
                break;
            }
            match tvu_socket.recv_from(&mut buf) {
                Ok((n, _)) => {
                    let bytes = &buf[..n];
                    if let Some(slot) = slot_of(bytes) {
                        observed_tip.fetch_max(slot, Ordering::Relaxed);
                    }
                    reconstructor.insert_packet(bytes);
                    sink.on_raw_shred(bytes);
                }
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut => {}
                Err(e) => log::warn!("[shred-net][tvu] recv error: {e}"),
            }
        }
    }))
}

#[allow(clippy::too_many_arguments)]
fn spawn_repair(
    cfg: &ShredNetConfig,
    keypair: Arc<Keypair>,
    exit: Arc<AtomicBool>,
    cluster_info: Arc<solana_gossip::cluster_info::ClusterInfo>,
    reconstructor: Arc<Reconstructor>,
    sink: Arc<dyn BlockSink>,
    targets: Arc<Mutex<VecDeque<u64>>>,
    stakes: Arc<Mutex<HashMap<[u8; 32], u64>>>,
    observed_tip: Arc<AtomicU64>,
) -> anyhow::Result<JoinHandle<()>> {
    const ORPHAN_AFTER: u32 = 5;
    // Concentrate repair bandwidth: a wide window dilutes the (rate-limited)
    // response stream across too many slots, so none completes before its
    // deadline. The window must be matched to available peer bandwidth.
    let target_window = cfg.target_window.max(1);
    let top_peers = cfg.top_peers.max(1);

    let bind_ip = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
    let repair_sock = UdpSocket::bind(SocketAddr::new(bind_ip, cfg.repair_port))?;
    repair_sock.set_read_timeout(Some(Duration::from_millis(5)))?;
    // Repair RESPONSES (the data path for historical slots) arrive in bursts —
    // one WindowIndex round fans out to ~20 peers × up to 128 indices, so shreds
    // come back thousands-at-once. The default ~200KB kernel buffer drops most of
    // them, forcing endless re-requests. Size it like the TVU socket.
    set_recv_buffer(&repair_sock, 32 * 1024 * 1024);
    let keep_window = cfg.keep_window;

    Ok(std::thread::spawn(move || {
        let mut driver: HashMap<u64, DriverState> = HashMap::new();
        let mut nonce: u32 = 0xdead_beef;
        let mut recv_buf = [0u8; 1500];
        let mut last_purge = Instant::now();

        // ── boundary counters (diagnostics) ──
        let mut diag_reqs: u64 = 0;
        let mut diag_send_err: u64 = 0;
        let mut diag_resp: u64 = 0;
        let mut diag_inserted: u64 = 0;
        let mut diag_pings: u64 = 0;
        let mut diag_pongs: u64 = 0;
        let mut diag_peers: usize = 0;
        let mut last_diag = Instant::now();

        loop {
            if exit.load(Ordering::Relaxed) {
                break;
            }

            // ── drain inbound: ping → pong, shred → Blockstore + fast lane ──
            // Cap high enough to empty a full burst each cycle (a small cap turns
            // the 32MB buffer into a slow drip).
            let mut drained = 0usize;
            while drained < 8192 {
                match repair_sock.recv_from(&mut recv_buf) {
                    Ok((n, src)) => {
                        match wire::parse_inbound(&recv_buf[..n]) {
                            wire::Inbound::Ping(token) => {
                                diag_pings += 1;
                                let pong = wire::build_pong(&keypair, token);
                                if repair_sock.send_to(&pong, src).is_ok() {
                                    diag_pongs += 1;
                                }
                            }
                            wire::Inbound::ShredResponse => {
                                diag_resp += 1;
                                if reconstructor.insert_packet(&recv_buf[..n]) {
                                    diag_inserted += 1;
                                }
                                sink.on_raw_shred(&recv_buf[..n]);
                            }
                            wire::Inbound::Other => {}
                        }
                        drained += 1;
                    }
                    Err(_) => break,
                }
            }

            sleep(Duration::from_millis(50));

            // ── admit new targets up to the in-flight window ──
            {
                let active = driver.len();
                let mut room = target_window.saturating_sub(active);
                let mut q = targets.lock().unwrap_or_else(|e| e.into_inner());
                while room > 0 {
                    match q.pop_front() {
                        Some(slot) => {
                            driver.entry(slot).or_insert_with(DriverState::new);
                            room -= 1;
                        }
                        None => break,
                    }
                }
            }

            // ── select peers ──
            let now_ms = now_millis();
            let stake_map = stakes.lock().unwrap_or_else(|e| e.into_inner()).clone();
            let peers = gossip::repair_peers(&cluster_info, &stake_map, now_ms, top_peers);
            diag_peers = peers.len();
            if peers.is_empty() {
                continue;
            }

            // ── per-slot repair dispatch ──
            let mut done = Vec::new();
            for (&slot, state) in driver.iter_mut() {
                if Instant::now() >= state.deadline {
                    done.push(slot);
                    continue;
                }
                if state.last_repair.elapsed() < Duration::from_millis(50) {
                    continue;
                }

                let is_full = reconstructor.is_full(slot);
                let last_index = reconstructor.last_index(slot);
                let missing = if is_full {
                    Vec::new()
                } else {
                    reconstructor.missing_indices(slot, 128)
                };

                let action = next_repair_action(
                    is_full,
                    last_index,
                    &missing,
                    state.highest_probed,
                    state.repair_rounds,
                    ORPHAN_AFTER,
                );

                match action {
                    RepairAction::Complete => {
                        if let Some(entries) = reconstructor.take_complete(slot) {
                            sink.on_complete_block(slot, entries);
                        }
                        done.push(slot);
                    }
                    RepairAction::Wait => {}
                    RepairAction::ProbeHighest => {
                        state.last_repair = Instant::now();
                        state.repair_rounds += 1;
                        state.highest_probed = true;
                        for (pk, addr) in &peers {
                            let req = wire::highest_window_index(&keypair, pk, slot, 0, nonce);
                            match repair_sock.send_to(&req, addr) {
                                Ok(_) => diag_reqs += 1,
                                Err(_) => diag_send_err += 1,
                            }
                            nonce = nonce.wrapping_add(1);
                        }
                    }
                    RepairAction::RequestOrphan => {
                        state.last_repair = Instant::now();
                        state.repair_rounds += 1;
                        for (pk, addr) in &peers {
                            let req = wire::orphan(&keypair, pk, slot, nonce);
                            match repair_sock.send_to(&req, addr) {
                                Ok(_) => diag_reqs += 1,
                                Err(_) => diag_send_err += 1,
                            }
                            nonce = nonce.wrapping_add(1);
                        }
                    }
                    RepairAction::RequestWindows(batch) => {
                        state.last_repair = Instant::now();
                        state.repair_rounds += 1;
                        // Ask ONE peer per missing index (rotate across peers and
                        // across rounds). Broadcasting every index to all 20 peers
                        // is ~20x redundant — you only need each shred once — and
                        // the flood trips serve_repair's per-requester rate limit,
                        // throttling responses to a trickle. Unserved indices are
                        // naturally retried next round against a different peer.
                        for (i, idx) in batch.iter().enumerate() {
                            let (pk, addr) = &peers[(i + state.repair_rounds as usize) % peers.len()];
                            let req = wire::window_index(&keypair, pk, slot, *idx, nonce);
                            match repair_sock.send_to(&req, addr) {
                                Ok(_) => diag_reqs += 1,
                                Err(_) => diag_send_err += 1,
                            }
                            nonce = nonce.wrapping_add(1);
                        }
                    }
                }
            }
            for slot in done {
                driver.remove(&slot);
            }

            // ── janitor: bound the ephemeral store ──
            if last_purge.elapsed() >= Duration::from_secs(5) {
                let tip = observed_tip.load(Ordering::Relaxed);
                if tip > keep_window {
                    reconstructor.purge_below(tip - keep_window);
                }
                last_purge = Instant::now();
            }

            // ── diagnostics: which boundary is the bottleneck? ──
            if last_diag.elapsed() >= Duration::from_secs(10) {
                let secs = last_diag.elapsed().as_secs_f64();
                let (mut known, mut full) = (0usize, 0usize);
                for &slot in driver.keys() {
                    if reconstructor.last_index(slot).is_some() {
                        known += 1;
                    }
                    if reconstructor.is_full(slot) {
                        full += 1;
                    }
                }
                eprintln!(
                    "[shred-net][repair] driver={} peers={} | reqs/s={:.0} resp/s={:.0} \
                     inserted/s={:.0} send_err={} pings={} pongs={} | last_idx_known={}/{} full_in_map={}",
                    driver.len(), diag_peers,
                    diag_reqs as f64 / secs,
                    diag_resp as f64 / secs,
                    diag_inserted as f64 / secs,
                    diag_send_err, diag_pings, diag_pongs,
                    known, driver.len(), full,
                );
                diag_reqs = 0; diag_resp = 0; diag_inserted = 0; diag_send_err = 0;
                diag_pings = 0; diag_pongs = 0;
                last_diag = Instant::now();
            }
        }
    }))
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Best-effort enlarge the UDP receive buffer (turbine bursts overflow the
/// default). Requires `sysctl net.core.rmem_max` headroom for full effect.
fn set_recv_buffer(socket: &UdpSocket, bytes: usize) {
    #[cfg(unix)]
    {
        use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd};
        let s2 = unsafe { socket2::Socket::from_raw_fd(socket.as_raw_fd()) };
        s2.set_recv_buffer_size(bytes).ok();
        let _ = s2.into_raw_fd(); // don't close the borrowed fd on drop
    }
    #[cfg(not(unix))]
    let _ = (socket, bytes);
}
