//! Complete-lane gRPC sidecar.
//!
//! Runs shred-net's `ShredNet` (P2P repair → ephemeral agave Blockstore →
//! complete blocks) and serves each completed block over the Jito-compatible
//! `SubscribeEntries` stream — one message per slot, `entries = bincode(Vec<Entry>)`
//! — which Lumen's `feeds/shredstream` subscriber already consumes unchanged.
//!
//! Run:
//!   shred-net-server --ip <PUBLIC_IP> --keypair id.json --grpc-port 8888 \
//!     --depth 6000 --top-peers 100 --window 96 \
//!     --entrypoint entrypoint.mainnet-beta.solana.com:8001 --entrypoint ... (several)

mod server;

use {
    server::{start_grpc_server, ProtoEntry, ProtoTransaction},
    shred_net::{BlockSink, ShredNet, ShredNetConfig},
    solana_entry::entry::Entry,
    solana_keypair::{read_keypair_file, Keypair},
    solana_signer::Signer,
    std::{
        net::IpAddr,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread::sleep,
        time::Duration,
    },
    tokio::sync::broadcast,
};

/// Broadcasts each complete block (and its txs) to gRPC subscribers.
struct GrpcSink {
    entry_tx: broadcast::Sender<ProtoEntry>,
    tx_tx:    broadcast::Sender<ProtoTransaction>,
}

impl BlockSink for GrpcSink {
    fn on_complete_block(&self, slot: u64, entries: Vec<Entry>) {
        // One message per complete slot: bincode(Vec<Entry>). Matches the
        // shredstream proto; Lumen deserializes with its own solana version
        // (bincode wire format is identical across the major versions).
        if let Ok(bytes) = bincode::serialize(&entries) {
            let _ = self.entry_tx.send(ProtoEntry { slot, entries: bytes });
        }
        for entry in &entries {
            for tx in &entry.transactions {
                let sig = tx.signatures.first().map(|s| s.to_string()).unwrap_or_default();
                if let Ok(raw) = bincode::serialize(tx) {
                    let _ = self.tx_tx.send(ProtoTransaction { slot, signature: sig, raw_tx: raw });
                }
            }
        }
    }
    // on_slot_skipped: nothing to emit (no block); a future revision could send
    // an explicit empty-slot marker so the consumer advances without waiting.
}

struct Args {
    ip:           IpAddr,
    gossip_port:  u16,
    tvu_port:     u16,
    repair_port:  u16,
    grpc_port:    u16,
    entrypoints:  Vec<String>,
    keypair_path: Option<String>,
    auth_token:   Option<String>,
    depth:        u64,
    top_peers:    usize,
    window:       usize,
}

fn parse_args() -> Result<Args, String> {
    let mut ip: Option<IpAddr> = None;
    let mut gossip_port = 8000u16;
    let mut tvu_port = 8200u16;
    let mut repair_port = 8210u16;
    let mut grpc_port = 8888u16;
    let mut entrypoints = Vec::new();
    let mut keypair_path = None;
    let mut auth_token = None;
    let mut depth = 6000u64;
    let mut top_peers = 64usize;
    let mut window = 64usize;

    let argv: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < argv.len() {
        match argv[i].as_str() {
            "--ip"          => { i += 1; ip = Some(argv[i].parse().map_err(|e| format!("--ip: {e}"))?); }
            "--port"        => { i += 1; gossip_port = argv[i].parse().map_err(|e| format!("--port: {e}"))?; }
            "--tvu-port"    => { i += 1; tvu_port = argv[i].parse().map_err(|e| format!("--tvu-port: {e}"))?; }
            "--repair-port" => { i += 1; repair_port = argv[i].parse().map_err(|e| format!("--repair-port: {e}"))?; }
            "--grpc-port"   => { i += 1; grpc_port = argv[i].parse().map_err(|e| format!("--grpc-port: {e}"))?; }
            "--entrypoint"  => { i += 1; entrypoints.push(argv[i].clone()); }
            "--keypair"     => { i += 1; keypair_path = Some(argv[i].clone()); }
            "--auth-token"  => { i += 1; auth_token = Some(argv[i].clone()); }
            "--depth"       => { i += 1; depth = argv[i].parse().map_err(|e| format!("--depth: {e}"))?; }
            "--top-peers"   => { i += 1; top_peers = argv[i].parse().map_err(|e| format!("--top-peers: {e}"))?; }
            "--window"      => { i += 1; window = argv[i].parse().map_err(|e| format!("--window: {e}"))?; }
            other => return Err(format!("unknown arg: {other}")),
        }
        i += 1;
    }

    Ok(Args {
        ip: ip.ok_or("--ip <PUBLIC_IP> is required")?,
        gossip_port, tvu_port, repair_port, grpc_port,
        entrypoints, keypair_path, auth_token, depth, top_peers, window,
    })
}

fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = parse_args().map_err(|e| anyhow::anyhow!(e))?;

    let keypair = Arc::new(match &args.keypair_path {
        Some(p) => read_keypair_file(p).map_err(|e| anyhow::anyhow!("read keypair {p}: {e}"))?,
        None => Keypair::new(),
    });
    eprintln!("[server] pubkey={}  advertise_ip={}", keypair.pubkey(), args.ip);
    eprintln!(
        "[server] grpc_port={}  depth={}  top_peers={}  window={}  entrypoints={}",
        args.grpc_port, args.depth, args.top_peers, args.window, args.entrypoints.len()
    );

    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    let exit = Arc::new(AtomicBool::new(false));

    let (entry_tx, _) = broadcast::channel::<ProtoEntry>(8_192);
    let (tx_tx, _) = broadcast::channel::<ProtoTransaction>(65_536);

    // gRPC server on the tokio runtime.
    let grpc_addr = format!("0.0.0.0:{}", args.grpc_port).parse()?;
    {
        let _guard = rt.enter();
        start_grpc_server(
            grpc_addr,
            Arc::new(entry_tx.clone()),
            Arc::new(tx_tx.clone()),
            args.auth_token.clone(),
            exit.clone(),
        );
    }

    // Ctrl-C → graceful exit.
    {
        let exit_c = exit.clone();
        rt.spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            eprintln!("\n[server] shutting down…");
            exit_c.store(true, Ordering::SeqCst);
        });
    }

    // Start the complete lane.
    let sink = Arc::new(GrpcSink { entry_tx, tx_tx });
    let net = ShredNet::start(
        ShredNetConfig {
            advertise_ip: args.ip,
            gossip_port:  args.gossip_port,
            tvu_port:     args.tvu_port,
            repair_port:  args.repair_port,
            shred_version: None,
            entrypoints:  args.entrypoints,
            keep_window:  args.depth + 2_000,
            target_window: args.window,
            top_peers:    args.top_peers,
        },
        keypair,
        sink,
    )?;
    eprintln!("[server] ShredNet started — waiting for first turbine shred…");

    // Wait for a tip, then drive repair: boot gap [tip-depth .. tip-1], and
    // continuously request newly-arriving slots (steady-state gap-fill).
    while net.observed_tip() == 0 {
        if exit.load(Ordering::Relaxed) {
            net.shutdown();
            return Ok(());
        }
        sleep(Duration::from_millis(250));
    }

    let tip = net.observed_tip();
    let mut last_req = tip.saturating_sub(args.depth);
    if tip > last_req {
        net.request_slots(last_req..=tip.saturating_sub(1));
    }
    last_req = tip.saturating_sub(1);
    eprintln!("[server] boot gap requested up to tip={tip}; now serving + gap-filling.");

    loop {
        if exit.load(Ordering::Relaxed) {
            break;
        }
        sleep(Duration::from_millis(400));
        let tip = net.observed_tip();
        if tip > last_req + 1 {
            net.request_slots((last_req + 1)..=(tip - 1));
            last_req = tip - 1;
        }
    }

    net.shutdown();
    Ok(())
}
