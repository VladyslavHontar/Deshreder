//! Jito-compatible `ShredstreamProxy` gRPC server (adapted from Colibri).
//! Streams complete-block `Entry` and per-tx `Transaction` messages produced by
//! the complete lane to subscribers (e.g. Lumen's `feeds/shredstream`).

use {
    std::{
        net::SocketAddr,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
    },
    tokio::sync::broadcast::Sender,
    tonic::{
        codegen::tokio_stream::wrappers::ReceiverStream, transport::Server, Request, Response,
        Status,
    },
};

pub mod shredstream {
    tonic::include_proto!("shredstream");
}

use shredstream::{
    shredstream_proxy_server::{ShredstreamProxy, ShredstreamProxyServer},
    Entry, SubscribeEntriesRequest, SubscribeTransactionsRequest, Transaction,
};

pub use shredstream::Entry as ProtoEntry;
pub use shredstream::Transaction as ProtoTransaction;

struct Service {
    entry_sender: Arc<Sender<Entry>>,
    tx_sender:    Arc<Sender<Transaction>>,
    auth_token:   Option<Arc<String>>,
}

fn check_auth(
    auth_token: &Option<Arc<String>>,
    md: &tonic::metadata::MetadataMap,
) -> Result<(), Status> {
    if let Some(expected) = auth_token {
        let provided = md
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .unwrap_or("");
        if provided.as_bytes() != expected.as_bytes() {
            return Err(Status::unauthenticated("invalid token"));
        }
    }
    Ok(())
}

#[tonic::async_trait]
impl ShredstreamProxy for Service {
    type SubscribeEntriesStream = ReceiverStream<Result<Entry, Status>>;
    type SubscribeTransactionsStream = ReceiverStream<Result<Transaction, Status>>;

    async fn subscribe_entries(
        &self,
        request: Request<SubscribeEntriesRequest>,
    ) -> Result<Response<Self::SubscribeEntriesStream>, Status> {
        check_auth(&self.auth_token, request.metadata())?;
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        let mut bcast = self.entry_sender.subscribe();
        tokio::spawn(async move {
            loop {
                match bcast.recv().await {
                    Ok(entry) => match tx.try_send(Ok(entry)) {
                        Ok(()) => {}
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => break,
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break,
                    },
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        let _ = tx.try_send(Err(Status::data_loss(format!(
                            "stream lagged: {n} entries dropped"
                        ))));
                        break;
                    }
                    Err(_) => break,
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn subscribe_transactions(
        &self,
        request: Request<SubscribeTransactionsRequest>,
    ) -> Result<Response<Self::SubscribeTransactionsStream>, Status> {
        check_auth(&self.auth_token, request.metadata())?;
        let (tx, rx) = tokio::sync::mpsc::channel(1_024);
        let mut bcast = self.tx_sender.subscribe();
        tokio::spawn(async move {
            loop {
                match bcast.recv().await {
                    Ok(t) => match tx.try_send(Ok(t)) {
                        Ok(()) => {}
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => break,
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break,
                    },
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        let _ = tx.try_send(Err(Status::data_loss(format!(
                            "stream lagged: {n} transactions dropped"
                        ))));
                        break;
                    }
                    Err(_) => break,
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// Spawn the gRPC server on the current tokio runtime. Stops when `exit` is set.
pub fn start_grpc_server(
    addr:         SocketAddr,
    entry_sender: Arc<Sender<Entry>>,
    tx_sender:    Arc<Sender<Transaction>>,
    auth_token:   Option<String>,
    exit:         Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        eprintln!("[grpc] listening on {addr}");
        let svc = Service {
            entry_sender,
            tx_sender,
            auth_token: auth_token.map(Arc::new),
        };
        let shutdown = async move {
            loop {
                if exit.load(Ordering::Relaxed) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        };
        if let Err(e) = Server::builder()
            .add_service(ShredstreamProxyServer::new(svc))
            .serve_with_shutdown(addr, shutdown)
            .await
        {
            eprintln!("[grpc] server error: {e}");
        }
    })
}
