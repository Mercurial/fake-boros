use std::fmt::Error;
use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;
use pallas::crypto::hash::Hash;
use pallas::network::miniprotocols::txsubmission::{EraTxBody, EraTxId, Request};
use pallas::network::{facades::PeerClient, miniprotocols::txsubmission::TxIdAndSize};
use tokio::sync::{Mutex, RwLock};
use tokio::task;

use crate::Mempool;

/// A TxSubmitPeer has:
/// - Its own mempool
/// - An optional PeerClient connection
/// - The peer address and network magic
pub struct TxSubmitPeer {
    mempool: Arc<Mutex<Mempool>>,
    client: Arc<Mutex<Option<PeerClient>>>,
    peer_addr: String,
    network_magic: u64,
    unfulfilled_request: Arc<RwLock<Option<usize>>>,
}

impl TxSubmitPeer {
    /// Lightweight constructor: just set fields and create a new mempool.
    /// No I/O occurs here.
    pub fn new(peer_addr: &str, network_magic: u64) -> Self {
        TxSubmitPeer {
            mempool: Arc::new(Mutex::new(Mempool::new())),
            client: Arc::new(Mutex::new(None)),
            peer_addr: peer_addr.to_string(),
            network_magic,
            unfulfilled_request: Arc::new(RwLock::new(None)),
        }
    }

    /// Initialize the peer connection (async).
    /// 1) Connect to the Cardano node at `peer_addr`.
    /// 2) Send the `txsubmission` init message.
    /// 3) Spawn a background task to handle requests.
    /// 4) Return immediately (non-blocking).
    pub async fn init(&mut self) -> Result<(), Error> {
        // 1) Connect to the node
        let mut client = PeerClient::connect(&self.peer_addr, self.network_magic)
            .await
            .unwrap();

        // 2) Initialize the txsubmission mini-protocol
        client.txsubmission().send_init().await.unwrap();

        // 3) Store it so we can spawn our background loop
        self.client = Arc::new(Mutex::new(Some(client)));

        // 4) Spawn the loop in another task
        self.start_background_task();

        Ok(())
    }

    /// Spawns a background async loop that continuously
    /// waits for new requests from the connected node.
    fn start_background_task(&mut self) {
        let client_arc = Arc::clone(&self.client);
        let mempool_arc = Arc::clone(&self.mempool);
        let unfullfilled_request_arc = Arc::clone(&self.unfulfilled_request);

        task::spawn(async move {
            loop {
                async fn process_unfullfilled(
                    mempool: &Mempool,
                    client: &mut PeerClient,
                    unfullfilled_request_arc: Arc<RwLock<Option<usize>>>,
                    request: usize,
                ) -> Option<usize> {
                    let available = mempool.pending_total();

                    if available > 0 {
                        println!(
                            "found enough txs to fulfill request: request={}, available={}",
                            request, available
                        );

                        // we have all the tx we need to we process the work unit as a new one. We don't
                        // acknowledge anything because that already happened on the initial attempt to
                        // fullfil the request.
                        reply_txs(mempool, client, unfullfilled_request_arc, 0, request)
                            .await
                            .unwrap();
                        None
                    } else {
                        println!(
                            "still not enough txs to fulfill request: request={}, available={}",
                            request, available
                        );

                        // we wait a few secs to avoid turning this stage into a hot loop.
                        // TODO: we need to watch the mempool and abort the wait if there's a change in
                        // the list of available txs.

                        // Drop the mempool reference to avoid a deadlock.
                        let _ = mempool;
                        
                        tokio::time::sleep(Duration::from_secs(10)).await;

                        // we store the request again so that the next schedule know we're still waiting
                        // for new transactions.
                        Some(request)
                    }
                }

                async fn reply_txs(
                    mempool: &Mempool,
                    client: &mut PeerClient,
                    unfullfilled_request_arc: Arc<RwLock<Option<usize>>>,
                    ack: usize,
                    req: usize,
                ) -> Result<(), Error> {
                    mempool.acknowledge(ack);

                    let available = mempool.pending_total();

                    if available > 0 {
                        let txs = mempool.request(req);
                        propagate_txs(client, txs).await.unwrap();

                        let mut unfullfilled_request = unfullfilled_request_arc.write().await;
                        *unfullfilled_request = None;
                    } else {
                        println!(
                            "not enough txs to fulfill request: req={}, available={}",
                            req, available
                        );
                        let mut unfullfilled_request = unfullfilled_request_arc.write().await;
                        *unfullfilled_request = Some(req);
                        println!("stored unfulfilled request: req={}", req);
                    }

                    Ok(())
                }

                // Try to read the next request from the tx-submission protocol
                let mut client = client_arc.lock().await;
                let client = client.as_mut().unwrap();

                println!("Checking for unfulfilled request...");

                let unfullfilled_request = unfullfilled_request_arc.read().await;
                let unfullfilled_request_clone = *unfullfilled_request;
                drop(unfullfilled_request);

                match unfullfilled_request_clone {
                    Some(request) => {
                        let mempool = mempool_arc.lock().await;
                        process_unfullfilled(
                            &mempool,
                            client,
                            Arc::clone(&unfullfilled_request_arc),
                            request,
                        )
                        .await;
                    }
                    None => {
                        println!("Waiting for next request...");
                        let request = match client.txsubmission().next_request().await {
                            Ok(r) => r,
                            Err(e) => {
                                println!("Error reading request: {:?}", e);
                                break;
                            }
                        };

                        println!("Received request...");

                        // Pattern-match the request and handle it as needed
                        match request {
                            Request::TxIds(ack, req) => {
                                let ack = ack as usize;
                                let req = req as usize;

                                println!("blocking tx ids request: req={}, ack={}", req, ack);

                                let mempool = mempool_arc.lock().await;
                                reply_txs(
                                    &mempool,
                                    client,
                                    Arc::clone(&unfullfilled_request_arc),
                                    ack,
                                    req,
                                )
                                .await
                                .unwrap();
                            }
                            Request::TxIdsNonBlocking(ack, req) => {
                                println!("non-blocking tx ids request: req={}, ack={}", req, ack);

                                let mempool = mempool_arc.lock().await;
                                mempool.acknowledge(ack as usize);

                                let txs = mempool.request(req as usize);
                                propagate_txs(client, txs).await.unwrap();
                            }
                            Request::Txs(ids) => {
                                println!("tx batch request");

                                let mempool = mempool_arc.lock().await;
                                let to_send = ids
                                    .iter()
                                    // we omit any missing tx, we assume that this would be considered a protocol
                                    // violation and rejected by the upstream.
                                    .filter_map(|x| {
                                        mempool.find_inflight(&Hash::from(x.1.as_slice()))
                                    })
                                    .map(|x| EraTxBody(x.era, x.bytes.clone()))
                                    .collect_vec();

                                let result = client.txsubmission().reply_txs(to_send).await;

                                if let Err(err) = &result {
                                    println!("error sending txs upstream: {:?}", err);
                                }
                            }
                        };
                    }
                }
            }
        });
    }

    pub async fn add_tx(&self, tx: Vec<u8>) {
        let mempool = self.mempool.lock().await;
        mempool.receive_raw(&tx).unwrap();
    }
}

async fn propagate_txs(client: &mut PeerClient, txs: Vec<crate::mempool::Tx>) -> Result<(), Error> {
    println!("propagating tx ids: {}", txs.len());

    let payload = txs
        .iter()
        .map(|x| TxIdAndSize(EraTxId(x.era, x.hash.to_vec()), x.bytes.len() as u32))
        .collect_vec();

    client.txsubmission().reply_tx_ids(payload).await.unwrap();

    Ok(())
}
