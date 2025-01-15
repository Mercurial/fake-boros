use mempool::Mempool;
use pallas::ledger::traverse::{tx, MultiEraTx};
use rocket::{launch, post, routes, State};
use rocket::data::{Data, ToByteUnit};
use rocket::http::Status;
use tx_submit_peer_manager::TxSubmitPeerManager;

mod mempool;
mod tx_submit_peer;
mod tx_submit_peer_manager;

use tracing::Level;

/// POST endpoint that accepts "application/cbor" data.
/// Reads the body as raw bytes, then pushes it into our mempool.
#[post("/api/submit/tx", format = "application/cbor", data = "<cbor_data>")]
async fn submit_tx(
    cbor_data: Data<'_>,
    tx_submit_peer_manager: &State<TxSubmitPeerManager>, 
) -> Result<String, Status> {
    // Limit how many bytes we read (16 KB here).
    let max_size = 16.kilobytes();

    // Read the raw bytes from the request body.
    let bytes = match cbor_data.open(max_size).into_bytes().await {
        Ok(buf) => buf,
        Err(_) => return Err(Status::PayloadTooLarge),
    };

    // The `bytes.value` is a `Vec<u8>` containing the raw CBOR data.
    let raw_cbor = bytes.value;

    tracing::info!("Tx Cbor: {:?}", hex::encode(&raw_cbor));

    let parsed_tx = MultiEraTx::decode(&raw_cbor).unwrap();
    let tx_hash = parsed_tx.hash();

    // Store the transaction in our mempool.
    // We'll lock the mutex, then push the new transaction.
    tx_submit_peer_manager.add_tx(raw_cbor.clone()).await;

    // Return the transaction hash as a response.
    Ok(hex::encode(tx_hash))
}

#[launch]
async fn rocket() -> _ {

    tracing_subscriber::fmt()
    .with_max_level(Level::INFO)
    .init();

    tracing::info!("Starting server...");

    let mempool = Mempool::new();

    let peer_addresses = vec![
        "preview-node.play.dev.cardano.org:3001".to_string(),
        "adaboy-preview-1c.gleeze.com:5000".to_string(),
        "testicles.kiwipool.org:9720".to_string()
    ];

    let mut tx_submit_peer_manager = TxSubmitPeerManager::new(2, peer_addresses);
    tx_submit_peer_manager.init().await.unwrap();

    rocket::build()
        .manage(mempool)
        .manage(tx_submit_peer_manager)
        .mount("/", routes![submit_tx])
}