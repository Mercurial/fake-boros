use mempool::Mempool;
use rocket::{launch, post, routes, State};
use rocket::data::{Data, ToByteUnit};
use rocket::http::Status;
use tx_submit_peer_manager::TxSubmitPeerManager;

mod mempool;
mod tx_submit_peer;
mod tx_submit_peer_manager;


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

    println!("Tx Cbor: {:?}", hex::encode(&raw_cbor));

    // Store the transaction in our mempool.
    // We'll lock the mutex, then push the new transaction.
    tx_submit_peer_manager.add_tx(raw_cbor.clone()).await;

    // // Return how many total transactions are currently in the mempool.
    // let len = mempool.pending_tx.lock().unwrap().len();
    Ok(format!(
        "Received {} bytes of CBOR data. Mempool size is now {}.",
        raw_cbor.len(),
        0
    ))
}

#[launch]
async fn rocket() -> _ {
    let mempool = Mempool::new();

    let peer_addr = "preview-node.play.dev.cardano.org:3001";
    let peer_addresses = vec![peer_addr.to_string()];

    let mut tx_submit_peer_manager = TxSubmitPeerManager::new(2, peer_addresses);
    tx_submit_peer_manager.init().await.unwrap();

    rocket::build()
        .manage(mempool)
        .manage(tx_submit_peer_manager)
        .mount("/", routes![submit_tx])
}