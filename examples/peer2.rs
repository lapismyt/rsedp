use rsedp::{CodecV1, MessageStorage, ReliableUdpConnection};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::UdpSocket;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();

    let db_path = PathBuf::from("peer2.redb");
    let storage = Arc::new(MessageStorage::new(&db_path)?);

    let key = [0u8; 32];
    let codec = Arc::new(CodecV1::new(&key));

    let socket = Arc::new(UdpSocket::bind("127.0.0.1:8082").await?);
    let target_addr = "127.0.0.1:8081".parse()?;

    // own_id = "controller", peer_id = "node_1"
    let conn = ReliableUdpConnection::new(
        socket.clone(),
        codec,
        storage,
        target_addr,
        "controller".into(),
        "node_1".into(),
    )
    .await?;

    println!("Peer 2 (Receiver: controller) started on 127.0.0.1:8082");

    let mut buf = [0u8; 4096];
    loop {
        let (len, _addr) = socket.recv_from(&mut buf).await?;
        if let Some(payload) = conn.process_incoming(&buf[..len]).await? {
            let received_msg: String = borsh::from_slice(&payload)?;
            println!("Received: {}", received_msg);

            let response = format!("ACK for '{}' from controller", received_msg);
            conn.send(&response).await?;
        }
    }
}
