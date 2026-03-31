use rsedp::{CodecV1, MessageStorage, ReliableUdpConnection};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::UdpSocket;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();

    // Используем стабильный путь для персистентности
    let db_path = PathBuf::from("peer1.redb");
    let storage = Arc::new(MessageStorage::new(&db_path)?);

    let key = [0u8; 32];
    let codec = Arc::new(CodecV1::new(&key));

    let socket = Arc::new(UdpSocket::bind("127.0.0.1:8081").await?);
    let target_addr = "127.0.0.1:8082".parse()?;

    // own_id = "node_1", peer_id = "controller"
    let conn = ReliableUdpConnection::new(
        socket.clone(),
        codec,
        storage,
        target_addr,
        "node_1".into(),
        "controller".into(),
    )
    .await?;

    println!("Peer 1 (Sender: node_1) started on 127.0.0.1:8081");
    println!("Sending message to 127.0.0.1:8082...");

    let message = format!("Command from node_1 at {:?}", std::time::SystemTime::now());
    conn.send(&message).await?;

    let mut buf = [0u8; 4096];
    loop {
        let (len, _addr) = socket.recv_from(&mut buf).await?;
        if let Some(payload) = conn.process_incoming(&buf[..len]).await? {
            let received_msg: String = borsh::from_slice(&payload)?;
            println!("Received response: {}", received_msg);
        }
    }
}
