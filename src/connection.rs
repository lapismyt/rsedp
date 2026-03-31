use crate::codec::CodecV1;
use crate::error::{CodecError, ConnectionError};
use crate::storage::MessageStorage;
use borsh::{BorshDeserialize, BorshSerialize};
use log::{debug, error};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::time::{self, Duration};

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
pub enum ProtocolFrameV1 {
    Data {
        sender_id: String,
        seq_num: u64,
        payload: Vec<u8>,
    },
    Ack(u64),
}

pub struct ReliableUdpConnection {
    socket: Arc<UdpSocket>,
    codec: Arc<CodecV1>,
    storage: Arc<MessageStorage>,
    target_addr: SocketAddr,
    own_id: String,
    peer_id: String,
}

impl ReliableUdpConnection {
    pub async fn new(
        socket: Arc<UdpSocket>,
        codec: Arc<CodecV1>,
        storage: Arc<MessageStorage>,
        target_addr: SocketAddr,
        own_id: String,
        peer_id: String,
    ) -> Result<Self, ConnectionError> {
        let conn = Self {
            socket,
            codec,
            storage,
            target_addr,
            own_id,
            peer_id,
        };

        // Start background retry task
        conn.spawn_retry_task();

        Ok(conn)
    }

    pub async fn send<T: BorshSerialize>(&self, data: &T) -> Result<(), ConnectionError> {
        let payload = borsh::to_vec(data).map_err(CodecError::Serialization)?;
        let seq_num = self.storage.store_outgoing(&self.peer_id, payload)?;

        self.send_raw_data(seq_num).await?;
        Ok(())
    }

    async fn send_raw_data(&self, seq_num: u64) -> Result<(), ConnectionError> {
        let all_outgoing = self.storage.get_all_outgoing(&self.peer_id)?;
        if let Some((_, payload)) = all_outgoing.iter().find(|(s, _)| *s == seq_num) {
            let frame = ProtocolFrameV1::Data {
                sender_id: self.own_id.clone(),
                seq_num,
                payload: payload.clone(),
            };
            let encoded = self.codec.encode(&frame)?;
            self.socket.send_to(&encoded, self.target_addr).await?;
        }
        Ok(())
    }

    pub async fn process_incoming(&self, data: &[u8]) -> Result<Option<Vec<u8>>, ConnectionError> {
        let frame: ProtocolFrameV1 = self.codec.decode(data)?;

        match frame {
            ProtocolFrameV1::Ack(seq_num) => {
                debug!("Received ACK for {}", seq_num);
                self.storage.remove_outgoing(&self.peer_id, seq_num)?;
                Ok(None)
            }
            ProtocolFrameV1::Data {
                sender_id,
                seq_num,
                payload,
            } => {
                debug!("Received Data from {} with seq {}", sender_id, seq_num);
                // Send ACK immediately
                let ack_frame = ProtocolFrameV1::Ack(seq_num);
                let encoded = self.codec.encode(&ack_frame)?;
                self.socket.send_to(&encoded, self.target_addr).await?;

                // Check if already processed from THIS sender
                if self.storage.is_received(&sender_id, seq_num)? {
                    debug!(
                        "Message {} from {} already processed, skipping",
                        seq_num, sender_id
                    );
                    return Ok(None);
                }

                self.storage.mark_received(&sender_id, seq_num)?;
                Ok(Some(payload))
            }
        }
    }

    fn spawn_retry_task(&self) {
        let storage = self.storage.clone();
        let socket = self.socket.clone();
        let codec = self.codec.clone();
        let target_addr = self.target_addr;
        let peer_id = self.peer_id.clone();
        let own_id = self.own_id.clone();

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(2));
            loop {
                interval.tick().await;

                match storage.get_all_outgoing(&peer_id) {
                    Ok(messages) => {
                        for (seq_num, payload) in messages {
                            debug!("Retrying message {} to {}", seq_num, peer_id);
                            let frame = ProtocolFrameV1::Data {
                                sender_id: own_id.clone(),
                                seq_num,
                                payload,
                            };
                            match codec.encode(&frame) {
                                Ok(encoded) => {
                                    if let Err(e) = socket.send_to(&encoded, target_addr).await {
                                        error!("Failed to resend message {}: {}", seq_num, e);
                                    }
                                }
                                Err(e) => error!("Failed to encode frame for retry: {}", e),
                            }
                        }
                    }
                    Err(e) => error!("Failed to get outgoing messages for retry: {}", e),
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_reliable_connection() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.redb");
        let storage = Arc::new(MessageStorage::new(&db_path).unwrap());
        let key = [0u8; 32];
        let codec = Arc::new(CodecV1::new(&key));

        let socket1 = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let addr1 = socket1.local_addr().unwrap();
        let socket2 = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let addr2 = socket2.local_addr().unwrap();

        let conn1 = ReliableUdpConnection::new(
            socket1.clone(),
            codec.clone(),
            storage.clone(),
            addr2,
            "node1".into(),
            "controller".into(),
        )
        .await
        .unwrap();

        let db_path2 = dir.path().join("test2.redb");
        let storage2 = Arc::new(MessageStorage::new(&db_path2).unwrap());
        let conn2 = ReliableUdpConnection::new(
            socket2.clone(),
            codec.clone(),
            storage2.clone(),
            addr1,
            "controller".into(),
            "node1".into(),
        )
        .await
        .unwrap();

        let test_msg = "Hello, VPN!".to_string();
        conn1.send(&test_msg).await.unwrap();

        let mut buf = [0u8; 1024];
        let (len, _addr) = socket2.recv_from(&mut buf).await.unwrap();
        let received = conn2.process_incoming(&buf[..len]).await.unwrap();

        assert!(received.is_some());
        let decoded_msg: String = borsh::from_slice(&received.unwrap()).unwrap();
        assert_eq!(decoded_msg, test_msg);

        let (len, _addr) = socket1.recv_from(&mut buf).await.unwrap();
        conn1.process_incoming(&buf[..len]).await.unwrap();

        assert!(storage.get_all_outgoing("controller").unwrap().is_empty());
    }
}
