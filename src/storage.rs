use crate::error::ConnectionError;
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use std::path::Path;

const OUTGOING_MESSAGES: TableDefinition<(&str, u64), Vec<u8>> = TableDefinition::new("outgoing");
const RECEIVED_MESSAGES: TableDefinition<(&str, u64), bool> = TableDefinition::new("received");
const METADATA: TableDefinition<&str, u64> = TableDefinition::new("metadata");

pub struct MessageStorage {
    db: Database,
}

impl MessageStorage {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, ConnectionError> {
        let db = Database::builder()
            .create(path)
            .map_err(|e| ConnectionError::Storage(format!("Failed to create/open DB: {}", e)))?;

        // Initialize tables
        let txn = db
            .begin_write()
            .map_err(|e| ConnectionError::Storage(format!("Failed to begin write txn: {}", e)))?;
        {
            let _ = txn.open_table(OUTGOING_MESSAGES).map_err(|e| {
                ConnectionError::Storage(format!("Failed to open outgoing table: {}", e))
            })?;
            let _ = txn.open_table(RECEIVED_MESSAGES).map_err(|e| {
                ConnectionError::Storage(format!("Failed to open received table: {}", e))
            })?;
            let _ = txn.open_table(METADATA).map_err(|e| {
                ConnectionError::Storage(format!("Failed to open metadata table: {}", e))
            })?;
        }
        txn.commit()
            .map_err(|e| ConnectionError::Storage(format!("Failed to commit txn: {}", e)))?;

        Ok(Self { db })
    }

    pub fn is_received(&self, peer_id: &str, seq_num: u64) -> Result<bool, ConnectionError> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e: redb::TransactionError| ConnectionError::Storage(e.to_string()))?;
        let received = txn
            .open_table(RECEIVED_MESSAGES)
            .map_err(|e: redb::TableError| ConnectionError::Storage(e.to_string()))?;
        Ok(received
            .get((peer_id, seq_num))
            .map_err(|e: redb::StorageError| ConnectionError::Storage(e.to_string()))?
            .is_some())
    }

    pub fn mark_received(&self, peer_id: &str, seq_num: u64) -> Result<(), ConnectionError> {
        let mut txn = self
            .db
            .begin_write()
            .map_err(|e| ConnectionError::Storage(e.to_string()))?;
        let _ = txn.set_durability(Durability::Immediate);
        {
            let mut received = txn
                .open_table(RECEIVED_MESSAGES)
                .map_err(|e| ConnectionError::Storage(e.to_string()))?;
            received
                .insert((peer_id, seq_num), true)
                .map_err(|e| ConnectionError::Storage(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| ConnectionError::Storage(e.to_string()))?;
        Ok(())
    }

    pub fn store_outgoing(&self, peer_id: &str, payload: Vec<u8>) -> Result<u64, ConnectionError> {
        let mut txn = self
            .db
            .begin_write()
            .map_err(|e| ConnectionError::Storage(e.to_string()))?;
        let _ = txn.set_durability(Durability::Immediate);
        let seq_num;
        {
            let mut metadata = txn
                .open_table(METADATA)
                .map_err(|e| ConnectionError::Storage(e.to_string()))?;
            let meta_key = format!("next_seq_{}", peer_id);
            seq_num = metadata
                .get(meta_key.as_str())
                .map_err(|e: redb::StorageError| ConnectionError::Storage(e.to_string()))?
                .map(|v| v.value())
                .unwrap_or(0);

            metadata
                .insert(meta_key.as_str(), seq_num + 1)
                .map_err(|e| ConnectionError::Storage(e.to_string()))?;

            let mut outgoing = txn
                .open_table(OUTGOING_MESSAGES)
                .map_err(|e| ConnectionError::Storage(e.to_string()))?;
            outgoing
                .insert((peer_id, seq_num), payload)
                .map_err(|e| ConnectionError::Storage(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| ConnectionError::Storage(e.to_string()))?;
        Ok(seq_num)
    }

    pub fn remove_outgoing(&self, peer_id: &str, seq_num: u64) -> Result<(), ConnectionError> {
        let mut txn = self
            .db
            .begin_write()
            .map_err(|e| ConnectionError::Storage(e.to_string()))?;
        let _ = txn.set_durability(Durability::Immediate);
        {
            let mut outgoing = txn
                .open_table(OUTGOING_MESSAGES)
                .map_err(|e| ConnectionError::Storage(e.to_string()))?;
            outgoing
                .remove((peer_id, seq_num))
                .map_err(|e| ConnectionError::Storage(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| ConnectionError::Storage(e.to_string()))?;
        Ok(())
    }

    pub fn get_all_outgoing(&self, peer_id: &str) -> Result<Vec<(u64, Vec<u8>)>, ConnectionError> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e: redb::TransactionError| ConnectionError::Storage(e.to_string()))?;
        let outgoing = txn
            .open_table(OUTGOING_MESSAGES)
            .map_err(|e: redb::TableError| ConnectionError::Storage(e.to_string()))?;

        let mut messages = Vec::new();
        // Range from (peer_id, 0) to (peer_id, max)
        for item in outgoing
            .range((peer_id, 0)..=(peer_id, u64::MAX))
            .map_err(|e: redb::StorageError| ConnectionError::Storage(e.to_string()))?
        {
            let (k, v) =
                item.map_err(|e: redb::StorageError| ConnectionError::Storage(e.to_string()))?;
            let (_p, seq) = k.value();
            messages.push((seq, v.value().to_vec()));
        }
        Ok(messages)
    }
}
