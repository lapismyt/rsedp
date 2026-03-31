use thiserror::Error;

#[derive(Error, Debug)]
pub enum CodecError {
    #[error("Serialization error: {0}")]
    Serialization(std::io::Error),
    #[error("Encryption error")]
    Encryption,
    #[error("Decryption error")]
    Decryption,
    #[error("Invalid data")]
    InvalidData,
}

#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Codec error: {0}")]
    Codec(#[from] CodecError),
    #[error("Storage error: {0}")]
    Storage(String),
}
