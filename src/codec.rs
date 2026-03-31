use borsh::{BorshDeserialize, BorshSerialize};
use chacha20poly1305::{
    ChaCha20Poly1305, Nonce,
    aead::{Aead, KeyInit},
};
use rand::Rng;

use crate::error::CodecError;

pub struct CodecV1 {
    cipher: ChaCha20Poly1305,
}

impl CodecV1 {
    pub fn new(key: &[u8; 32]) -> Self {
        Self {
            cipher: ChaCha20Poly1305::new(key.into()),
        }
    }

    pub fn encode<T: BorshSerialize>(&self, data: &T) -> Result<Vec<u8>, CodecError> {
        let serialized = borsh::to_vec(data).map_err(CodecError::Serialization)?;

        let mut nonce_bytes = [0u8; 12];
        rand::rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = self
            .cipher
            .encrypt(nonce, serialized.as_ref())
            .map_err(|_| CodecError::Encryption)?;

        let mut out = Vec::with_capacity(12 + ciphertext.len());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);

        Ok(out)
    }

    pub fn decode<T: BorshDeserialize>(&self, data: &[u8]) -> Result<T, CodecError> {
        if data.len() < 12 {
            return Err(CodecError::InvalidData);
        }

        let nonce = Nonce::from_slice(&data[..12]);
        let ciphertext = &data[12..];

        let plaintext = self
            .cipher
            .decrypt(nonce, ciphertext)
            .map_err(|_| CodecError::Decryption)?;

        let decoded = borsh::from_slice(&plaintext).map_err(CodecError::Serialization)?;

        Ok(decoded)
    }
}
