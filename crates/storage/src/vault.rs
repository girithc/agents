//! AES-256-GCM secrets vault.
//!
//! A 32-byte master key lives in `<data_dir>/master.key` (created on first
//! use with 0600 permissions). Secret values are encrypted per-write with a
//! fresh 12-byte nonce; the stored blob is `nonce || ciphertext`.

use std::fs;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};
use rand::RngCore;

use crate::error::{Result, StorageError};

const KEY_LEN: usize = 32;
const NONCE_LEN: usize = 12;

pub struct Vault {
    cipher: Aes256Gcm,
    #[allow(dead_code)]
    key_path: PathBuf,
}

impl Vault {
    /// Open (or create) the master key file and construct the cipher.
    pub fn open(key_path: &Path) -> Result<Self> {
        if let Some(dir) = key_path.parent() {
            if !dir.as_os_str().is_empty() {
                fs::create_dir_all(dir)?;
            }
        }

        let key: Vec<u8> = if key_path.exists() {
            let bytes = fs::read(key_path)?;
            if bytes.len() != KEY_LEN {
                return Err(StorageError::Crypto(format!(
                    "master key at {} has {} bytes, expected {KEY_LEN}",
                    key_path.display(),
                    bytes.len()
                )));
            }
            bytes
        } else {
            let mut key = vec![0u8; KEY_LEN];
            rand::rngs::OsRng.fill_bytes(&mut key);
            let mut f = fs::OpenOptions::new()
                .mode(0o600)
                .create_new(true)
                .write(true)
                .open(key_path)?;
            f.write_all(&key)?;
            key
        };

        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key));
        Ok(Self {
            cipher,
            key_path: key_path.to_path_buf(),
        })
    }

    pub fn encrypt(&self, plaintext: &str) -> Result<Vec<u8>> {
        let mut nonce_bytes = [0u8; NONCE_LEN];
        rand::rngs::OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ct = self
            .cipher
            .encrypt(nonce, plaintext.as_bytes())
            .map_err(|e| StorageError::Crypto(format!("encrypt failed: {e}")))?;
        let mut blob = nonce_bytes.to_vec();
        blob.extend_from_slice(&ct);
        Ok(blob)
    }

    pub fn decrypt(&self, blob: &[u8]) -> Result<String> {
        if blob.len() <= NONCE_LEN {
            return Err(StorageError::Crypto("encrypted blob too short".into()));
        }
        let (nonce, ct) = blob.split_at(NONCE_LEN);
        let pt = self
            .cipher
            .decrypt(Nonce::from_slice(nonce), ct)
            .map_err(|_| StorageError::Crypto("decrypt failed (wrong master key?)".into()))?;
        String::from_utf8(pt).map_err(|e| StorageError::Crypto(format!("plaintext not utf8: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let vault = Vault::open(&dir.path().join("master.key")).unwrap();
        let blob = vault.encrypt("hunter2 🤫").unwrap();
        assert_ne!(blob, b"hunter2");
        assert_eq!(vault.decrypt(&blob).unwrap(), "hunter2 🤫");
    }

    #[test]
    fn decrypt_with_wrong_key_fails() {
        let dir = tempfile::tempdir().unwrap();
        let v1 = Vault::open(&dir.path().join("k1")).unwrap();
        let v2 = Vault::open(&dir.path().join("k2")).unwrap();
        let blob = v1.encrypt("secret").unwrap();
        assert!(v2.decrypt(&blob).is_err());
    }
}
