use crate::error::{Error, Result};
use aes_gcm::{
    Aes256Gcm, KeyInit, Nonce,
    aead::{Aead, AeadCore, OsRng, Payload, rand_core::RngCore},
};
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier, password_hash::SaltString};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use sha2::{Digest, Sha256};

pub(crate) fn random_token() -> String {
    let mut bytes = [0; 32];
    OsRng.fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}
pub(crate) fn digest(value: &str) -> Vec<u8> {
    Sha256::digest(value.as_bytes()).to_vec()
}
pub(crate) fn hash_password(password: &str) -> Result<String> {
    Argon2::default()
        .hash_password(password.as_bytes(), &SaltString::generate(&mut OsRng))
        .map(|h| h.to_string())
        .map_err(|_| Error::Internal)
}
pub(crate) fn verify_password(password: &str, hash: &str) -> bool {
    PasswordHash::new(hash).is_ok_and(|h| {
        Argon2::default()
            .verify_password(password.as_bytes(), &h)
            .is_ok()
    })
}
pub(crate) fn validate_password(password: &str) -> Result<()> {
    if !(12..=128).contains(&password.len()) {
        return Err(Error::Invalid("登录密码需要 12–128 字节"));
    }
    Ok(())
}
pub(crate) fn cipher(key: &[u8; 32]) -> Aes256Gcm {
    Aes256Gcm::new(key.into())
}
pub(crate) fn seal(cipher: &Aes256Gcm, value: &str, context: &str) -> Result<Vec<u8>> {
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let encrypted = cipher
        .encrypt(
            &nonce,
            Payload {
                msg: value.as_bytes(),
                aad: context.as_bytes(),
            },
        )
        .map_err(|_| Error::Internal)?;
    let mut result = nonce.to_vec();
    result.extend(encrypted);
    Ok(result)
}
pub(crate) fn unseal(cipher: &Aes256Gcm, value: &[u8], context: &str) -> Result<String> {
    if value.len() < 28 {
        return Err(Error::Internal);
    }
    let bytes = cipher
        .decrypt(
            Nonce::from_slice(&value[..12]),
            Payload {
                msg: &value[12..],
                aad: context.as_bytes(),
            },
        )
        .map_err(|_| Error::Internal)?;
    String::from_utf8(bytes).map_err(|_| Error::Internal)
}
