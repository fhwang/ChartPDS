//! Content-addressed key type and hashing.

use sha2::{Digest, Sha256};
use thiserror::Error;

/// A content-addressed key for a blob in the archive.
///
/// Always the lowercase hex encoding of a SHA-256 hash (64 ASCII chars).
/// The only way to obtain one is via [`compute_blob_key`] (from content) or
/// [`BlobKey::from_hex_str`] (from a serialized form). Both validate.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlobKey(String);

impl BlobKey {
    /// Parse a `BlobKey` from a 64-character lowercase hex string.
    ///
    /// # Errors
    ///
    /// Returns [`KeyParseError`] if the input is not exactly 64 chars or
    /// contains any non-hex character.
    pub fn from_hex_str(s: &str) -> Result<Self, KeyParseError> {
        if s.len() != 64 {
            return Err(KeyParseError::WrongLength { got: s.len() });
        }
        if !s
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(KeyParseError::NotHex);
        }
        Ok(Self(s.to_owned()))
    }

    /// Borrow the underlying hex string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for BlobKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for BlobKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Compute the [`BlobKey`] for the given content (SHA-256 hex).
#[must_use]
pub fn compute_blob_key(content: &[u8]) -> BlobKey {
    let mut hasher = Sha256::new();
    hasher.update(content);
    let digest = hasher.finalize();
    BlobKey(hex::encode(digest))
}

/// Errors returned by [`BlobKey::from_hex_str`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum KeyParseError {
    /// The input length was not 64 characters.
    #[error("blob key must be exactly 64 hex chars (got {got})")]
    WrongLength {
        /// Length of the input that was rejected.
        got: usize,
    },

    /// The input contained a non-hex-digit byte.
    #[error("blob key contains non-hex characters")]
    NotHex,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_blob_key_matches_known_sha256_of_empty_input() {
        // SHA-256 of empty input is well-known.
        let key = compute_blob_key(b"");
        assert_eq!(
            key.as_str(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn compute_blob_key_matches_known_sha256_of_hello() {
        let key = compute_blob_key(b"hello");
        assert_eq!(
            key.as_str(),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn blob_key_from_hex_str_accepts_valid_64char_hex() {
        let s = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let key = BlobKey::from_hex_str(s).expect("valid key");
        assert_eq!(key.as_str(), s);
    }

    #[test]
    fn blob_key_from_hex_str_rejects_wrong_length() {
        assert!(BlobKey::from_hex_str("abc").is_err());
        assert!(BlobKey::from_hex_str(&"a".repeat(63)).is_err());
        assert!(BlobKey::from_hex_str(&"a".repeat(65)).is_err());
    }

    #[test]
    fn blob_key_from_hex_str_rejects_non_hex_chars() {
        let s = format!("z{}", "a".repeat(63));
        assert!(BlobKey::from_hex_str(&s).is_err());
    }

    #[test]
    fn blob_key_display_outputs_hex_string() {
        let key = compute_blob_key(b"hello");
        assert_eq!(
            format!("{key}"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn compute_blob_key_is_deterministic() {
        assert_eq!(compute_blob_key(b"abc"), compute_blob_key(b"abc"));
    }

    #[test]
    fn compute_blob_key_differs_for_different_content() {
        assert_ne!(compute_blob_key(b"abc"), compute_blob_key(b"abd"));
    }

    #[test]
    fn compute_blob_key_round_trips_through_from_hex_str() {
        let original = compute_blob_key(b"round trip");
        let reparsed = BlobKey::from_hex_str(original.as_str()).expect("round-trip parse");
        assert_eq!(original, reparsed);
    }

    #[test]
    fn blob_key_from_hex_str_rejects_empty_string() {
        assert!(BlobKey::from_hex_str("").is_err());
    }

    #[test]
    fn blob_key_from_hex_str_rejects_uppercase_hex() {
        // The canonical form is lowercase; the parser must not silently normalize.
        let s = "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855";
        assert!(BlobKey::from_hex_str(s).is_err());
    }

    #[test]
    fn blob_key_from_hex_str_rejects_mixed_case_hex() {
        let s = "e3B0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert!(BlobKey::from_hex_str(s).is_err());
    }

    #[test]
    fn blob_key_from_hex_str_rejects_non_hex_at_non_zero_position() {
        // Plant a "z" partway through. Ensures the validator scans the whole string.
        let mut s = "a".repeat(64);
        s.replace_range(40..41, "z");
        assert!(BlobKey::from_hex_str(&s).is_err());
    }

    #[test]
    fn blob_key_from_hex_str_rejects_whitespace() {
        let s = " ".repeat(64);
        assert!(BlobKey::from_hex_str(&s).is_err());
    }

    #[test]
    fn blob_key_from_hex_str_rejects_multibyte_utf8() {
        // 16 four-byte emoji = 64 bytes total. Should fail the hex check, not the
        // length check. (Pins the byte-vs-char semantics of the validator.)
        let s = "\u{1F480}".repeat(16);
        assert_eq!(s.len(), 64);
        assert!(BlobKey::from_hex_str(&s).is_err());
    }
}
