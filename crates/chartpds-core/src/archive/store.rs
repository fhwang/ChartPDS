//! `Archive` struct wrapping an `object_store` backend.

use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::ObjectStore;

use crate::archive::{compute_blob_key, BlobKey, Result};

/// Content-addressed blob archive backed by an [`ObjectStore`].
///
/// Construct with [`Archive::new`] passing an `Arc<dyn ObjectStore>`. The
/// backend choice (S3, local FS, in-memory) is the caller's responsibility;
/// the archive only sees the trait object.
///
/// `Clone` is a cheap refcount bump on the inner `Arc`; clone freely.
#[derive(Clone)]
pub struct Archive {
    backend: Arc<dyn ObjectStore>,
}

impl std::fmt::Debug for Archive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Archive").finish_non_exhaustive()
    }
}

impl Archive {
    /// Wrap an `object_store` backend in an archive.
    #[must_use]
    pub fn new(backend: Arc<dyn ObjectStore>) -> Self {
        Self { backend }
    }

    /// Write a blob to the archive. Returns its content-addressed key
    /// (the SHA-256 hex of `content`).
    ///
    /// Putting the same content twice is idempotent: the second put overwrites
    /// the existing blob with identical bytes and returns the same key.
    ///
    /// # Errors
    ///
    /// Returns [`crate::archive::Error::Backend`] if the underlying object
    /// store rejects the write.
    pub async fn put(&self, content: Bytes) -> Result<BlobKey> {
        let key = compute_blob_key(&content);
        let path = Path::from(key.as_str());
        self.backend.put(&path, content.into()).await?;
        Ok(key)
    }

    /// Read a blob from the archive by its key.
    ///
    /// # Errors
    ///
    /// - [`crate::archive::Error::NotFound`] if no blob with this key exists.
    /// - [`crate::archive::Error::Backend`] for any other storage failure.
    pub async fn get(&self, key: &BlobKey) -> Result<Bytes> {
        let path = Path::from(key.as_str());
        let result = self.backend.get(&path).await.map_err(|err| {
            if matches!(err, object_store::Error::NotFound { .. }) {
                crate::archive::Error::NotFound {
                    key: key.as_str().to_owned(),
                }
            } else {
                crate::archive::Error::Backend(err)
            }
        })?;
        let bytes = result
            .bytes()
            .await
            .map_err(crate::archive::Error::Backend)?;
        Ok(bytes)
    }

    /// List all blob keys in the archive.
    ///
    /// Returns the keys in no particular order. Paths that do not parse as
    /// valid `BlobKey` hex strings (e.g. directory markers) are silently
    /// skipped.
    ///
    /// # Errors
    ///
    /// Returns [`crate::archive::Error::Backend`] if the storage backend
    /// fails while listing objects.
    pub async fn list_keys(&self) -> Result<Vec<BlobKey>> {
        use futures::TryStreamExt;
        let mut keys = Vec::new();
        let mut stream = self.backend.list(None);
        while let Some(meta) = stream
            .try_next()
            .await
            .map_err(crate::archive::Error::Backend)?
        {
            if let Ok(key) = BlobKey::from_hex_str(meta.location.as_ref()) {
                keys.push(key);
            }
        }
        Ok(keys)
    }

    /// Check whether a blob with the given key is present in the archive.
    ///
    /// # Errors
    ///
    /// Returns [`crate::archive::Error::Backend`] if the storage backend
    /// reports a failure other than "not found."
    pub async fn exists(&self, key: &BlobKey) -> Result<bool> {
        let path = Path::from(key.as_str());
        match self.backend.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(err) => Err(crate::archive::Error::Backend(err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    #[tokio::test]
    async fn archive_can_be_constructed_with_in_memory_backend() {
        let backend = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
        let _archive = Archive::new(backend);
    }

    #[tokio::test]
    async fn put_returns_blob_key_matching_content() {
        let backend = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
        let archive = Archive::new(backend);

        let content = bytes::Bytes::from_static(b"hello");
        let key = archive.put(content).await.expect("put succeeds");

        // Known SHA-256 of "hello".
        assert_eq!(
            key.as_str(),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[tokio::test]
    async fn get_round_trips_content_for_known_key() {
        let backend = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
        let archive = Archive::new(backend);

        let content = Bytes::from_static(b"hello");
        let key = archive.put(content.clone()).await.expect("put succeeds");

        let got = archive.get(&key).await.expect("get succeeds");
        assert_eq!(got, content);
    }

    #[tokio::test]
    async fn get_returns_not_found_for_missing_key() {
        let backend = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
        let archive = Archive::new(backend);

        // A valid-format key that was never put.
        let key = BlobKey::from_hex_str(
            "0000000000000000000000000000000000000000000000000000000000000000",
        )
        .expect("valid format");

        let err = archive
            .get(&key)
            .await
            .expect_err("expected NotFound on missing blob");

        assert!(matches!(err, crate::archive::Error::NotFound { .. }));
    }

    #[tokio::test]
    async fn exists_returns_false_before_put() {
        let backend = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
        let archive = Archive::new(backend);

        let key = BlobKey::from_hex_str(
            "0000000000000000000000000000000000000000000000000000000000000000",
        )
        .expect("valid format");

        assert!(!archive.exists(&key).await.expect("exists succeeds"));
    }

    #[tokio::test]
    async fn exists_returns_true_after_put() {
        let backend = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
        let archive = Archive::new(backend);

        let key = archive
            .put(Bytes::from_static(b"hello"))
            .await
            .expect("put succeeds");

        assert!(archive.exists(&key).await.expect("exists succeeds"));
    }

    #[tokio::test]
    async fn list_keys_returns_all_stored_blob_keys() {
        let backend = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
        let archive = Archive::new(backend);

        let k1 = archive
            .put(Bytes::from_static(b"alpha"))
            .await
            .expect("put");
        let k2 = archive
            .put(Bytes::from_static(b"bravo"))
            .await
            .expect("put");
        let k3 = archive
            .put(Bytes::from_static(b"charlie"))
            .await
            .expect("put");

        let mut keys = archive.list_keys().await.expect("list_keys");
        keys.sort_by(|a, b| a.as_str().cmp(b.as_str()));

        let mut expected = vec![k1, k2, k3];
        expected.sort_by(|a, b| a.as_str().cmp(b.as_str()));

        assert_eq!(keys, expected);
    }

    #[tokio::test]
    async fn list_keys_returns_empty_for_empty_archive() {
        let backend = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
        let archive = Archive::new(backend);

        let keys = archive.list_keys().await.expect("list_keys");
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn put_is_idempotent_for_identical_content() {
        let backend = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
        let archive = Archive::new(backend);

        let content = Bytes::from_static(b"the same content");
        let key1 = archive
            .put(content.clone())
            .await
            .expect("first put succeeds");
        let key2 = archive
            .put(content.clone())
            .await
            .expect("second put succeeds");

        assert_eq!(key1, key2);

        let got = archive.get(&key1).await.expect("get succeeds");
        assert_eq!(got, content);
    }
}
