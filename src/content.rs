use philharmonic_store::{ContentStore, StoreError};
use philharmonic_types::{ContentValue, Sha256};

use async_trait::async_trait;

use crate::codec;
use crate::connection::ConnectionProvider;
use crate::error;
use crate::store::SqlStore;

#[async_trait]
impl<P: ConnectionProvider> ContentStore for SqlStore<P> {
    async fn put(&self, value: &ContentValue) -> Result<(), StoreError> {
        let hash = codec::sha256_to_binary(value.digest());
        let mut conn = self.conns.acquire_write().await?;
        sqlx::query("INSERT IGNORE INTO content (content_hash, content_bytes) VALUES (?, ?)")
            .bind(&hash[..])
            .bind(value.bytes())
            .execute(&mut *conn)
            .await
            .map_err(error::translate)?;
        Ok(())
    }

    async fn get(&self, hash: Sha256) -> Result<Option<ContentValue>, StoreError> {
        let hash_bytes = codec::sha256_to_binary(hash);
        let mut conn = self.conns.acquire_read().await?;
        let row: Option<(Vec<u8>,)> =
            sqlx::query_as("SELECT content_bytes FROM content WHERE content_hash = ?")
                .bind(&hash_bytes[..])
                .fetch_optional(&mut *conn)
                .await
                .map_err(error::translate)?;
        Ok(row.map(|(bytes,)| ContentValue::from_parts_unchecked(hash, bytes)))
    }

    async fn exists(&self, hash: Sha256) -> Result<bool, StoreError> {
        let hash_bytes = codec::sha256_to_binary(hash);
        let mut conn = self.conns.acquire_read().await?;
        let row: Option<(i32,)> =
            sqlx::query_as("SELECT 1 FROM content WHERE content_hash = ? LIMIT 1")
                .bind(&hash_bytes[..])
                .fetch_optional(&mut *conn)
                .await
                .map_err(error::translate)?;
        Ok(row.is_some())
    }
}
