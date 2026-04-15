use philharmonic_store::{IdentityStore, StoreError};
use philharmonic_types::{Identity, Uuid};

use async_trait::async_trait;

use crate::codec;
use crate::connection::ConnectionProvider;
use crate::error;
use crate::store::SqlStore;

#[async_trait]
impl<P: ConnectionProvider> IdentityStore for SqlStore<P> {
    async fn mint(&self) -> Result<Identity, StoreError> {
        let internal = Uuid::now_v7();
        let public = Uuid::new_v4();
        let internal_bytes = codec::uuid_to_binary(internal);
        let public_bytes = codec::uuid_to_binary(public);

        let mut conn = self.conns.acquire_write().await?;
        let result = sqlx::query("INSERT INTO identity (internal, public) VALUES (?, ?)")
            .bind(&internal_bytes[..])
            .bind(&public_bytes[..])
            .execute(&mut *conn)
            .await;

        match result {
            Ok(_) => Ok(Identity { internal, public }),
            Err(e) if error::is_duplicate_key(&e) => {
                Err(StoreError::IdentityCollision { uuid: internal })
            }
            Err(e) => Err(error::translate(e)),
        }
    }

    async fn resolve_public(&self, public: Uuid) -> Result<Option<Identity>, StoreError> {
        let public_bytes = codec::uuid_to_binary(public);
        let mut conn = self.conns.acquire_read().await?;
        let row: Option<(Vec<u8>, Vec<u8>)> =
            sqlx::query_as("SELECT internal, public FROM identity WHERE public = ?")
                .bind(&public_bytes[..])
                .fetch_optional(&mut *conn)
                .await
                .map_err(error::translate)?;

        match row {
            None => Ok(None),
            Some((internal_bytes, public_bytes)) => Ok(Some(Identity {
                internal: codec::binary_to_uuid(&internal_bytes)?,
                public: codec::binary_to_uuid(&public_bytes)?,
            })),
        }
    }

    async fn resolve_internal(&self, internal: Uuid) -> Result<Option<Identity>, StoreError> {
        let internal_bytes = codec::uuid_to_binary(internal);
        let mut conn = self.conns.acquire_read().await?;
        let row: Option<(Vec<u8>, Vec<u8>)> =
            sqlx::query_as("SELECT internal, public FROM identity WHERE internal = ?")
                .bind(&internal_bytes[..])
                .fetch_optional(&mut *conn)
                .await
                .map_err(error::translate)?;

        match row {
            None => Ok(None),
            Some((internal_bytes, public_bytes)) => Ok(Some(Identity {
                internal: codec::binary_to_uuid(&internal_bytes)?,
                public: codec::binary_to_uuid(&public_bytes)?,
            })),
        }
    }
}
