use philharmonic_store::{
    EntityRefValue, EntityRow, EntityStore, RevisionInput, RevisionRef, RevisionRow, StoreError,
};
use philharmonic_types::{Identity, ScalarValue, Sha256, UnixMillis, Uuid};

use sqlx::{Acquire, MySqlConnection};

use async_trait::async_trait;

use std::collections::HashMap;

use crate::codec;
use crate::connection::ConnectionProvider;
use crate::error;
use crate::store::SqlStore;

#[async_trait]
impl<P: ConnectionProvider> EntityStore for SqlStore<P> {
    async fn create_entity(&self, identity: Identity, kind: Uuid) -> Result<(), StoreError> {
        let id_bytes = codec::uuid_to_binary(identity.internal);
        let kind_bytes = codec::uuid_to_binary(kind);
        let now = UnixMillis::now().as_i64();

        let mut conn = self.conns.acquire_write().await?;
        let result = sqlx::query("INSERT INTO entity (id, kind, created_at) VALUES (?, ?, ?)")
            .bind(&id_bytes[..])
            .bind(&kind_bytes[..])
            .bind(now)
            .execute(&mut *conn)
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) if error::is_duplicate_key(&e) => Err(StoreError::IdentityCollision {
                uuid: identity.internal,
            }),
            Err(e) => Err(error::translate(e)),
        }
    }

    async fn get_entity(&self, entity_id: Uuid) -> Result<Option<EntityRow>, StoreError> {
        let id_bytes = codec::uuid_to_binary(entity_id);
        let mut conn = self.conns.acquire_read().await?;

        let row: Option<(Vec<u8>, Vec<u8>, Vec<u8>, i64)> = sqlx::query_as(
            "SELECT e.id, i.public, e.kind, e.created_at \
             FROM entity e \
             JOIN identity i ON i.internal = e.id \
             WHERE e.id = ?",
        )
        .bind(&id_bytes[..])
        .fetch_optional(&mut *conn)
        .await
        .map_err(error::translate)?;

        match row {
            None => Ok(None),
            Some((internal_bytes, public_bytes, kind_bytes, created_at)) => {
                let internal = codec::binary_to_uuid(&internal_bytes)?;
                let public = codec::binary_to_uuid(&public_bytes)?;
                let kind = codec::binary_to_uuid(&kind_bytes)?;
                Ok(Some(EntityRow {
                    identity: Identity { internal, public },
                    kind,
                    created_at: UnixMillis(created_at),
                }))
            }
        }
    }

    async fn append_revision(
        &self,
        entity_id: Uuid,
        revision_seq: u64,
        input: &RevisionInput,
    ) -> Result<(), StoreError> {
        let id_bytes = codec::uuid_to_binary(entity_id);
        let now = UnixMillis::now().as_i64();

        let mut conn = self.conns.acquire_write().await?;
        let mut tx = conn.begin().await.map_err(error::translate)?;

        // Verify entity exists. Without declared FKs, this is the only
        // check that prevents orphaned revisions.
        let exists: Option<(i32,)> = sqlx::query_as("SELECT 1 FROM entity WHERE id = ? LIMIT 1")
            .bind(&id_bytes[..])
            .fetch_optional(&mut *tx)
            .await
            .map_err(error::translate)?;

        if exists.is_none() {
            return Err(StoreError::EntityNotFound { entity_id });
        }

        // Insert the revision row.
        let rev_result = sqlx::query(
            "INSERT INTO entity_revision (entity_id, revision_seq, created_at) \
             VALUES (?, ?, ?)",
        )
        .bind(&id_bytes[..])
        .bind(revision_seq)
        .bind(now)
        .execute(&mut *tx)
        .await;

        match rev_result {
            Ok(_) => {}
            Err(e) if error::is_duplicate_key(&e) => {
                return Err(StoreError::RevisionConflict {
                    entity_id,
                    revision_seq,
                });
            }
            Err(e) => return Err(error::translate(e)),
        }

        // Insert content-hash attributes.
        for (name, hash) in &input.content_attrs {
            let hash_bytes = codec::sha256_to_binary(*hash);
            sqlx::query(
                "INSERT INTO attribute_content \
                 (entity_id, revision_seq, attribute_name, content_hash) \
                 VALUES (?, ?, ?, ?)",
            )
            .bind(&id_bytes[..])
            .bind(revision_seq)
            .bind(name.as_str())
            .bind(&hash_bytes[..])
            .execute(&mut *tx)
            .await
            .map_err(error::translate)?;
        }

        // Insert entity-reference attributes.
        for (name, ref_value) in &input.entity_attrs {
            let target_bytes = codec::uuid_to_binary(ref_value.target_entity_id);
            sqlx::query(
                "INSERT INTO attribute_entity \
                 (entity_id, revision_seq, attribute_name, \
                  target_entity_id, target_revision_seq) \
                 VALUES (?, ?, ?, ?, ?)",
            )
            .bind(&id_bytes[..])
            .bind(revision_seq)
            .bind(name.as_str())
            .bind(&target_bytes[..])
            .bind(ref_value.target_revision_seq)
            .execute(&mut *tx)
            .await
            .map_err(error::translate)?;
        }

        // Insert scalar attributes.
        for (name, value) in &input.scalar_attrs {
            let (kind, val_bool, val_i64) = codec::scalar_to_cols(value);
            sqlx::query(
                "INSERT INTO attribute_scalar \
                 (entity_id, revision_seq, attribute_name, \
                  value_kind, value_bool, value_i64) \
                 VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(&id_bytes[..])
            .bind(revision_seq)
            .bind(name.as_str())
            .bind(kind)
            .bind(val_bool)
            .bind(val_i64)
            .execute(&mut *tx)
            .await
            .map_err(error::translate)?;
        }

        tx.commit().await.map_err(error::translate)?;
        Ok(())
    }

    async fn get_revision(
        &self,
        entity_id: Uuid,
        revision_seq: u64,
    ) -> Result<Option<RevisionRow>, StoreError> {
        let id_bytes = codec::uuid_to_binary(entity_id);
        let mut conn = self.conns.acquire_read().await?;

        // Fetch the revision header.
        let header: Option<(i64,)> = sqlx::query_as(
            "SELECT created_at FROM entity_revision \
             WHERE entity_id = ? AND revision_seq = ?",
        )
        .bind(&id_bytes[..])
        .bind(revision_seq)
        .fetch_optional(&mut *conn)
        .await
        .map_err(error::translate)?;

        let Some((created_at,)) = header else {
            return Ok(None);
        };

        // Fetch the three attribute sets.
        let content_attrs = fetch_content_attrs(&mut *conn, &id_bytes, revision_seq).await?;
        let entity_attrs = fetch_entity_attrs(&mut *conn, &id_bytes, revision_seq).await?;
        let scalar_attrs = fetch_scalar_attrs(&mut *conn, &id_bytes, revision_seq).await?;

        Ok(Some(RevisionRow {
            entity_id,
            revision_seq,
            created_at: UnixMillis(created_at),
            content_attrs,
            entity_attrs,
            scalar_attrs,
        }))
    }

    async fn get_latest_revision(
        &self,
        entity_id: Uuid,
    ) -> Result<Option<RevisionRow>, StoreError> {
        let id_bytes = codec::uuid_to_binary(entity_id);
        let mut conn = self.conns.acquire_read().await?;

        let latest: Option<(u64,)> = sqlx::query_as(
            "SELECT MAX(revision_seq) FROM entity_revision \
             WHERE entity_id = ?",
        )
        .bind(&id_bytes[..])
        .fetch_optional(&mut *conn)
        .await
        .map_err(error::translate)?;

        // MAX returns NULL (mapped to None by fetch_optional) when there
        // are no rows. sqlx maps NULL inside a found row to None via
        // Option<u64> in the tuple, so we also handle that case.
        let seq = match latest {
            Some((seq,)) => seq,
            None => return Ok(None),
        };

        // Reuse the connection we already hold rather than acquiring
        // a new one. Fetch header + attributes inline.
        let header: Option<(i64,)> = sqlx::query_as(
            "SELECT created_at FROM entity_revision \
             WHERE entity_id = ? AND revision_seq = ?",
        )
        .bind(&id_bytes[..])
        .bind(seq)
        .fetch_optional(&mut *conn)
        .await
        .map_err(error::translate)?;

        let Some((created_at,)) = header else {
            // Between the MAX query and the header fetch, the row can't
            // have disappeared (append-only). If it did, something is
            // very wrong — return None and let the caller handle it.
            return Ok(None);
        };

        let content_attrs = fetch_content_attrs(&mut *conn, &id_bytes, seq).await?;
        let entity_attrs = fetch_entity_attrs(&mut *conn, &id_bytes, seq).await?;
        let scalar_attrs = fetch_scalar_attrs(&mut *conn, &id_bytes, seq).await?;

        Ok(Some(RevisionRow {
            entity_id,
            revision_seq: seq,
            created_at: UnixMillis(created_at),
            content_attrs,
            entity_attrs,
            scalar_attrs,
        }))
    }

    async fn list_revisions_referencing(
        &self,
        target_entity_id: Uuid,
        attribute_name: &str,
    ) -> Result<Vec<RevisionRef>, StoreError> {
        let target_bytes = codec::uuid_to_binary(target_entity_id);
        let mut conn = self.conns.acquire_read().await?;

        let rows: Vec<(Vec<u8>, u64)> = sqlx::query_as(
            "SELECT entity_id, revision_seq FROM attribute_entity \
             WHERE target_entity_id = ? AND attribute_name = ?",
        )
        .bind(&target_bytes[..])
        .bind(attribute_name)
        .fetch_all(&mut *conn)
        .await
        .map_err(error::translate)?;

        rows.into_iter()
            .map(|(id_bytes, seq)| {
                let entity_id = codec::binary_to_uuid(&id_bytes)?;
                Ok(RevisionRef::new(entity_id, seq))
            })
            .collect()
    }

    async fn find_by_scalar(
        &self,
        kind: Uuid,
        attribute_name: &str,
        value: &ScalarValue,
    ) -> Result<Vec<EntityRow>, StoreError> {
        let kind_bytes = codec::uuid_to_binary(kind);

        // Build the query based on scalar type. The WHERE clause targets
        // the type-specific value column, which uses the corresponding
        // index (ix_attr_scalar_bool or ix_attr_scalar_i64).
        let (sql, bind_value): (&str, i64) = match value {
            ScalarValue::Bool(b) => (
                "SELECT e.id, i.public, e.kind, e.created_at \
                 FROM entity e \
                 JOIN identity i ON i.internal = e.id \
                 JOIN attribute_scalar a ON a.entity_id = e.id \
                 WHERE e.kind = ? \
                   AND a.attribute_name = ? \
                   AND a.value_bool = ? \
                   AND a.revision_seq = (\
                       SELECT MAX(r.revision_seq) \
                       FROM entity_revision r \
                       WHERE r.entity_id = e.id\
                   )",
                *b as i64,
            ),
            ScalarValue::I64(n) => (
                "SELECT e.id, i.public, e.kind, e.created_at \
                 FROM entity e \
                 JOIN identity i ON i.internal = e.id \
                 JOIN attribute_scalar a ON a.entity_id = e.id \
                 WHERE e.kind = ? \
                   AND a.attribute_name = ? \
                   AND a.value_i64 = ? \
                   AND a.revision_seq = (\
                       SELECT MAX(r.revision_seq) \
                       FROM entity_revision r \
                       WHERE r.entity_id = e.id\
                   )",
                *n,
            ),
        };

        let mut conn = self.conns.acquire_read().await?;
        let rows: Vec<(Vec<u8>, Vec<u8>, Vec<u8>, i64)> = sqlx::query_as(sql)
            .bind(&kind_bytes[..])
            .bind(attribute_name)
            .bind(bind_value)
            .fetch_all(&mut *conn)
            .await
            .map_err(error::translate)?;

        rows.into_iter()
            .map(|(id_bytes, public_bytes, kind_bytes, created_at)| {
                let internal = codec::binary_to_uuid(&id_bytes)?;
                let public = codec::binary_to_uuid(&public_bytes)?;
                let kind = codec::binary_to_uuid(&kind_bytes)?;
                Ok(EntityRow {
                    identity: Identity { internal, public },
                    kind,
                    created_at: UnixMillis(created_at),
                })
            })
            .collect()
    }
}

// ── attribute fetch helpers ─────────────────────────────────────────

async fn fetch_content_attrs(
    conn: &mut MySqlConnection,
    entity_id_bytes: &[u8],
    revision_seq: u64,
) -> Result<HashMap<String, Sha256>, StoreError> {
    let rows: Vec<(String, Vec<u8>)> = sqlx::query_as(
        "SELECT attribute_name, content_hash FROM attribute_content \
         WHERE entity_id = ? AND revision_seq = ?",
    )
    .bind(entity_id_bytes)
    .bind(revision_seq)
    .fetch_all(conn)
    .await
    .map_err(error::translate)?;

    rows.into_iter()
        .map(|(name, hash_bytes)| {
            let hash = codec::binary_to_sha256(&hash_bytes)?;
            Ok((name, hash))
        })
        .collect()
}

async fn fetch_entity_attrs(
    conn: &mut MySqlConnection,
    entity_id_bytes: &[u8],
    revision_seq: u64,
) -> Result<HashMap<String, EntityRefValue>, StoreError> {
    let rows: Vec<(String, Vec<u8>, Option<u64>)> = sqlx::query_as(
        "SELECT attribute_name, target_entity_id, target_revision_seq \
         FROM attribute_entity \
         WHERE entity_id = ? AND revision_seq = ?",
    )
    .bind(entity_id_bytes)
    .bind(revision_seq)
    .fetch_all(conn)
    .await
    .map_err(error::translate)?;

    rows.into_iter()
        .map(|(name, target_bytes, target_seq)| {
            let target = codec::binary_to_uuid(&target_bytes)?;
            Ok((
                name,
                EntityRefValue {
                    target_entity_id: target,
                    target_revision_seq: target_seq,
                },
            ))
        })
        .collect()
}

async fn fetch_scalar_attrs(
    conn: &mut MySqlConnection,
    entity_id_bytes: &[u8],
    revision_seq: u64,
) -> Result<HashMap<String, ScalarValue>, StoreError> {
    let rows: Vec<(String, u8, Option<u8>, Option<i64>)> = sqlx::query_as(
        "SELECT attribute_name, value_kind, value_bool, value_i64 \
         FROM attribute_scalar \
         WHERE entity_id = ? AND revision_seq = ?",
    )
    .bind(entity_id_bytes)
    .bind(revision_seq)
    .fetch_all(conn)
    .await
    .map_err(error::translate)?;

    rows.into_iter()
        .map(|(name, kind, val_bool, val_i64)| {
            let value = codec::cols_to_scalar(&name, kind, val_bool, val_i64)?;
            Ok((name, value))
        })
        .collect()
}
