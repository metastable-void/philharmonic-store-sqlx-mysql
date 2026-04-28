use philharmonic_store::StoreError;

use sqlx::MySqlPool;

use crate::error;

const CREATE_IDENTITY: &str = "\
CREATE TABLE IF NOT EXISTS identity (
    internal BINARY(16) NOT NULL,
    public   BINARY(16) NOT NULL,
    PRIMARY KEY (internal),
    UNIQUE KEY uk_identity_public (public)
) ENGINE=InnoDB";

const CREATE_CONTENT: &str = "\
CREATE TABLE IF NOT EXISTS content (
    content_hash  BINARY(32) NOT NULL,
    content_bytes MEDIUMBLOB NOT NULL,
    PRIMARY KEY (content_hash)
) ENGINE=InnoDB";

const CREATE_ENTITY: &str = "\
CREATE TABLE IF NOT EXISTS entity (
    id         BINARY(16) NOT NULL,
    kind       BINARY(16) NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (id),
    KEY ix_entity_kind (kind)
) ENGINE=InnoDB";

const CREATE_ENTITY_REVISION: &str = "\
CREATE TABLE IF NOT EXISTS entity_revision (
    entity_id    BINARY(16) NOT NULL,
    revision_seq BIGINT UNSIGNED NOT NULL,
    created_at   BIGINT NOT NULL,
    PRIMARY KEY (entity_id, revision_seq)
) ENGINE=InnoDB";

const CREATE_ATTRIBUTE_CONTENT: &str = "\
CREATE TABLE IF NOT EXISTS attribute_content (
    entity_id      BINARY(16) NOT NULL,
    revision_seq   BIGINT UNSIGNED NOT NULL,
    attribute_name VARCHAR(64) NOT NULL,
    content_hash   BINARY(32) NOT NULL,
    PRIMARY KEY (entity_id, revision_seq, attribute_name),
    KEY ix_attr_content_hash (attribute_name, content_hash)
) ENGINE=InnoDB";

const CREATE_ATTRIBUTE_ENTITY: &str = "\
CREATE TABLE IF NOT EXISTS attribute_entity (
    entity_id           BINARY(16) NOT NULL,
    revision_seq        BIGINT UNSIGNED NOT NULL,
    attribute_name      VARCHAR(64) NOT NULL,
    target_entity_id    BINARY(16) NOT NULL,
    target_revision_seq BIGINT UNSIGNED NULL,
    PRIMARY KEY (entity_id, revision_seq, attribute_name),
    KEY ix_attr_entity_target (target_entity_id, attribute_name)
) ENGINE=InnoDB";

const CREATE_ATTRIBUTE_SCALAR: &str = "\
CREATE TABLE IF NOT EXISTS attribute_scalar (
    entity_id      BINARY(16) NOT NULL,
    revision_seq   BIGINT UNSIGNED NOT NULL,
    attribute_name VARCHAR(64) NOT NULL,
    value_kind     TINYINT UNSIGNED NOT NULL,
    value_bool     TINYINT UNSIGNED NULL,
    value_i64      BIGINT NULL,
    PRIMARY KEY (entity_id, revision_seq, attribute_name),
    KEY ix_attr_scalar_bool (attribute_name, value_bool),
    KEY ix_attr_scalar_i64 (attribute_name, value_i64)
) ENGINE=InnoDB";

/// All DDL statements in dependency order.
///
/// `identity` and `content` have no dependencies and come first.
/// `entity` depends on `identity` conceptually (writer enforces, no FK).
/// `entity_revision` depends on `entity`. The three attribute tables
/// depend on `entity_revision`. Order matters only because a human
/// reading the migration output expects to see parent tables created
/// before child tables.
const DDL: &[&str] = &[
    CREATE_IDENTITY,
    CREATE_CONTENT,
    CREATE_ENTITY,
    CREATE_ENTITY_REVISION,
    CREATE_ATTRIBUTE_CONTENT,
    CREATE_ATTRIBUTE_ENTITY,
    CREATE_ATTRIBUTE_SCALAR,
];

/// Create the substrate schema if it doesn't already exist.
///
/// Runs each `CREATE TABLE IF NOT EXISTS` statement in order against
/// the provided pool. Idempotent: safe to call on every application
/// startup without checking whether the schema already exists.
///
/// Takes a raw `&MySqlPool` rather than a `ConnectionProvider` because
/// schema migration is an operational step that happens before the
/// store is constructed, and routing it through `acquire_read` /
/// `acquire_write` would be semantically wrong (DDL is neither a
/// normal read nor a normal write).
///
/// # Example
///
/// ```ignore
/// let pool = MySqlPool::connect(&url).await?;
/// migrate(&pool).await?;
/// let store = SqlStore::from_pool(pool);
/// ```
///
/// # Errors
///
/// Returns `StoreError::Backend` if any DDL statement fails. Common
/// causes: insufficient privileges (`CREATE` grant missing), connection
/// loss mid-migration, or syntax incompatibilities on non-MySQL backends
/// that don't support `ENGINE=InnoDB`.
pub async fn migrate(pool: &MySqlPool) -> Result<(), StoreError> {
    for ddl in DDL {
        sqlx::query(ddl)
            .execute(pool)
            .await
            .map_err(error::translate)?;
    }
    Ok(())
}
