use philharmonic_store::StoreError;

use sqlx::{MySql, MySqlPool, pool::PoolConnection};

use async_trait::async_trait;

use crate::error;

/// A source of database connections for [`SqlStore`](crate::SqlStore).
///
/// The substrate's SQL implementation acquires connections through this
/// trait rather than holding a pool directly, so that deployments can
/// swap connection-management strategies without touching the trait
/// implementations.
///
/// The trait distinguishes read and write connections. The default
/// implementation ([`SinglePool`]) routes both to the same pool; a
/// deployment with read replicas or write-routed clustering can provide
/// an implementation that routes them differently.
///
/// # Consistency contract
///
/// Implementations must guarantee read-your-own-writes within a single
/// `SqlStore` instance: if a write connection commits a row, a subsequent
/// read connection acquired from the same provider must observe that row.
/// Deployments using read replicas with replication lag must ensure
/// session affinity or equivalent to honor this contract.
///
/// This contract is what makes the substrate's optimistic-concurrency
/// pattern (read latest revision, append next, retry on conflict) correct.
/// Violating it produces phantom conflicts or missed revisions.
#[async_trait]
pub trait ConnectionProvider: Send + Sync {
    /// Acquire a connection for a read-only query.
    async fn acquire_read(&self) -> Result<PoolConnection<MySql>, StoreError>;

    /// Acquire a connection for a read-write operation.
    async fn acquire_write(&self) -> Result<PoolConnection<MySql>, StoreError>;
}

/// A [`ConnectionProvider`] backed by a single `MySqlPool`.
///
/// Both `acquire_read` and `acquire_write` draw from the same pool.
/// This is the right choice for single-endpoint deployments (one primary,
/// no read replicas) and for development/testing where simplicity matters
/// more than read-scaling.
///
/// Connection pool sizing, timeouts, and TLS are configured on the
/// `MySqlPool` at construction time (via `MySqlPoolOptions` or
/// connection-string parameters); `SinglePool` does not add its own
/// configuration layer.
pub struct SinglePool {
    pool: MySqlPool,
}

impl SinglePool {
    pub fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }

    /// Access the underlying pool.
    ///
    /// Useful for passing to [`migrate`](crate::migrate), which takes
    /// a `&MySqlPool` rather than a `&dyn ConnectionProvider`, because
    /// schema migration is an operational concern that doesn't go through
    /// the substrate's read/write routing.
    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }
}

#[async_trait]
impl ConnectionProvider for SinglePool {
    async fn acquire_read(&self) -> Result<PoolConnection<MySql>, StoreError> {
        self.pool.acquire().await.map_err(error::translate)
    }

    async fn acquire_write(&self) -> Result<PoolConnection<MySql>, StoreError> {
        self.pool.acquire().await.map_err(error::translate)
    }
}
