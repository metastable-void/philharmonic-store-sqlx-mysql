use crate::connection::{ConnectionProvider, SinglePool};

use sqlx::MySqlPool;

/// The MySQL-family storage backend for the Philharmonic substrate.
///
/// `SqlStore` implements the three substrate traits ([`ContentStore`],
/// [`IdentityStore`], [`EntityStore`]) by issuing SQL against a
/// MySQL-compatible database through a [`ConnectionProvider`].
///
/// The `P` parameter determines how connections are acquired. The default
/// is [`SinglePool`], which draws both reads and writes from one
/// `MySqlPool`. Deployments that need read/write splitting, sharded
/// routing, or other connection-management strategies provide a custom
/// `ConnectionProvider` implementation.
///
/// # Construction
///
/// ```ignore
/// // Simple: single pool, most deployments.
/// let pool = MySqlPool::connect(&url).await?;
/// let store = SqlStore::from_pool(pool);
///
/// // Custom provider: read/write split, anycast, etc.
/// let provider = MyCustomProvider::new(/* ... */);
/// let store = SqlStore::new(provider);
/// ```
///
/// # Schema
///
/// `SqlStore` expects the substrate schema to exist before any operations
/// are called. Use [`migrate`](crate::migrate) to create the schema:
///
/// ```ignore
/// let provider = SinglePool::new(pool);
/// migrate(provider.pool()).await?;
/// let store = SqlStore::new(provider);
/// ```
///
/// [`ContentStore`]: philharmonic_store::ContentStore
/// [`IdentityStore`]: philharmonic_store::IdentityStore
/// [`EntityStore`]: philharmonic_store::EntityStore
pub struct SqlStore<P: ConnectionProvider = SinglePool> {
    pub(crate) conns: P,
}

impl<P: ConnectionProvider> SqlStore<P> {
    /// Construct a `SqlStore` with the given connection provider.
    pub fn new(conns: P) -> Self {
        Self { conns }
    }
}

impl SqlStore<SinglePool> {
    /// Construct a `SqlStore` backed by a single `MySqlPool`.
    ///
    /// Convenience for the common case. Equivalent to
    /// `SqlStore::new(SinglePool::new(pool))`.
    pub fn from_pool(pool: MySqlPool) -> Self {
        Self::new(SinglePool::new(pool))
    }
}
