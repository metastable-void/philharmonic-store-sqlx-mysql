//! MySQL-family storage backend for the Philharmonic substrate.
//!
//! This crate implements the three substrate traits
//! ([`ContentStore`], [`IdentityStore`], [`EntityStore`]) from
//! [`philharmonic_store`] by issuing SQL against a MySQL-compatible
//! database through [`sqlx`].
//!
//! # Targets
//!
//! The SQL uses only features common to MySQL 8, MariaDB 10.5+,
//! Amazon Aurora MySQL, and TiDB. No JSON columns, no vendor-specific
//! operators, no declared foreign keys. Timestamps are `BIGINT`
//! millis-since-epoch, UUIDs are `BINARY(16)`, hashes are `BINARY(32)`.
//!
//! # Getting started
//!
//! ```ignore
//! use philharmonic_store_sqlx_mysql::{SqlStore, migrate};
//! use sqlx::MySqlPool;
//!
//! let pool = MySqlPool::connect("mysql://root@localhost/philharmonic").await?;
//! migrate(&pool).await?;
//! let store = SqlStore::from_pool(pool);
//!
//! // `store` now implements ContentStore, IdentityStore, and EntityStore.
//! ```
//!
//! # Custom connection providers
//!
//! The default [`SinglePool`] routes all reads and writes through one
//! pool. Deployments that need read/write splitting, sharded routing,
//! or other connection-management strategies implement
//! [`ConnectionProvider`] and pass the custom provider to
//! [`SqlStore::new`].
//!
//! [`ContentStore`]: philharmonic_store::ContentStore
//! [`IdentityStore`]: philharmonic_store::IdentityStore
//! [`EntityStore`]: philharmonic_store::EntityStore

mod codec;
mod connection;
mod content;
mod entity;
mod error;
mod identity;
mod schema;
mod store;

pub use connection::{ConnectionProvider, SinglePool};
pub use schema::migrate;
pub use store::SqlStore;
