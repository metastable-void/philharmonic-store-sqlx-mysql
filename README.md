# philharmonic-store-sqlx-mysql

MySQL-family storage backend for the [Philharmonic][philharmonic]
workflow orchestration system, implementing the substrate traits
defined in [`philharmonic-store`][store].

This crate provides [`SqlStore`], which implements `ContentStore`,
`IdentityStore`, and `EntityStore` by issuing SQL against a
MySQL-compatible database through [`sqlx`][sqlx].

## Targets

The SQL uses only features common to all of these:

- MySQL 8
- MariaDB 10.5+
- Amazon Aurora MySQL
- TiDB

No JSON columns, no vendor-specific operators, no declared foreign keys.
Timestamps are `BIGINT` millis-since-epoch, UUIDs are `BINARY(16)`,
hashes are `BINARY(32)`. The writer enforces referential integrity;
the schema declares no FK constraints, for broadest compatibility
(TiDB's FK support varies by version).

## Usage

```rust,ignore
use philharmonic_store::{ContentStore, ContentStoreExt, StoreExt};
use philharmonic_store_sqlx_mysql::{SqlStore, migrate};
use sqlx::MySqlPool;

// 1. Connect and migrate.
let pool = MySqlPool::connect("mysql://root@localhost/philharmonic").await?;
migrate(&pool).await?;

// 2. Construct the store.
let store = SqlStore::from_pool(pool);

// 3. Use the substrate traits.
let hash = store.put_typed(&my_canonical_json).await?;
let id = store.create_entity_minting::<MyEntityKind>().await?;
```

`migrate` is idempotent — safe to call on every application startup.
It runs `CREATE TABLE IF NOT EXISTS` for the seven substrate tables
and returns.

## Connection providers

The default `SinglePool` routes reads and writes through one
`MySqlPool`. Deployments that need read/write splitting or custom
routing implement the `ConnectionProvider` trait and pass the provider
to `SqlStore::new`:

```rust,ignore
let provider = MyReadWriteSplitProvider::new(writer_pool, reader_pool);
let store = SqlStore::new(provider);
```

Providers must guarantee read-your-own-writes within a single
`SqlStore` instance. The substrate's optimistic-concurrency pattern
(read latest revision, append next, retry on conflict) depends on
this property.

## Schema

Seven tables, created by `migrate`:

- `identity` — `(internal, public)` UUID pairs.
- `content` — content-addressed blobs keyed by SHA-256 hash.
- `entity` — entity registry with kind UUID and creation timestamp.
- `entity_revision` — append-only revision log per entity.
- `attribute_content` — content-hash attributes per revision.
- `attribute_entity` — entity-reference attributes per revision.
- `attribute_scalar` — scalar attributes (bool, i64) per revision.

All tables use `InnoDB`. No foreign keys are declared. Primary keys
provide optimistic concurrency for revision appends — duplicate
`(entity_id, revision_seq)` inserts fail cleanly as
`StoreError::RevisionConflict`.

## Status

Early development. Tracks `philharmonic-store` 0.1.x for trait
compatibility. Breaking changes in 0.x releases are possible; semver
guarantees begin at 1.0.

[philharmonic]: https://github.com/metastable-void/philharmonic
[store]: https://crates.io/crates/philharmonic-store
[sqlx]: https://crates.io/crates/sqlx

## License

**This crate is dual-licensed under `Apache-2.0 OR MPL-2.0`**;
either license is sufficient; choose whichever fits your project.

**Rationale**: We generally want our reusable Rust crates to be
under a license permissive enough to be friendly for the Rust
community as a whole, while maintaining GPL-2.0 compatibility via
the MPL-2.0 arm. This is FSF-safer for everyone than `MIT OR Apache-2.0`,
still being permissive. **This is the standard licensing** for our reusable
Rust crate projects. Someone's `GPL-2.0-or-later` project should not be
forced to drop the `GPL-2.0` option because of our crates,
while `Apache-2.0` is the non-copyleft (permissive) license recommended
by the FSF, which we base our decisions on.

## Contributing

This crate is developed as a submodule of the Philharmonic
workspace. Workspace-wide development conventions — git workflow,
script wrappers, Rust code rules, versioning, terminology — live
in the workspace meta-repo at
[metastable-void/philharmonic-workspace](https://github.com/metastable-void/philharmonic-workspace),
authoritatively in its
[`CONTRIBUTING.md`](https://github.com/metastable-void/philharmonic-workspace/blob/main/CONTRIBUTING.md).
