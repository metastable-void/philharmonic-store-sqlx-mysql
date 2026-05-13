use philharmonic_store::{
    ContentStore, EntityRefValue, EntityStore, IdentityStore, RevisionInput, RevisionRef,
    StoreError,
};
use philharmonic_store_sqlx_mysql::{SinglePool, SqlStore, migrate};

use philharmonic_types::{
    CanonicalJson, ContentSlot, ContentValue, Entity, EntitySlot, InternalId, PublicId, ScalarSlot,
    ScalarType, ScalarValue, Sha256, SlotPinning, UnixMillis, Uuid,
};

use sqlx::{MySqlPool, mysql::MySqlPoolOptions};

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dockerlet::{Container, GenericImage, IntoContainerPort, WaitFor};
use tokio::sync::OnceCell;

/// Warm shared MySQL container for the test binary. Started once
/// (~30s); reused by every `#[test]` in this file. Per-test
/// isolation is by unique database name, not by container —
/// dropping that container per test would cost 28+ image-pulls
/// or starts in the worst case.
static SHARED_MYSQL: OnceCell<SharedMysql> = OnceCell::const_new();

struct SharedMysql {
    _container: Container,
    base_url: String,
}

async fn shared_mysql() -> &'static SharedMysql {
    SHARED_MYSQL
        .get_or_init(|| async {
            let container = GenericImage::new("mysql", "8")
                .with_exposed_port(3306.tcp())
                .with_env_var("MYSQL_ALLOW_EMPTY_PASSWORD", "yes")
                .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
                .with_startup_timeout(Duration::from_secs(180))
                .start()
                .await
                .expect("start shared MySQL container");
            let host = container.get_host().await.expect("container host");
            let port = container
                .get_host_port_ipv4(3306.tcp())
                .await
                .expect("container port");
            SharedMysql {
                _container: container,
                base_url: format!("mysql://root@{host}:{port}"),
            }
        })
        .await
}

fn unique_db_name() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("dl_t_{}_{n}", std::process::id())
}

/// Connect to MySQL with exponential backoff over a 30s deadline.
/// The mysql:8 image emits "ready for connections" during internal
/// init before it accepts external connections, so the first few
/// connects after `WaitFor::message_on_stderr` can EOF.
async fn connect_with_retry(url: &str) -> MySqlPool {
    use std::time::Instant;
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut backoff = Duration::from_millis(100);
    let mut last_err = None;
    while Instant::now() < deadline {
        match MySqlPool::connect(url).await {
            Ok(pool) => return pool,
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, Duration::from_secs(2));
            }
        }
    }
    panic!(
        "could not connect to shared MySQL within 30s: {:?}",
        last_err
    );
}

struct TestContext {
    db_name: String,
    pool: MySqlPool,
    store: SqlStore<SinglePool>,
}

impl Drop for TestContext {
    fn drop(&mut self) {
        // Best-effort per-test DB cleanup. The shared container
        // stays warm; only this test's database goes away. Use a
        // dedicated short-lived runtime since Drop can't .await.
        let db_name = self.db_name.clone();
        let base_url = shared_mysql_base_url();
        std::thread::spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
            {
                Ok(rt) => rt,
                Err(_) => return,
            };
            runtime.block_on(async move {
                if let Ok(pool) = MySqlPool::connect(&base_url).await {
                    let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS `{db_name}`"))
                        .execute(&pool)
                        .await;
                }
            });
        });
    }
}

fn shared_mysql_base_url() -> String {
    // Safe sync access: `shared_mysql()` is awaited in every
    // test's `setup()`, so by the time any `Drop` fires the
    // OnceCell is initialised.
    SHARED_MYSQL
        .get()
        .map(|s| s.base_url.clone())
        .unwrap_or_default()
}

struct TestKindA;
impl Entity for TestKindA {
    const KIND: Uuid = Uuid::from_u128(0xAAAA_0000_0000_0000_0000_0000_0000_0001);
    const NAME: &'static str = "test_kind_a";
    const CONTENT_SLOTS: &'static [ContentSlot] = &[ContentSlot::new("body")];
    const ENTITY_SLOTS: &'static [EntitySlot] = &[];
    const SCALAR_SLOTS: &'static [ScalarSlot] = &[
        ScalarSlot::new("is_active", ScalarType::Bool, true),
        ScalarSlot::new("priority", ScalarType::I64, true),
    ];
}

struct TestKindB;
impl Entity for TestKindB {
    const KIND: Uuid = Uuid::from_u128(0xBBBB_0000_0000_0000_0000_0000_0000_0002);
    const NAME: &'static str = "test_kind_b";
    const CONTENT_SLOTS: &'static [ContentSlot] = &[];
    const ENTITY_SLOTS: &'static [EntitySlot] =
        &[EntitySlot::of::<TestKindA>("parent", SlotPinning::Latest)];
    const SCALAR_SLOTS: &'static [ScalarSlot] = &[];
}

async fn setup() -> TestContext {
    let shared = shared_mysql().await;
    let db_name = unique_db_name();

    // Create the per-test database against the daemon's `mysql`
    // system database (always present in mysql:8). The mysql:8
    // image prints "ready for connections" during its internal
    // init before it fully accepts external connections; retry
    // the initial connect over a 30s deadline.
    let admin_url = format!("{}/mysql", shared.base_url);
    let admin_pool = connect_with_retry(&admin_url).await;
    sqlx::query(&format!("CREATE DATABASE `{db_name}`"))
        .execute(&admin_pool)
        .await
        .unwrap();
    drop(admin_pool);

    let database_url = format!("{}/{db_name}", shared.base_url);
    let pool = MySqlPoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&database_url)
        .await
        .unwrap();

    migrate(&pool).await.unwrap();

    let store = SqlStore::from_pool(pool.clone());

    TestContext {
        db_name,
        pool,
        store,
    }
}

fn content(bytes: &[u8]) -> ContentValue {
    let json = CanonicalJson::from_bytes(bytes).unwrap();
    ContentValue::from(&json)
}

// ── schema ───────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn migrate_is_idempotent() {
    let ctx = setup().await;

    migrate(&ctx.pool).await.unwrap();
    migrate(&ctx.pool).await.unwrap();

    let data_type = sqlx::query_scalar::<_, String>(
        "SELECT CAST(DATA_TYPE AS CHAR) FROM INFORMATION_SCHEMA.COLUMNS \
         WHERE TABLE_SCHEMA = DATABASE() \
         AND TABLE_NAME = 'content' \
         AND COLUMN_NAME = 'content_bytes'",
    )
    .fetch_one(&ctx.pool)
    .await
    .unwrap();
    assert_eq!(data_type, "longblob");
}

// ── content store ───────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn content_put_and_get() {
    let ctx = setup().await;

    let value = content(br#"{"hello":"world"}"#);
    ctx.store.put(&value).await.unwrap();

    let got = ctx.store.get(value.digest()).await.unwrap();

    assert!(got.is_some());
    let got = got.unwrap();
    assert_eq!(got.digest(), value.digest());
    assert_eq!(got.bytes(), value.bytes());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn content_put_is_idempotent() {
    let ctx = setup().await;

    let value = content(br#"{"idempotent":true}"#);
    ctx.store.put(&value).await.unwrap();
    ctx.store.put(&value).await.unwrap();

    let got = ctx.store.get(value.digest()).await.unwrap();

    assert!(got.is_some());
    let got = got.unwrap();
    assert_eq!(got.digest(), value.digest());
    assert_eq!(got.bytes(), value.bytes());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn content_get_missing_returns_none() {
    let ctx = setup().await;

    let missing_hash = Sha256::of(b"this hash was never inserted");

    let got = ctx.store.get(missing_hash).await.unwrap();

    assert!(got.is_none());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn content_exists_true_and_false() {
    let ctx = setup().await;

    let value = content(br#"{"exists":"yes"}"#);
    let missing_hash = Sha256::of(b"missing");

    ctx.store.put(&value).await.unwrap();

    let exists = ctx.store.exists(value.digest()).await.unwrap();
    let missing = ctx.store.exists(missing_hash).await.unwrap();

    assert!(exists);
    assert!(!missing);
}

// ── identity store ──────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn identity_mint_and_resolve_public() {
    let ctx = setup().await;

    let identity = ctx.store.mint().await.unwrap();

    let got = ctx.store.resolve_public(identity.public).await.unwrap();

    assert_eq!(got, Some(identity));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn identity_mint_and_resolve_internal() {
    let ctx = setup().await;

    let identity = ctx.store.mint().await.unwrap();

    let got = ctx.store.resolve_internal(identity.internal).await.unwrap();

    assert_eq!(got, Some(identity));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn identity_resolve_unknown_returns_none() {
    let ctx = setup().await;

    let missing_public = Uuid::new_v4();
    let missing_internal = Uuid::now_v7();

    let got_public = ctx.store.resolve_public(missing_public).await.unwrap();
    let got_internal = ctx.store.resolve_internal(missing_internal).await.unwrap();

    assert_eq!(got_public, None);
    assert_eq!(got_internal, None);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn identity_mint_produces_valid_versions() {
    let ctx = setup().await;

    let identity = ctx.store.mint().await.unwrap();

    assert_eq!(identity.internal.get_version_num(), 7);
    assert_eq!(identity.public.get_version_num(), 4);
    assert!(InternalId::<TestKindA>::from_uuid(identity.internal).is_ok());
    assert!(PublicId::<TestKindA>::from_uuid(identity.public).is_ok());
}

// ── entity store: creation and reads ────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn entity_create_and_get() {
    let ctx = setup().await;

    let identity = ctx.store.mint().await.unwrap();
    let test_started_at = UnixMillis::now().as_i64();

    ctx.store
        .create_entity(identity, TestKindA::KIND)
        .await
        .unwrap();

    let got = ctx.store.get_entity(identity.internal).await.unwrap();

    assert!(got.is_some());
    let got = got.unwrap();
    assert_eq!(got.identity, identity);
    assert_eq!(got.kind, TestKindA::KIND);
    assert!((test_started_at - got.created_at.as_i64()).abs() < 5000);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn entity_get_missing_returns_none() {
    let ctx = setup().await;

    let missing_id = Uuid::now_v7();

    let got = ctx.store.get_entity(missing_id).await.unwrap();

    assert!(got.is_none());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn entity_create_duplicate_returns_collision() {
    let ctx = setup().await;

    let identity = ctx.store.mint().await.unwrap();

    ctx.store
        .create_entity(identity, TestKindA::KIND)
        .await
        .unwrap();

    let result = ctx.store.create_entity(identity, TestKindA::KIND).await;

    match result {
        Err(StoreError::IdentityCollision { uuid }) => {
            assert_eq!(uuid, identity.internal);
        }
        other => panic!("expected IdentityCollision, got {other:?}"),
    }
}

// ── entity store: revisions ─────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn revision_append_and_get() {
    let ctx = setup().await;

    let target_identity = ctx.store.mint().await.unwrap();
    ctx.store
        .create_entity(target_identity, TestKindA::KIND)
        .await
        .unwrap();

    let identity = ctx.store.mint().await.unwrap();
    ctx.store
        .create_entity(identity, TestKindB::KIND)
        .await
        .unwrap();

    let body = content(br#"{"revision":0}"#);
    ctx.store.put(&body).await.unwrap();

    let input = RevisionInput::new()
        .with_content("body", body.digest())
        .with_entity("parent", EntityRefValue::latest(target_identity.internal))
        .with_scalar("priority", ScalarValue::I64(42));

    ctx.store
        .append_revision(identity.internal, 0, &input)
        .await
        .unwrap();

    let got = ctx.store.get_revision(identity.internal, 0).await.unwrap();

    assert!(got.is_some());
    let got = got.unwrap();
    assert_eq!(got.entity_id, identity.internal);
    assert_eq!(got.revision_seq, 0);
    assert_eq!(got.content_attrs.get("body"), Some(&body.digest()));
    assert_eq!(
        got.entity_attrs.get("parent"),
        Some(&EntityRefValue::latest(target_identity.internal))
    );
    assert_eq!(
        got.scalar_attrs.get("priority"),
        Some(&ScalarValue::I64(42))
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn revision_append_multiple() {
    let ctx = setup().await;

    let identity = ctx.store.mint().await.unwrap();
    ctx.store
        .create_entity(identity, TestKindA::KIND)
        .await
        .unwrap();

    for seq in 0..=2_u64 {
        let input = RevisionInput::new().with_scalar("priority", ScalarValue::I64(seq as i64));
        ctx.store
            .append_revision(identity.internal, seq, &input)
            .await
            .unwrap();
    }

    let rev0 = ctx.store.get_revision(identity.internal, 0).await.unwrap();
    let rev1 = ctx.store.get_revision(identity.internal, 1).await.unwrap();
    let rev2 = ctx.store.get_revision(identity.internal, 2).await.unwrap();

    assert_eq!(
        rev0.unwrap().scalar_attrs.get("priority"),
        Some(&ScalarValue::I64(0))
    );
    assert_eq!(
        rev1.unwrap().scalar_attrs.get("priority"),
        Some(&ScalarValue::I64(1))
    );
    assert_eq!(
        rev2.unwrap().scalar_attrs.get("priority"),
        Some(&ScalarValue::I64(2))
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn revision_get_latest() {
    let ctx = setup().await;

    let identity = ctx.store.mint().await.unwrap();
    ctx.store
        .create_entity(identity, TestKindA::KIND)
        .await
        .unwrap();

    let rev0 = RevisionInput::new().with_scalar("priority", ScalarValue::I64(10));
    let rev1 = RevisionInput::new().with_scalar("priority", ScalarValue::I64(11));

    ctx.store
        .append_revision(identity.internal, 0, &rev0)
        .await
        .unwrap();
    ctx.store
        .append_revision(identity.internal, 1, &rev1)
        .await
        .unwrap();

    let latest = ctx
        .store
        .get_latest_revision(identity.internal)
        .await
        .unwrap();

    assert!(latest.is_some());
    let latest = latest.unwrap();
    assert_eq!(latest.revision_seq, 1);
    assert_eq!(
        latest.scalar_attrs.get("priority"),
        Some(&ScalarValue::I64(11))
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn revision_get_latest_no_revisions() {
    let ctx = setup().await;

    let identity = ctx.store.mint().await.unwrap();
    ctx.store
        .create_entity(identity, TestKindA::KIND)
        .await
        .unwrap();

    let latest = ctx
        .store
        .get_latest_revision(identity.internal)
        .await
        .unwrap();

    assert!(latest.is_none());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn revision_conflict_on_duplicate_seq() {
    let ctx = setup().await;

    let identity = ctx.store.mint().await.unwrap();
    ctx.store
        .create_entity(identity, TestKindA::KIND)
        .await
        .unwrap();

    let rev = RevisionInput::new().with_scalar("priority", ScalarValue::I64(1));
    ctx.store
        .append_revision(identity.internal, 0, &rev)
        .await
        .unwrap();

    let result = ctx.store.append_revision(identity.internal, 0, &rev).await;

    match result {
        Err(StoreError::RevisionConflict {
            entity_id,
            revision_seq,
        }) => {
            assert_eq!(entity_id, identity.internal);
            assert_eq!(revision_seq, 0);
        }
        other => panic!("expected RevisionConflict, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn revision_append_missing_entity() {
    let ctx = setup().await;

    let missing_entity_id = Uuid::now_v7();
    let input = RevisionInput::new().with_scalar("priority", ScalarValue::I64(5));

    let result = ctx
        .store
        .append_revision(missing_entity_id, 0, &input)
        .await;

    match result {
        Err(StoreError::EntityNotFound { entity_id }) => {
            assert_eq!(entity_id, missing_entity_id);
        }
        other => panic!("expected EntityNotFound, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn revision_content_attrs_round_trip() {
    let ctx = setup().await;

    let identity = ctx.store.mint().await.unwrap();
    ctx.store
        .create_entity(identity, TestKindA::KIND)
        .await
        .unwrap();

    let body = content(br#"{"body":"value"}"#);
    let metadata = content(br#"{"meta":123}"#);
    ctx.store.put(&body).await.unwrap();
    ctx.store.put(&metadata).await.unwrap();

    let input = RevisionInput::new()
        .with_content("body", body.digest())
        .with_content("metadata", metadata.digest());

    ctx.store
        .append_revision(identity.internal, 0, &input)
        .await
        .unwrap();

    let got = ctx
        .store
        .get_revision(identity.internal, 0)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(got.content_attrs.len(), 2);
    assert_eq!(got.content_attrs.get("body"), Some(&body.digest()));
    assert_eq!(got.content_attrs.get("metadata"), Some(&metadata.digest()));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn revision_entity_attrs_pinned_and_latest() {
    let ctx = setup().await;

    let target_identity = ctx.store.mint().await.unwrap();
    ctx.store
        .create_entity(target_identity, TestKindA::KIND)
        .await
        .unwrap();

    let identity = ctx.store.mint().await.unwrap();
    ctx.store
        .create_entity(identity, TestKindB::KIND)
        .await
        .unwrap();

    let input = RevisionInput::new()
        .with_entity(
            "parent_pinned",
            EntityRefValue::pinned(target_identity.internal, 3),
        )
        .with_entity(
            "parent_latest",
            EntityRefValue::latest(target_identity.internal),
        );

    ctx.store
        .append_revision(identity.internal, 0, &input)
        .await
        .unwrap();

    let got = ctx
        .store
        .get_revision(identity.internal, 0)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        got.entity_attrs.get("parent_pinned"),
        Some(&EntityRefValue {
            target_entity_id: target_identity.internal,
            target_revision_seq: Some(3),
        })
    );
    assert_eq!(
        got.entity_attrs.get("parent_latest"),
        Some(&EntityRefValue {
            target_entity_id: target_identity.internal,
            target_revision_seq: None,
        })
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn revision_scalar_bool_and_i64() {
    let ctx = setup().await;

    let identity = ctx.store.mint().await.unwrap();
    ctx.store
        .create_entity(identity, TestKindA::KIND)
        .await
        .unwrap();

    let input = RevisionInput::new()
        .with_scalar("is_active", ScalarValue::Bool(true))
        .with_scalar("priority", ScalarValue::I64(99));

    ctx.store
        .append_revision(identity.internal, 0, &input)
        .await
        .unwrap();

    let got = ctx
        .store
        .get_revision(identity.internal, 0)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        got.scalar_attrs.get("is_active"),
        Some(&ScalarValue::Bool(true))
    );
    assert_eq!(
        got.scalar_attrs.get("priority"),
        Some(&ScalarValue::I64(99))
    );
}

// ── entity store: queries ───────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn find_by_scalar_bool() {
    let ctx = setup().await;

    let active_identity = ctx.store.mint().await.unwrap();
    let inactive_identity = ctx.store.mint().await.unwrap();

    ctx.store
        .create_entity(active_identity, TestKindA::KIND)
        .await
        .unwrap();
    ctx.store
        .create_entity(inactive_identity, TestKindA::KIND)
        .await
        .unwrap();

    let active_rev = RevisionInput::new().with_scalar("is_active", ScalarValue::Bool(true));
    let inactive_rev = RevisionInput::new().with_scalar("is_active", ScalarValue::Bool(false));

    ctx.store
        .append_revision(active_identity.internal, 0, &active_rev)
        .await
        .unwrap();
    ctx.store
        .append_revision(inactive_identity.internal, 0, &inactive_rev)
        .await
        .unwrap();

    let rows = ctx
        .store
        .find_by_scalar(TestKindA::KIND, "is_active", &ScalarValue::Bool(true))
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].identity.internal, active_identity.internal);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn find_by_scalar_i64() {
    let ctx = setup().await;

    let low_identity = ctx.store.mint().await.unwrap();
    let high_identity = ctx.store.mint().await.unwrap();

    ctx.store
        .create_entity(low_identity, TestKindA::KIND)
        .await
        .unwrap();
    ctx.store
        .create_entity(high_identity, TestKindA::KIND)
        .await
        .unwrap();

    let low_rev = RevisionInput::new().with_scalar("priority", ScalarValue::I64(1));
    let high_rev = RevisionInput::new().with_scalar("priority", ScalarValue::I64(10));

    ctx.store
        .append_revision(low_identity.internal, 0, &low_rev)
        .await
        .unwrap();
    ctx.store
        .append_revision(high_identity.internal, 0, &high_rev)
        .await
        .unwrap();

    let rows = ctx
        .store
        .find_by_scalar(TestKindA::KIND, "priority", &ScalarValue::I64(10))
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].identity.internal, high_identity.internal);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn find_by_scalar_wrong_kind_returns_empty() {
    let ctx = setup().await;

    let identity = ctx.store.mint().await.unwrap();
    ctx.store
        .create_entity(identity, TestKindA::KIND)
        .await
        .unwrap();

    let rev = RevisionInput::new().with_scalar("is_active", ScalarValue::Bool(true));
    ctx.store
        .append_revision(identity.internal, 0, &rev)
        .await
        .unwrap();

    let rows = ctx
        .store
        .find_by_scalar(TestKindB::KIND, "is_active", &ScalarValue::Bool(true))
        .await
        .unwrap();

    assert!(rows.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn find_by_scalar_uses_latest_revision() {
    let ctx = setup().await;

    let identity = ctx.store.mint().await.unwrap();
    ctx.store
        .create_entity(identity, TestKindA::KIND)
        .await
        .unwrap();

    let rev0 = RevisionInput::new().with_scalar("is_active", ScalarValue::Bool(true));
    let rev1 = RevisionInput::new().with_scalar("is_active", ScalarValue::Bool(false));

    ctx.store
        .append_revision(identity.internal, 0, &rev0)
        .await
        .unwrap();
    ctx.store
        .append_revision(identity.internal, 1, &rev1)
        .await
        .unwrap();

    let true_rows = ctx
        .store
        .find_by_scalar(TestKindA::KIND, "is_active", &ScalarValue::Bool(true))
        .await
        .unwrap();
    let false_rows = ctx
        .store
        .find_by_scalar(TestKindA::KIND, "is_active", &ScalarValue::Bool(false))
        .await
        .unwrap();

    assert!(true_rows.is_empty());
    assert_eq!(false_rows.len(), 1);
    assert_eq!(false_rows[0].identity.internal, identity.internal);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn list_revisions_referencing() {
    let ctx = setup().await;

    let target = ctx.store.mint().await.unwrap();
    ctx.store
        .create_entity(target, TestKindA::KIND)
        .await
        .unwrap();

    let source = ctx.store.mint().await.unwrap();
    ctx.store
        .create_entity(source, TestKindB::KIND)
        .await
        .unwrap();

    let input = RevisionInput::new().with_entity("parent", EntityRefValue::latest(target.internal));
    ctx.store
        .append_revision(source.internal, 0, &input)
        .await
        .unwrap();

    let refs = ctx
        .store
        .list_revisions_referencing(target.internal, "parent")
        .await
        .unwrap();

    assert_eq!(refs, vec![RevisionRef::new(source.internal, 0)]);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn list_revisions_referencing_wrong_attr_returns_empty() {
    let ctx = setup().await;

    let target = ctx.store.mint().await.unwrap();
    ctx.store
        .create_entity(target, TestKindA::KIND)
        .await
        .unwrap();

    let source = ctx.store.mint().await.unwrap();
    ctx.store
        .create_entity(source, TestKindB::KIND)
        .await
        .unwrap();

    let input = RevisionInput::new().with_entity("parent", EntityRefValue::latest(target.internal));
    ctx.store
        .append_revision(source.internal, 0, &input)
        .await
        .unwrap();

    let refs = ctx
        .store
        .list_revisions_referencing(target.internal, "other")
        .await
        .unwrap();

    assert_eq!(refs, vec![]);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn list_revisions_referencing_no_refs_returns_empty() {
    let ctx = setup().await;

    let target = ctx.store.mint().await.unwrap();
    ctx.store
        .create_entity(target, TestKindA::KIND)
        .await
        .unwrap();

    let refs = ctx
        .store
        .list_revisions_referencing(target.internal, "parent")
        .await
        .unwrap();

    assert_eq!(refs, vec![]);
}
