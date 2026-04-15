use philharmonic_store::{BackendError, StoreError};

/// Translate a `sqlx::Error` into a `StoreError::Backend` with a
/// retryability hint.
///
/// This is the catch-all translator for sqlx errors that don't have a
/// specific substrate-level meaning. Call sites that need to distinguish
/// specific MySQL errors (duplicate key, etc.) should inspect the error
/// *before* calling this, and return the appropriate semantic variant
/// (`RevisionConflict`, `IdentityCollision`, etc.) directly.
pub(crate) fn translate(err: sqlx::Error) -> StoreError {
    let retryable = is_retryable(&err);
    StoreError::Backend(BackendError::new(format!("sqlx: {err}"), retryable))
}

/// Whether a `sqlx::Error` represents a transient condition worth retrying.
///
/// Recognizes:
/// * I/O errors (connection dropped, network blip)
/// * Pool exhaustion (`PoolTimedOut`, `PoolClosed`)
/// * MySQL deadlock (error 1213)
/// * MySQL lock-wait timeout (error 1205)
///
/// Everything else is treated as non-retryable.
fn is_retryable(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Io(_) | sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed => true,
        sqlx::Error::Database(db_err) => {
            matches!(db_err.code().as_deref(), Some("1213") | Some("1205"))
        }
        _ => false,
    }
}

/// Whether a `sqlx::Error` is a MySQL duplicate-key violation (error 1062).
///
/// Used by trait implementations to distinguish expected constraint
/// violations (revision conflicts, identity collisions) from unexpected
/// database errors. Call sites inspect this *before* falling through to
/// `translate`, so that the semantic variant reaches the consumer rather
/// than a generic `Backend` error.
///
/// # Example
///
/// ```ignore
/// match result {
///     Ok(_) => Ok(()),
///     Err(e) if is_duplicate_key(&e) => Err(StoreError::RevisionConflict { .. }),
///     Err(e) => Err(translate(e)),
/// }
/// ```
pub(crate) fn is_duplicate_key(err: &sqlx::Error) -> bool {
    let Some(db_err) = err.as_database_error() else {
        return false;
    };

    // mysql/sqlx can expose either vendor code (1062) or SQLSTATE (23000).
    if matches!(db_err.code().as_deref(), Some("1062") | Some("23000")) {
        return true;
    }

    if let Some(mysql_err) = db_err.try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>() {
        return mysql_err.number() == 1062;
    }

    let message = db_err.message();
    message.contains("1062") && message.contains("Duplicate entry")
}
