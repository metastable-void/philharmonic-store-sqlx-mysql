use philharmonic_types::{ScalarValue, Sha256, Uuid};

use philharmonic_store::StoreError;

/// Decode a `BINARY(16)` column value into a `Uuid`.
///
/// MySQL returns `BINARY(16)` columns as `Vec<u8>` through sqlx. This
/// function validates the length and converts. The resulting `Uuid` is
/// not version-checked here — version validation (v4 vs. v7) happens
/// at the `Identity::typed` boundary in consumer code.
pub(crate) fn binary_to_uuid(bytes: &[u8]) -> Result<Uuid, StoreError> {
    let arr: [u8; 16] = bytes.try_into().map_err(|_| {
        StoreError::Backend(philharmonic_store::BackendError::fatal(format!(
            "expected 16 bytes for UUID, got {}",
            bytes.len(),
        )))
    })?;
    Ok(Uuid::from_bytes(arr))
}

/// Encode a `Uuid` as `BINARY(16)` bytes for binding to a query parameter.
///
/// Returns a fixed-size array rather than a slice so that callers can
/// bind it directly without lifetime concerns.
pub(crate) fn uuid_to_binary(uuid: Uuid) -> [u8; 16] {
    *uuid.as_bytes()
}

/// Decode a `BINARY(32)` column value into a `Sha256`.
///
/// Same shape as `binary_to_uuid` but for 32-byte hash digests.
pub(crate) fn binary_to_sha256(bytes: &[u8]) -> Result<Sha256, StoreError> {
    let arr: [u8; 32] = bytes.try_into().map_err(|_| {
        StoreError::Backend(philharmonic_store::BackendError::fatal(format!(
            "expected 32 bytes for SHA-256, got {}",
            bytes.len(),
        )))
    })?;
    Ok(Sha256::from_bytes_unchecked(arr))
}

/// Encode a `Sha256` as `BINARY(32)` bytes for binding to a query parameter.
pub(crate) fn sha256_to_binary(hash: Sha256) -> [u8; 32] {
    *hash.as_bytes()
}

/// Column discriminator values for `attribute_scalar.value_kind`.
///
/// These are the on-disk encoding of `ScalarType` variants. They must
/// never change once written; adding new scalar types means adding new
/// discriminator values, not reusing existing ones.
const VALUE_KIND_BOOL: u8 = 0;
const VALUE_KIND_I64: u8 = 1;

/// Decompose a `ScalarValue` into column values for `attribute_scalar`.
///
/// Returns `(value_kind, value_bool, value_i64)` matching the three
/// non-PK columns of the `attribute_scalar` table. Exactly one of
/// `value_bool` / `value_i64` is `Some`; the other is `None`.
pub(crate) fn scalar_to_cols(value: &ScalarValue) -> (u8, Option<u8>, Option<i64>) {
    match value {
        ScalarValue::Bool(b) => (VALUE_KIND_BOOL, Some(*b as u8), None),
        ScalarValue::I64(n) => (VALUE_KIND_I64, None, Some(*n)),
    }
}

/// Reconstruct a `ScalarValue` from column values read from `attribute_scalar`.
///
/// Inverse of `scalar_to_cols`. Returns `StoreError::ScalarTypeMismatch`
/// if the discriminator is unknown or the expected value column is `NULL`
/// when it shouldn't be.
pub(crate) fn cols_to_scalar(
    attribute_name: &str,
    value_kind: u8,
    value_bool: Option<u8>,
    value_i64: Option<i64>,
) -> Result<ScalarValue, StoreError> {
    match value_kind {
        VALUE_KIND_BOOL => {
            let raw = value_bool.ok_or_else(|| StoreError::ScalarTypeMismatch {
                attribute_name: attribute_name.to_owned(),
                detail: "value_kind=0 (bool) but value_bool is NULL".to_owned(),
            })?;
            Ok(ScalarValue::Bool(raw != 0))
        }
        VALUE_KIND_I64 => {
            let n = value_i64.ok_or_else(|| StoreError::ScalarTypeMismatch {
                attribute_name: attribute_name.to_owned(),
                detail: "value_kind=1 (i64) but value_i64 is NULL".to_owned(),
            })?;
            Ok(ScalarValue::I64(n))
        }
        other => Err(StoreError::ScalarTypeMismatch {
            attribute_name: attribute_name.to_owned(),
            detail: format!("unknown value_kind discriminator: {other}"),
        }),
    }
}
