# Changelog

All notable changes to this crate are documented in this file.

The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this crate adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.5] - 2026-05-14

### Changed
- Internal Cargo.toml audit: `default-features = false` set on
  direct dependencies with explicit feature lists for what the
  crate actually uses. No behaviour change. (D24)

## [0.1.4] - 2026-05-13

- Dev: migrate integration-test fixtures from `testcontainers` /
  `testcontainers-modules` to `dockerlet 0.1`. No public-API
  change; runtime behaviour unchanged.

## [0.1.3]

- Changed substrate `content.content_bytes` from `MEDIUMBLOB` to `LONGBLOB` for fresh schemas and startup migration.

## [0.1.2]

- Fixed schema migration `is_duplicate_key_name` check: was matching
  SQLSTATE instead of message text.

## [0.1.1]

- Bump `philharmonic-types` pin `0.3.4` → `0.3.5`. Picks up
  the `Sha256` human-readable-aware serde (JSON encoding
  unchanged; CBOR now emits a 32-byte byte string).

## [0.1.0]

Current published baseline. Git history is the authoritative
record for this and earlier releases; future releases will be
documented going forward in this file.
