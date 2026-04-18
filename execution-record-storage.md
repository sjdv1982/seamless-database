# Execution Record Storage in `seamless-database`

## Summary

Implement storage for post-transformation execution records in the existing database backend only. The current normal cache and irreproducible cache behavior stay in place; the work is to make `MetaData` store canonical execution-record bodies, expose them through the remote database API, and preserve them when normal entries move to `IrreproducibleTransformation`.

Execution capture, worker-side environment probing, content-addressed environment sub-buffers, and audit attempt tracking are out of scope for this implementation.

## Key Changes

- Update `seamless-database/database_models.py`:
  - Change `MetaData` to `checksum` PK, `result` indexed checksum, and `metadata` JSON.
  - Remove `MetaData` from the base-model primary-key upsert fallback so records cannot be silently overwritten.
  - Add guarded schema initialization for `meta_data`: create fresh if absent, drop/recreate only if the old two-column table is empty, preserve upgraded tables, and fail loudly if an incompatible non-empty table exists.

- Extend `seamless-database/database.py`:
  - Keep request type `metadata`.
  - `PUT metadata` accepts `checksum`, `result`, and `value` where `value` is the canonical execution-record JSON body.
  - Validate identity only: checksum syntax, integer `schema_version`, body `tf_checksum` matching request `checksum`, body `result_checksum` matching request `result`, and sane `checksum_fields` format if present.
  - Make `PUT metadata` atomically create `Transformation`, `RevTransformation`, and `MetaData` when the normal result row is missing.
  - If a normal result already exists with the same result, store missing metadata or treat an identical metadata record as idempotent success.
  - Reject mismatched result rows, differing duplicate metadata, malformed bodies, and attempts to recreate a normal entry after irreproducible rows already exist for that `tf_checksum`.
  - Add `GET irreproducible` returning a list of rows by `tf_checksum`, optionally filtered by `result`, with each row including `checksum`, `result`, and `metadata`.
  - Update `PUT irreproducible` to verify any `MetaData.result` matches the moved result, move the record body unchanged, and perform insert/delete work transactionally.
  - Fix PUT handling so `_put()` can return explicit HTTP responses, allowing conflict/error statuses to be preserved.
  - Bump the database protocol version to `2.1` and update README/API notes.

- Extend remote client APIs in `seamless-remote/seamless_remote/database_client.py` and `seamless-remote/seamless_remote/database_remote.py`:
  - Add `set_execution_record(tf_checksum, result_checksum, record)`.
  - Add `get_execution_record(tf_checksum)`.
  - Add `get_irreproducible_records(tf_checksum, result_checksum=None)`.
  - Keep existing `set_transformation_result()` and `undo_transformation_result()` behavior compatible for callers that do not yet produce execution records.

## Test Plan

- Add database model/schema tests:
  - Fresh database creates upgraded `meta_data`.
  - Empty old two-column `meta_data` is dropped and recreated.
  - Non-empty old-layout `meta_data` fails loudly.
  - Existing upgraded `meta_data` rows survive repeated `db_init()` calls.

- Add database API tests:
  - `PUT metadata` auto-creates normal `Transformation` and `RevTransformation` rows.
  - `GET metadata` returns the stored canonical record body.
  - Identical duplicate `PUT metadata` succeeds idempotently.
  - Differing duplicate records and result mismatches are rejected.
  - Malformed checksum/body identity is rejected.
  - `PUT irreproducible` moves metadata unchanged and removes the normal rows.
  - `GET irreproducible` returns all stored rows for a transformation.

- Add remote client tests:
  - `set_execution_record()` and `get_execution_record()` round-trip through a local database server.
  - `get_irreproducible_records()` returns list-shaped results after a move.

- Run at minimum:
  - `pytest seamless-database/tests`
  - `pytest seamless-remote/tests` for any added client tests

## Assumptions

- Execution records are supplied by a future producer; this implementation does not capture hardware, Python packages, conda environments, runtime config, or metavars.
- The database validates record identity and storage consistency, not the full environment-record schema.
- Once a transformation has irreproducible rows, normal metadata writes for that `tf_checksum` are rejected to avoid silently migrating it back into the normal cache.
- The canonical record body remains self-contained with `tf_checksum` and `result_checksum`; database columns are lookup/layout conveniences.
