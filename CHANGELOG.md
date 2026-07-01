## Unreleased

* Add a JSON-backed sync store adapter.
* Add engine-generated operation ids for common enqueue flows.
* Add aggregated entity sync state helpers for UI.
* Add conflict resolution helpers for retry, discard, and fail decisions.
* Add automatic retry scheduling for delayed operations.
* Add queue-wide snapshot helpers for global sync indicators.
* Add optimized pending-query support for JSON storage adapters.
* Add drain result summaries with processed outcome counts.
* Add manual retry support for failed operations.
* Add manual discard support for queued operations.
* Add latest-mutation enqueueing for pending operation coalescing.
* Add transport-provided retry delay support for failures.
* Add jittered retry backoff support.
* Add entity-scoped pending discard support.
* Add entity record inspection helpers.
* Add entity-scoped drain support.
* Add operation-scoped drain support.
* Add full-drain reruns for drain requests received during active drains.
* Add pending operation update support.

## 0.1.0-dev.1

* Add the initial sync queue core.
* Add operation, record, result, retry, store, and transport APIs.
* Add an in-memory store for tests and examples.
* Add connectivity-aware drain control.
* Add JSON serialization for queue records and operations.
