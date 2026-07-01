# sync_queue

A local-first sync queue for Flutter apps. The package starts with a small,
testable core for durable operations, retry backoff, conflict surfacing, and
UI-friendly status streams.

## Features

- Queue create, update, delete, or custom operations.
- Keep only the latest pending mutation for noisy edit flows.
- Persist queue records behind a storage interface.
- Send operations through an app-owned transport adapter.
- Retry failed operations with exponential backoff.
- Add jitter to retry delays to avoid retry bursts.
- Honor transport-provided retry delays.
- Automatically retry due operations on schedule.
- Inspect drain summaries after each sync pass.
- Pause automatic draining while offline.
- Drain pending operations when connectivity returns.
- Serialize operations and records for durable stores.
- Adapt any JSON-capable persistence layer through `JsonSyncStore`.
- Let JSON storage adapters provide optimized pending queries.
- Surface conflicts instead of hiding them.
- Retry failed operations after user action.
- Discard queued operations before they are sent.
- Watch per-entity sync status from Flutter UI.
- Read aggregated entity sync state for badges and status rows.
- Read queue-wide snapshots for global indicators and debug panels.

## Getting started

This first version intentionally keeps storage and networking abstract. Use the
in-memory store for tests and prototypes, then add a real adapter for your app's
database.

## Usage

```dart
final store = InMemorySyncStore();
final connectivity = ManualSyncConnectivity();
final engine = SyncEngine(
  store: store,
  transport: MyApiSyncTransport(),
  connectivity: connectivity,
  retryPolicy: const RetryPolicy(jitterFactor: 0.2),
);

await engine.enqueue(
  SyncOperation(
    id: 'operation-1',
    entity: const SyncEntityRef(type: 'task', id: 'task-1'),
    type: SyncOperationType.update,
    payload: const {'title': 'Ship the package'},
  ),
);

await engine.enqueueMutation(
  entity: const SyncEntityRef(type: 'task', id: 'task-2'),
  type: SyncOperationType.update,
  payload: const {'title': 'Generated operation id'},
);

await engine.enqueueLatestMutation(
  entity: const SyncEntityRef(type: 'task', id: 'task-2'),
  type: SyncOperationType.update,
  payload: const {'title': 'Only the latest pending update stays queued'},
);

final drain = await engine.drain();
print(drain.succeededCount);

engine.watchEntity(const SyncEntityRef(type: 'task', id: 'task-1')).listen(
  (record) {
    // pending, syncing, synced, failed, or conflicted
    print(record.status);
  },
);

engine.watchEntityState(const SyncEntityRef(type: 'task', id: 'task-1')).listen(
  (state) => print(state.status),
);

engine.watchQueueSnapshot().listen(
  (snapshot) => print(snapshot.status),
);

await engine.resolveConflict(
  'operation-1',
  const SyncConflictResolution.retry(
    payload: {'title': 'Merged title'},
  ),
);

await engine.retryFailedOperation(
  'operation-1',
  payload: {'title': 'Try again'},
);

await engine.discardOperation('operation-1');

final encoded = (await store.readAll()).map((record) => record.toJson());
```

## Roadmap

- Storage adapters for Drift and Hive.
- Conflict resolver helpers.
- Optimistic update helpers.
- Flutter widgets for sync badges and debug views.
