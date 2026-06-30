# offline_sync_kit

A local-first sync queue for Flutter apps. The package starts with a small,
testable core for durable operations, retry backoff, conflict surfacing, and
UI-friendly status streams.

## Features

- Queue create, update, delete, or custom operations.
- Persist queue records behind a storage interface.
- Send operations through an app-owned transport adapter.
- Retry failed operations with exponential backoff.
- Surface conflicts instead of hiding them.
- Watch per-entity sync status from Flutter UI.

## Getting started

This first version intentionally keeps storage and networking abstract. Use the
in-memory store for tests and prototypes, then add a real adapter for your app's
database.

## Usage

```dart
final store = InMemorySyncStore();
final engine = SyncEngine(
  store: store,
  transport: MyApiSyncTransport(),
);

await engine.enqueue(
  SyncOperation(
    id: 'operation-1',
    entity: const SyncEntityRef(type: 'task', id: 'task-1'),
    type: SyncOperationType.update,
    payload: const {'title': 'Ship the package'},
  ),
);

engine.watchEntity(const SyncEntityRef(type: 'task', id: 'task-1')).listen(
  (record) {
    // pending, syncing, synced, failed, or conflicted
    print(record.status);
  },
);
```

## Roadmap

- Storage adapters for Drift and Hive.
- Conflict resolver helpers.
- Connectivity-aware auto drain.
- Optimistic update helpers.
- Flutter widgets for sync badges and debug views.
