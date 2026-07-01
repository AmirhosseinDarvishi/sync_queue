# sync_queue

A local-first sync queue for Flutter apps. The package starts with a small,
testable core for durable operations, retry backoff, conflict surfacing, and
UI-friendly status streams.

## Features

- Queue create, update, delete, or custom operations.
- Use create, update, and delete enqueue helpers for common mutations.
- Reject duplicate operation ids before they overwrite queued work.
- Keep only the latest pending mutation for noisy edit flows.
- Update pending operations before they are sent.
- Pair optimistic local changes with queue commit rollback.
- Persist queue records behind a storage interface.
- Send operations through an app-owned transport adapter.
- Bound transport sends with an optional timeout.
- Retry failed operations with exponential backoff.
- Add jitter to retry delays to avoid retry bursts.
- Honor transport-provided retry delays.
- Automatically retry due operations on schedule.
- Schedule retry timers only for runnable queued work.
- Read the next runnable retry time for custom schedulers.
- Inspect drain summaries after each sync pass.
- Limit drain passes for batched background sync.
- Preserve operation order for each entity during drain passes.
- Watch engine lifecycle state for sync spinners and debug panels.
- Coalesce full drain requests that arrive while another drain is active.
- Drain one due operation without draining unrelated work.
- Drain due work for a specific entity.
- Recover interrupted syncing operations after app restarts.
- Pause automatic draining while offline.
- Drain pending operations when connectivity returns.
- Serialize operations and records for durable stores.
- Adapt any JSON-capable persistence layer through `JsonSyncStore`.
- Let JSON storage adapters provide optimized pending queries.
- Surface conflicts instead of hiding them.
- Retry failed operations after user action.
- Retry all failed operations or only failed work for one entity.
- Discard all failed operations or only failed work for one entity.
- Discard queued operations before they are sent.
- Discard pending work for a specific entity.
- Watch per-entity sync status from Flutter UI.
- Inspect queued records for a specific entity.
- Filter queued records by entity and lifecycle status.
- Read aggregated entity sync state for badges and status rows.
- Read queue-wide snapshots for global indicators and debug panels.
- Watch combined sync state for app bars, badges, and debug panels.

## Getting Started

`sync_queue` intentionally keeps storage and networking abstract. Your app owns
the API client, local database, and connectivity source; the package owns queue
state, retry timing, conflict surfacing, and UI-friendly snapshots.

Use `InMemorySyncStore` for tests and prototypes, then replace it with
`JsonSyncStore` or your own `SyncStore` adapter for production persistence.

See `example/main.dart` for a complete fake API flow with offline queuing,
optimistic updates, failed-operation retry, and conflict resolution.

## Quick Start

```dart
final store = InMemorySyncStore();
final connectivity = ManualSyncConnectivity();
final engine = SyncEngine(
  store: store,
  transport: MyApiSyncTransport(),
  connectivity: connectivity,
  retryPolicy: const RetryPolicy(jitterFactor: 0.2),
  sendTimeout: const Duration(seconds: 30),
);

await engine.enqueueUpdate(
  entity: const SyncEntityRef(type: 'task', id: 'task-2'),
  payload: const {'title': 'Ship the package'},
);
```

## Transport Adapter

Implement `SyncTransport` with your API client. Return success when the server
accepts the operation, failure when it should retry or stop, and conflict when
your app needs a merge decision.

```dart
class TaskSyncTransport implements SyncTransport {
  TaskSyncTransport(this.api);

  final TaskApi api;

  @override
  Future<SyncResult> send(SyncOperation operation) async {
    try {
      await api.sendTaskMutation(
        id: operation.entity.id,
        type: operation.type.wireName,
        payload: operation.payload,
      );
      return const SyncResult.success();
    } on VersionConflict catch (error) {
      return SyncResult.conflict(
        message: 'Server version changed.',
        local: operation.payload,
        remote: error.remotePayload,
      );
    } on RateLimited catch (error) {
      return SyncResult.failure(
        SyncFailure(
          message: 'Rate limited.',
          code: 'rate_limited',
          retryAfter: error.retryAfter,
        ),
      );
    }
  }
}
```

## Storage Adapter

Use `JsonSyncStore` when your database can store maps, JSON blobs, or encoded
records. Implement `SyncJsonQueryStorage` if your database can efficiently query
due pending records.

```dart
final engine = SyncEngine(
  store: JsonSyncStore(MyJsonStorage(database)),
  transport: TaskSyncTransport(api),
);
```

For tests, `InMemorySyncStore` is usually enough:

```dart
final engine = SyncEngine(
  store: InMemorySyncStore(),
  transport: FakeSyncTransport(),
);
```

## Queue Mutations

Operation ids must be unique while they are in the queue. Duplicate ids are
rejected before they can replace existing work.

```dart
await engine.enqueueCreate(
  entity: const SyncEntityRef(type: 'task', id: 'task-1'),
  payload: const {'title': 'New task'},
);

await engine.enqueueLatestMutation(
  entity: const SyncEntityRef(type: 'task', id: 'task-1'),
  type: SyncOperationType.update,
  payload: const {'title': 'Only the latest pending update stays queued'},
);

await SyncOptimistic.run(
  apply: () => updateLocalTaskTitle('task-1', 'Optimistic title'),
  commit: () => engine.enqueueUpdate(
    entity: const SyncEntityRef(type: 'task', id: 'task-1'),
    payload: const {'title': 'Optimistic title'},
  ),
  rollback: (_, _) => updateLocalTaskTitle('task-1', 'Previous title'),
);
```

## Draining

Operations for the same entity are drained in creation order. If an older
operation is failed, conflicted, syncing, or waiting for a future retry, newer
operations for that entity wait behind it while unrelated entities can continue.

```dart
final drain = await engine.drain();
final batch = await engine.drain(maxOperations: 25);

if (batch.shouldContinue) {
  scheduleAnotherSyncPass();
}

await engine.drainEntity(
  const SyncEntityRef(type: 'task', id: 'task-1'),
);

await engine.drainOperation('operation-1');

await engine.recoverInterruptedOperations(
  staleAfter: const Duration(minutes: 5),
);

final nextRetryAt = await engine.readNextRetryAt();
```

## UI State

```dart
engine.watchSyncState().listen((state) {
  if (state.isSyncing) {
    showSyncSpinner();
  }

  if (state.needsAttention) {
    showSyncIssueBadge();
  }
});

final taskRecords = await engine.readEntityRecords(
  const SyncEntityRef(type: 'task', id: 'task-1'),
);

final attentionRecords = await engine.readRecords(
  statuses: {SyncStatus.failed, SyncStatus.conflicted},
);
```

## Failures and Conflicts

```dart
await engine.retryFailedOperations(
  entity: const SyncEntityRef(type: 'task', id: 'task-1'),
);

await engine.discardFailedOperations(
  entity: const SyncEntityRef(type: 'task', id: 'task-1'),
);

await engine.resolveConflict(
  'operation-1',
  const SyncConflictResolution.retry(
    payload: {'title': 'Merged title'},
  ),
);
```

## Roadmap

- Optional Drift and Hive adapter packages.
- More conflict merge utilities for app-specific strategies.
- Flutter widgets for sync badges and debug views.
