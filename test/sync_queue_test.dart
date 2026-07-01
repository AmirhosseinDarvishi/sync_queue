import 'dart:async';

import 'package:flutter_test/flutter_test.dart';
import 'package:sync_queue/sync_queue.dart';

class FakeTransport implements SyncTransport {
  FakeTransport(this.handler);

  final Future<SyncResult> Function(SyncOperation operation) handler;
  final sent = <SyncOperation>[];

  @override
  Future<SyncResult> send(SyncOperation operation) async {
    sent.add(operation);
    return handler(operation);
  }
}

class FakeTimer implements Timer {
  FakeTimer(this.duration, this._callback);

  final Duration duration;
  final void Function() _callback;
  var _isActive = true;
  var _tick = 0;

  @override
  bool get isActive => _isActive;

  @override
  int get tick => _tick;

  @override
  void cancel() {
    _isActive = false;
  }

  void fire() {
    if (!_isActive) {
      return;
    }

    _isActive = false;
    _tick += 1;
    _callback();
  }
}

class FakeTimerFactory {
  final timers = <FakeTimer>[];

  Timer call(Duration duration, void Function() callback) {
    final timer = FakeTimer(duration, callback);
    timers.add(timer);
    return timer;
  }
}

class TrackingSyncJsonStorage extends InMemorySyncJsonStorage {
  var readAllCalls = 0;
  var readPendingCalls = 0;

  @override
  Future<List<SyncJsonMap>> readAll() async {
    readAllCalls += 1;
    throw StateError('readAll should not be used for pending queries.');
  }

  @override
  Future<List<SyncJsonMap>> readPending({required DateTime dueAt}) async {
    readPendingCalls += 1;
    return super.readPending(dueAt: dueAt);
  }
}

void main() {
  SyncOperation operation({
    String id = 'op-1',
    SyncEntityRef entity = const SyncEntityRef(type: 'task', id: 'task-1'),
    Map<String, Object?> payload = const <String, Object?>{
      'title': 'Ship package',
    },
    Map<String, Object?> headers = const <String, Object?>{},
    DateTime? createdAt,
  }) {
    return SyncOperation(
      id: id,
      entity: entity,
      type: SyncOperationType.update,
      payload: payload,
      headers: headers,
      createdAt: createdAt,
    );
  }

  test('enqueue stores pending operations without immediate sync', () async {
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(store: store, transport: transport);

    await engine.enqueue(operation(), syncImmediately: false);

    final records = await store.readAll();
    expect(records, hasLength(1));
    expect(records.single.status, SyncStatus.pending);
    expect(transport.sent, isEmpty);
    await engine.dispose();
  });

  test('enqueue mutation generates operation ids through the engine', () async {
    final now = DateTime.utc(2026, 6, 30, 12);
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(
      store: store,
      transport: transport,
      operationIdGenerator: () => 'generated-op',
      clock: () => now,
    );

    await engine.enqueueMutation(
      entity: const SyncEntityRef(type: 'task', id: 'task-generated'),
      type: SyncOperationType.update,
      payload: const <String, Object?>{'title': 'Generated'},
      syncImmediately: false,
    );

    final record = (await store.readAll()).single;
    expect(record.operation.id, 'generated-op');
    expect(record.operation.createdAt, now);
    expect(record.operation.entity.id, 'task-generated');
    expect(record.operation.payload, const <String, Object?>{
      'title': 'Generated',
    });
    await engine.dispose();
  });

  test('enqueue create update delete helpers create typed mutations', () async {
    var generatedId = 0;
    final now = DateTime.utc(2026, 7, 1, 12);
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(
      store: store,
      transport: transport,
      operationIdGenerator: () => 'generated-${generatedId++}',
      clock: () => now,
    );
    const task = SyncEntityRef(type: 'task', id: 'task-1');

    await engine.enqueueCreate(
      entity: task,
      payload: const <String, Object?>{'title': 'Created'},
      headers: const <String, Object?>{'source': 'form'},
      syncImmediately: false,
    );
    await engine.enqueueUpdate(
      entity: task,
      payload: const <String, Object?>{'title': 'Updated'},
      syncImmediately: false,
    );
    await engine.enqueueDelete(entity: task, syncImmediately: false);

    final records = await store.readAll();

    expect(transport.sent, isEmpty);
    expect(records.map((record) => record.operation.id), <String>[
      'generated-0',
      'generated-1',
      'generated-2',
    ]);
    expect(records.map((record) => record.operation.type), <SyncOperationType>[
      SyncOperationType.create,
      SyncOperationType.update,
      SyncOperationType.delete,
    ]);
    expect(records[0].operation.payload, const <String, Object?>{
      'title': 'Created',
    });
    expect(records[0].operation.headers, const <String, Object?>{
      'source': 'form',
    });
    expect(records[1].operation.payload, const <String, Object?>{
      'title': 'Updated',
    });
    expect(records[2].operation.payload, isEmpty);
    expect(records.every((record) => record.operation.createdAt == now), true);

    await engine.dispose();
  });

  test('enqueue latest mutation replaces older pending work', () async {
    var generatedId = 0;
    final now = DateTime.utc(2026, 6, 30, 12);
    final task = const SyncEntityRef(type: 'task', id: 'task-1');
    final invoice = const SyncEntityRef(type: 'invoice', id: 'invoice-1');
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(
      store: store,
      transport: transport,
      operationIdGenerator: () => 'generated-${generatedId++}',
      clock: () => now,
    );

    await engine.enqueue(
      operation(
        id: 'older-task',
        entity: task,
        payload: const <String, Object?>{'title': 'Older'},
      ),
      syncImmediately: false,
    );
    await engine.enqueue(
      operation(id: 'invoice-work', entity: invoice),
      syncImmediately: false,
    );
    await store.put(
      SyncRecord(
        operation: operation(id: 'failed-task', entity: task),
        status: SyncStatus.failed,
        lastFailure: const SyncFailure(message: 'Needs attention'),
      ),
    );

    final latest = await engine.enqueueLatestMutation(
      entity: task,
      type: SyncOperationType.update,
      payload: const <String, Object?>{'title': 'Latest'},
      syncImmediately: false,
    );

    final records = {
      for (final record in await store.readAll()) record.operation.id: record,
    };

    expect(latest.operation.id, 'generated-0');
    expect(latest.operation.createdAt, now);
    expect(latest.operation.payload, const <String, Object?>{
      'title': 'Latest',
    });
    expect(
      records.keys,
      unorderedEquals(<String>['invoice-work', 'failed-task', 'generated-0']),
    );
    expect(records['failed-task']?.status, SyncStatus.failed);
    expect(transport.sent, isEmpty);

    await engine.dispose();
  });

  test('pending operations can be updated before sending', () async {
    final now = DateTime.utc(2026, 7, 1, 12);
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(
      store: store,
      transport: transport,
      clock: () => now,
    );

    await store.put(
      SyncRecord(
        operation: operation(
          payload: const <String, Object?>{'title': 'Old'},
          headers: const <String, Object?>{'route': 'old'},
        ),
        attempts: 2,
        nextAttemptAt: now.add(const Duration(minutes: 1)),
        lastFailure: const SyncFailure(message: 'Temporary outage'),
      ),
    );

    final updated = await engine.updatePendingOperation(
      'op-1',
      payload: const <String, Object?>{'title': 'Updated'},
      headers: const <String, Object?>{'route': 'new'},
      syncImmediately: false,
    );
    final stored = (await store.readAll()).single;

    expect(updated?.status, SyncStatus.pending);
    expect(updated?.attempts, 0);
    expect(updated?.nextAttemptAt, isNull);
    expect(updated?.lastFailure, isNull);
    expect(updated?.updatedAt, now);
    expect(stored.operation.payload, const <String, Object?>{
      'title': 'Updated',
    });
    expect(stored.operation.headers, const <String, Object?>{'route': 'new'});
    expect(transport.sent, isEmpty);

    await engine.drain();

    expect(transport.sent, hasLength(1));
    expect(transport.sent.single.payload, const <String, Object?>{
      'title': 'Updated',
    });
    expect(transport.sent.single.headers, const <String, Object?>{
      'route': 'new',
    });
    expect(await store.readAll(), isEmpty);

    await engine.dispose();
  });

  test('pending operation update rejects non-pending records', () async {
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(store: store, transport: transport);

    await store.put(
      SyncRecord(operation: operation(), status: SyncStatus.syncing),
    );

    expect(
      () => engine.updatePendingOperation('op-1', syncImmediately: false),
      throwsA(isA<StateError>()),
    );
    expect(
      await engine.updatePendingOperation('missing', syncImmediately: false),
      isNull,
    );

    await engine.dispose();
  });

  test('optimistic helper applies local change before queue commit', () async {
    var title = 'Old';
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(store: store, transport: transport);

    final record = await SyncOptimistic.run<SyncRecord>(
      apply: () {
        title = 'New';
      },
      commit: () => engine.enqueueMutation(
        entity: const SyncEntityRef(type: 'task', id: 'task-1'),
        type: SyncOperationType.update,
        payload: <String, Object?>{'title': title},
        syncImmediately: false,
      ),
      rollback: (_, _) {
        title = 'Old';
      },
    );

    expect(title, 'New');
    expect(record.operation.payload, const <String, Object?>{'title': 'New'});
    expect(
      (await store.readAll()).single.operation.payload,
      const <String, Object?>{'title': 'New'},
    );

    await engine.dispose();
  });

  test('optimistic helper rolls back when commit fails', () async {
    var title = 'Old';
    final error = StateError('queue failed');

    await expectLater(
      SyncOptimistic.run<void>(
        apply: () {
          title = 'New';
        },
        commit: () async {
          throw error;
        },
        rollback: (rollbackError, stackTrace) {
          expect(rollbackError, same(error));
          expect(stackTrace, isA<StackTrace>());
          title = 'Old';
        },
      ),
      throwsA(same(error)),
    );
    expect(title, 'Old');
  });

  test('drain sends due operations and removes successful records', () async {
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(store: store, transport: transport);
    final statuses = <SyncStatus>[];
    final subscription = engine
        .watchEntity(operation().entity)
        .listen((record) => statuses.add(record.status));

    await engine.enqueue(operation(), syncImmediately: false);
    final result = await engine.drain();

    expect(result.status, SyncDrainStatus.completed);
    expect(result.processedCount, 1);
    expect(result.succeededCount, 1);
    expect(result.retryScheduledCount, 0);
    expect(result.failedCount, 0);
    expect(result.conflictedCount, 0);
    expect(result.didWork, isTrue);
    expect(result.needsAttention, isFalse);
    expect(transport.sent, hasLength(1));
    expect(await store.readAll(), isEmpty);
    expect(
      statuses,
      containsAllInOrder(<SyncStatus>[
        SyncStatus.pending,
        SyncStatus.syncing,
        SyncStatus.synced,
      ]),
    );
    await subscription.cancel();
    await engine.dispose();
  });

  test('drain can process a bounded batch', () async {
    final now = DateTime.utc(2026, 7, 1, 12);
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(
      store: store,
      transport: transport,
      clock: () => now,
    );

    await engine.enqueue(
      operation(id: 'first', createdAt: now),
      syncImmediately: false,
    );
    await engine.enqueue(
      operation(id: 'second', createdAt: now.add(const Duration(seconds: 1))),
      syncImmediately: false,
    );
    await engine.enqueue(
      operation(id: 'third', createdAt: now.add(const Duration(seconds: 2))),
      syncImmediately: false,
    );

    final firstBatch = await engine.drain(maxOperations: 2);

    expect(firstBatch.status, SyncDrainStatus.completed);
    expect(firstBatch.processedCount, 2);
    expect(firstBatch.succeededCount, 2);
    expect(firstBatch.reachedLimit, isTrue);
    expect(firstBatch.shouldContinue, isTrue);
    expect(transport.sent.map((operation) => operation.id), <String>[
      'first',
      'second',
    ]);
    final remaining = await store.readAll();
    expect(remaining, hasLength(1));
    expect(remaining.single.operation.id, 'third');
    expect(remaining.single.status, SyncStatus.pending);

    final secondBatch = await engine.drain(maxOperations: 2);

    expect(secondBatch.processedCount, 1);
    expect(secondBatch.succeededCount, 1);
    expect(secondBatch.reachedLimit, isFalse);
    expect(secondBatch.shouldContinue, isFalse);
    expect(await store.readAll(), isEmpty);

    await engine.dispose();
  });

  test('drain entity can process a bounded batch', () async {
    final now = DateTime.utc(2026, 7, 1, 12);
    final task = const SyncEntityRef(type: 'task', id: 'task-1');
    final invoice = const SyncEntityRef(type: 'invoice', id: 'invoice-1');
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(
      store: store,
      transport: transport,
      clock: () => now,
    );

    await engine.enqueue(
      operation(id: 'task-first', entity: task, createdAt: now),
      syncImmediately: false,
    );
    await engine.enqueue(
      operation(
        id: 'task-second',
        entity: task,
        createdAt: now.add(const Duration(seconds: 1)),
      ),
      syncImmediately: false,
    );
    await engine.enqueue(
      operation(
        id: 'invoice-work',
        entity: invoice,
        createdAt: now.add(const Duration(seconds: 2)),
      ),
      syncImmediately: false,
    );

    final result = await engine.drainEntity(task, maxOperations: 1);

    expect(result.processedCount, 1);
    expect(result.succeededCount, 1);
    expect(result.reachedLimit, isTrue);
    expect(transport.sent.map((operation) => operation.id), <String>[
      'task-first',
    ]);
    final remainingIds = (await store.readAll())
        .map((record) => record.operation.id)
        .toList();
    expect(remainingIds, <String>['task-second', 'invoice-work']);

    await engine.dispose();
  });

  test('drain rejects non-positive operation limits', () async {
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(store: store, transport: transport);

    expect(() => engine.drain(maxOperations: 0), throwsArgumentError);
    expect(
      () => engine.drainEntity(
        const SyncEntityRef(type: 'task', id: 'task-1'),
        maxOperations: -1,
      ),
      throwsArgumentError,
    );

    await engine.dispose();
  });

  test('drain returns outcome counts for processed records', () async {
    final now = DateTime.utc(2026, 6, 30, 12);
    final store = InMemorySyncStore();
    final transport = FakeTransport((operation) async {
      return switch (operation.id) {
        'success' => const SyncResult.success(),
        'retry' => const SyncResult.failure(
          SyncFailure(message: 'Network unavailable'),
        ),
        'fail' => const SyncResult.failure(
          SyncFailure(message: 'Bad request', isRetryable: false),
        ),
        'conflict' => const SyncResult.conflict(message: 'Server changed'),
        _ => throw StateError('Unexpected operation ${operation.id}'),
      };
    });
    final engine = SyncEngine(
      store: store,
      transport: transport,
      retryPolicy: const RetryPolicy(baseDelay: Duration(seconds: 5)),
      clock: () => now,
    );

    await engine.enqueue(operation(id: 'success'), syncImmediately: false);
    await engine.enqueue(operation(id: 'retry'), syncImmediately: false);
    await engine.enqueue(operation(id: 'fail'), syncImmediately: false);
    await engine.enqueue(operation(id: 'conflict'), syncImmediately: false);

    final result = await engine.drain();

    expect(result.status, SyncDrainStatus.completed);
    expect(result.isCompleted, isTrue);
    expect(result.wasSkipped, isFalse);
    expect(result.didWork, isTrue);
    expect(result.needsAttention, isTrue);
    expect(result.processedCount, 4);
    expect(result.succeededCount, 1);
    expect(result.retryScheduledCount, 1);
    expect(result.failedCount, 1);
    expect(result.conflictedCount, 1);

    final records = {
      for (final record in await store.readAll()) record.operation.id: record,
    };
    expect(
      records.keys,
      unorderedEquals(<String>['retry', 'fail', 'conflict']),
    );
    expect(records['retry']?.status, SyncStatus.pending);
    expect(
      records['retry']?.nextAttemptAt,
      now.add(const Duration(seconds: 5)),
    );
    expect(records['fail']?.status, SyncStatus.failed);
    expect(records['conflict']?.status, SyncStatus.conflicted);

    await engine.dispose();
  });

  test('watch engine state emits drain lifecycle snapshots', () async {
    final store = InMemorySyncStore();
    final started = Completer<void>();
    final release = Completer<SyncResult>();
    final transport = FakeTransport((_) {
      if (!started.isCompleted) {
        started.complete();
      }
      return release.future;
    });
    final engine = SyncEngine(store: store, transport: transport);
    final states = engine
        .watchEngineState()
        .map((state) => state.status)
        .take(3);
    final expectation = expectLater(
      states,
      emitsInOrder(<SyncEngineStatus>[
        SyncEngineStatus.idle,
        SyncEngineStatus.draining,
        SyncEngineStatus.idle,
      ]),
    );

    await Future<void>.delayed(Duration.zero);
    await engine.enqueue(operation(), syncImmediately: false);
    final drain = engine.drain();

    await started.future;
    expect(engine.engineState.status, SyncEngineStatus.draining);
    expect(engine.engineState.isDraining, isTrue);

    release.complete(const SyncResult.success());
    final result = await drain;

    expect(engine.engineState.status, SyncEngineStatus.idle);
    expect(engine.engineState.lastDrainResult, same(result));
    expect(engine.engineState.hasLastDrainResult, isTrue);
    await expectation;

    await engine.dispose();
  });

  test('engine state remembers skipped drain results', () async {
    final store = InMemorySyncStore();
    final connectivity = ManualSyncConnectivity(SyncConnectivityStatus.offline);
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(
      store: store,
      transport: transport,
      connectivity: connectivity,
    );

    final result = await engine.drain();

    expect(result.status, SyncDrainStatus.skippedOffline);
    expect(engine.engineState.status, SyncEngineStatus.idle);
    expect(engine.engineState.lastDrainResult, same(result));
    expect(engine.engineState.lastDrainWasSkipped, isTrue);

    await engine.dispose();
    await connectivity.dispose();
  });

  test('watch engine state emits disposed snapshots', () async {
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(store: store, transport: transport);
    final states = engine
        .watchEngineState()
        .map((state) => state.status)
        .take(2);
    final expectation = expectLater(
      states,
      emitsInOrder(<SyncEngineStatus>[
        SyncEngineStatus.idle,
        SyncEngineStatus.disposed,
      ]),
    );

    await Future<void>.delayed(Duration.zero);
    await engine.dispose();

    expect(engine.engineState.status, SyncEngineStatus.disposed);
    expect(engine.engineState.isDisposed, isTrue);
    await expectation;
  });

  test('dispose can be called repeatedly without throwing', () async {
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(store: store, transport: transport);

    await engine.dispose();
    await engine.dispose();

    expect(engine.engineState.status, SyncEngineStatus.disposed);
  });

  test('drain after dispose returns skipped disposed result', () async {
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(store: store, transport: transport);

    await engine.enqueue(operation(), syncImmediately: false);
    await engine.dispose();

    final result = await engine.drain();

    expect(result.status, SyncDrainStatus.skippedDisposed);
    expect(result.wasSkipped, isTrue);
    expect(engine.engineState.status, SyncEngineStatus.disposed);
    expect(transport.sent, isEmpty);
  });

  test(
    'drain reruns when another full drain is requested while active',
    () async {
      final now = DateTime.utc(2026, 7, 1, 12);
      final store = InMemorySyncStore();
      late SyncEngine engine;
      var enqueuedSecond = false;
      final transport = FakeTransport((sent) async {
        if (sent.id == 'first' && !enqueuedSecond) {
          enqueuedSecond = true;
          await engine.enqueue(
            operation(
              id: 'second',
              createdAt: now.add(const Duration(seconds: 1)),
            ),
          );
        }

        return const SyncResult.success();
      });
      engine = SyncEngine(store: store, transport: transport, clock: () => now);

      await engine.enqueue(operation(id: 'first'), syncImmediately: false);
      final result = await engine.drain();

      expect(result.status, SyncDrainStatus.completed);
      expect(result.processedCount, 2);
      expect(result.succeededCount, 2);
      expect(transport.sent.map((operation) => operation.id), <String>[
        'first',
        'second',
      ]);
      expect(await store.readAll(), isEmpty);

      await engine.dispose();
    },
  );

  test('drain entity sends only due work for one entity', () async {
    final now = DateTime.utc(2026, 7, 1, 12);
    final task = const SyncEntityRef(type: 'task', id: 'task-1');
    final invoice = const SyncEntityRef(type: 'invoice', id: 'invoice-1');
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(
      store: store,
      transport: transport,
      clock: () => now,
    );

    await engine.enqueue(
      operation(id: 'task-due', entity: task),
      syncImmediately: false,
    );
    await engine.enqueue(
      operation(id: 'invoice-due', entity: invoice),
      syncImmediately: false,
    );
    await store.put(
      SyncRecord(
        operation: operation(
          id: 'task-future',
          entity: task,
          createdAt: DateTime.utc(2026, 7, 1, 12, 1),
        ),
        nextAttemptAt: now.add(const Duration(minutes: 1)),
      ),
    );

    final result = await engine.drainEntity(task);
    final records = {
      for (final record in await store.readAll()) record.operation.id: record,
    };

    expect(result.status, SyncDrainStatus.completed);
    expect(result.processedCount, 1);
    expect(result.succeededCount, 1);
    expect(result.didWork, isTrue);
    expect(transport.sent.map((operation) => operation.id), <String>[
      'task-due',
    ]);
    expect(
      records.keys,
      unorderedEquals(<String>['invoice-due', 'task-future']),
    );
    expect(records['invoice-due']?.status, SyncStatus.pending);
    expect(records['task-future']?.status, SyncStatus.pending);

    await engine.dispose();
  });

  test('drain operation sends only one due operation', () async {
    final now = DateTime.utc(2026, 7, 1, 12);
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(
      store: store,
      transport: transport,
      clock: () => now,
    );

    await engine.enqueue(operation(id: 'target'), syncImmediately: false);
    await engine.enqueue(operation(id: 'other-due'), syncImmediately: false);
    await store.put(
      SyncRecord(
        operation: operation(
          id: 'future-target',
          createdAt: DateTime.utc(2026, 7, 1, 12, 1),
        ),
        nextAttemptAt: now.add(const Duration(minutes: 1)),
      ),
    );

    final result = await engine.drainOperation('target');
    final records = {
      for (final record in await store.readAll()) record.operation.id: record,
    };

    expect(result.status, SyncDrainStatus.completed);
    expect(result.processedCount, 1);
    expect(result.succeededCount, 1);
    expect(transport.sent.map((operation) => operation.id), <String>['target']);
    expect(
      records.keys,
      unorderedEquals(<String>['other-due', 'future-target']),
    );
    expect(records['other-due']?.status, SyncStatus.pending);
    expect(records['future-target']?.status, SyncStatus.pending);

    await engine.dispose();
  });

  test('retryable failures are rescheduled with backoff', () async {
    var now = DateTime(2026);
    final store = InMemorySyncStore();
    final transport = FakeTransport(
      (_) async =>
          const SyncResult.failure(SyncFailure(message: 'Network unavailable')),
    );
    final engine = SyncEngine(
      store: store,
      transport: transport,
      retryPolicy: const RetryPolicy(baseDelay: Duration(seconds: 5)),
      clock: () => now,
    );

    await engine.enqueue(operation(), syncImmediately: false);
    await engine.drain();

    final record = (await store.readAll()).single;
    expect(record.status, SyncStatus.pending);
    expect(record.attempts, 1);
    expect(record.nextAttemptAt, DateTime(2026, 1, 1, 0, 0, 5));

    await engine.drain();
    expect(transport.sent, hasLength(1));

    now = now.add(const Duration(seconds: 5));
    await engine.drain();
    expect(transport.sent, hasLength(2));
    await engine.dispose();
  });

  test('retry policy can apply bounded jitter', () {
    const policy = RetryPolicy(
      baseDelay: Duration(seconds: 10),
      multiplier: 2,
      jitterFactor: 0.25,
    );

    expect(
      policy.delayForAttempt(1, jitter: 0),
      const Duration(milliseconds: 7500),
    );
    expect(
      policy.delayForAttempt(1, jitter: 0.5),
      const Duration(milliseconds: 8750),
    );
    expect(policy.delayForAttempt(1, jitter: 1), const Duration(seconds: 10));
    expect(policy.delayForAttempt(2, jitter: 0), const Duration(seconds: 15));
    expect(policy.delayForAttempt(2, jitter: 1), const Duration(seconds: 20));
  });

  test('retryable failures can jitter retry backoff', () async {
    final now = DateTime.utc(2026, 7, 1, 12);
    final timers = FakeTimerFactory();
    final store = InMemorySyncStore();
    final transport = FakeTransport(
      (_) async =>
          const SyncResult.failure(SyncFailure(message: 'Temporary outage')),
    );
    final engine = SyncEngine(
      store: store,
      transport: transport,
      retryPolicy: const RetryPolicy(
        baseDelay: Duration(seconds: 10),
        jitterFactor: 0.5,
      ),
      retryJitter: () => 0.25,
      timerFactory: timers.call,
      clock: () => now,
    );

    await engine.enqueue(operation(), syncImmediately: false);
    await engine.drain();

    final retryDelay = const Duration(milliseconds: 6250);
    final record = (await store.readAll()).single;
    expect(record.nextAttemptAt, now.add(retryDelay));
    expect(timers.timers.single.duration, retryDelay);

    await engine.dispose();
  });

  test('retryable failures can override backoff with retry after', () async {
    final now = DateTime.utc(2026, 7, 1, 12);
    final timers = FakeTimerFactory();
    final store = InMemorySyncStore();
    final transport = FakeTransport(
      (_) async => const SyncResult.failure(
        SyncFailure(
          message: 'Rate limited',
          code: 'rate_limited',
          retryAfter: Duration(seconds: 30),
        ),
      ),
    );
    final engine = SyncEngine(
      store: store,
      transport: transport,
      retryPolicy: const RetryPolicy(baseDelay: Duration(seconds: 5)),
      timerFactory: timers.call,
      clock: () => now,
    );

    await engine.enqueue(operation(), syncImmediately: false);
    await engine.drain();

    final record = (await store.readAll()).single;
    expect(record.status, SyncStatus.pending);
    expect(record.nextAttemptAt, now.add(const Duration(seconds: 30)));
    expect(record.lastFailure?.retryAfter, const Duration(seconds: 30));
    expect(timers.timers.single.duration, const Duration(seconds: 30));

    await engine.dispose();
  });

  test(
    'retryable failures automatically drain when retry timer fires',
    () async {
      var now = DateTime(2026);
      var attempts = 0;
      final timers = FakeTimerFactory();
      final store = InMemorySyncStore();
      final transport = FakeTransport((_) async {
        attempts += 1;
        if (attempts == 1) {
          return const SyncResult.failure(
            SyncFailure(message: 'Network unavailable'),
          );
        }

        return const SyncResult.success();
      });
      final engine = SyncEngine(
        store: store,
        transport: transport,
        retryPolicy: const RetryPolicy(baseDelay: Duration(seconds: 5)),
        timerFactory: timers.call,
        clock: () => now,
      );

      await engine.enqueue(operation(), syncImmediately: false);
      await engine.drain();

      expect(transport.sent, hasLength(1));
      expect(timers.timers, hasLength(1));
      expect(timers.timers.single.duration, const Duration(seconds: 5));

      final synced = engine.events.firstWhere(
        (record) => record.status == SyncStatus.synced,
      );
      now = now.add(const Duration(seconds: 5));
      timers.timers.single.fire();
      await synced;

      expect(transport.sent, hasLength(2));
      expect(await store.readAll(), isEmpty);
      await engine.dispose();
    },
  );

  test('retry scheduling can be disabled for external schedulers', () async {
    var now = DateTime(2026);
    final timers = FakeTimerFactory();
    final store = InMemorySyncStore();
    final transport = FakeTransport(
      (_) async =>
          const SyncResult.failure(SyncFailure(message: 'Network unavailable')),
    );
    final engine = SyncEngine(
      store: store,
      transport: transport,
      retryPolicy: const RetryPolicy(baseDelay: Duration(seconds: 5)),
      autoDrainOnRetry: false,
      timerFactory: timers.call,
      clock: () => now,
    );

    await engine.enqueue(operation(), syncImmediately: false);
    await engine.drain();

    expect(timers.timers, isEmpty);
    now = now.add(const Duration(seconds: 5));
    await engine.drain();
    expect(transport.sent, hasLength(2));
    await engine.dispose();
  });

  test('conflicts are persisted for app-specific resolution', () async {
    final store = InMemorySyncStore();
    final transport = FakeTransport(
      (_) async => const SyncResult.conflict(
        message: 'Server version changed',
        local: <String, Object?>{'title': 'Local'},
        remote: <String, Object?>{'title': 'Remote'},
      ),
    );
    final engine = SyncEngine(store: store, transport: transport);

    await engine.enqueue(operation(), syncImmediately: false);
    await engine.drain();

    final record = (await store.readAll()).single;
    expect(record.status, SyncStatus.conflicted);
    expect(record.conflict?.message, 'Server version changed');
    await engine.dispose();
  });

  test('failed operations can be manually retried', () async {
    var sendCount = 0;
    final store = InMemorySyncStore();
    final transport = FakeTransport((operation) async {
      sendCount += 1;
      if (sendCount == 1) {
        return const SyncResult.failure(
          SyncFailure(message: 'Bad request', isRetryable: false),
        );
      }

      return const SyncResult.success();
    });
    final engine = SyncEngine(store: store, transport: transport);

    await engine.enqueue(operation(), syncImmediately: false);
    await engine.drain();

    final failed = (await store.readAll()).single;
    expect(failed.status, SyncStatus.failed);
    expect(failed.lastFailure?.message, 'Bad request');
    expect(failed.attempts, 1);

    final retried = await engine.retryFailedOperation(
      'op-1',
      payload: const <String, Object?>{'title': 'Retry payload'},
      syncImmediately: false,
    );

    expect(retried?.status, SyncStatus.pending);
    expect(retried?.attempts, 0);
    expect(retried?.lastFailure, isNull);
    expect(retried?.operation.payload, const <String, Object?>{
      'title': 'Retry payload',
    });

    await engine.drain();

    expect(transport.sent, hasLength(2));
    expect(transport.sent.last.payload, const <String, Object?>{
      'title': 'Retry payload',
    });
    expect(await store.readAll(), isEmpty);
    await engine.dispose();
  });

  test('failed operations can be retried in bulk', () async {
    final now = DateTime.utc(2026, 7, 1, 12);
    final task = const SyncEntityRef(type: 'task', id: 'task-1');
    final invoice = const SyncEntityRef(type: 'invoice', id: 'invoice-1');
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(
      store: store,
      transport: transport,
      clock: () => now,
    );
    final emitted = engine.events
        .map((record) => '${record.operation.id}:${record.status.name}')
        .take(2);
    final expectation = expectLater(
      emitted,
      emitsInOrder(<String>['task-failed:pending', 'invoice-failed:pending']),
    );

    await store.put(
      SyncRecord(
        operation: operation(id: 'task-failed', entity: task, createdAt: now),
        status: SyncStatus.failed,
        attempts: 2,
        lastFailure: const SyncFailure(message: 'Task failed'),
      ),
    );
    await store.put(
      SyncRecord(
        operation: operation(
          id: 'invoice-failed',
          entity: invoice,
          createdAt: now.add(const Duration(seconds: 1)),
        ),
        status: SyncStatus.failed,
        attempts: 3,
        nextAttemptAt: now.add(const Duration(minutes: 5)),
        lastFailure: const SyncFailure(message: 'Invoice failed'),
      ),
    );
    await store.put(
      SyncRecord(
        operation: operation(
          id: 'pending-work',
          createdAt: now.add(const Duration(seconds: 2)),
        ),
      ),
    );
    await store.put(
      SyncRecord(
        operation: operation(
          id: 'conflicted-work',
          createdAt: now.add(const Duration(seconds: 3)),
        ),
        status: SyncStatus.conflicted,
      ),
    );

    final retried = await engine.retryFailedOperations(syncImmediately: false);

    expect(retried.map((record) => record.operation.id), <String>[
      'task-failed',
      'invoice-failed',
    ]);
    expect(
      retried.every((record) => record.status == SyncStatus.pending),
      true,
    );
    expect(retried.every((record) => record.attempts == 0), true);
    expect(retried.every((record) => record.lastFailure == null), true);
    expect(retried.every((record) => record.nextAttemptAt == null), true);
    expect(transport.sent, isEmpty);
    await expectation;

    final records = {
      for (final record in await store.readAll()) record.operation.id: record,
    };
    expect(records['task-failed']?.status, SyncStatus.pending);
    expect(records['invoice-failed']?.status, SyncStatus.pending);
    expect(records['pending-work']?.status, SyncStatus.pending);
    expect(records['conflicted-work']?.status, SyncStatus.conflicted);

    await engine.dispose();
  });

  test('failed operation bulk retry can be scoped to one entity', () async {
    final task = const SyncEntityRef(type: 'task', id: 'task-1');
    final invoice = const SyncEntityRef(type: 'invoice', id: 'invoice-1');
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(store: store, transport: transport);

    await store.put(
      SyncRecord(
        operation: operation(id: 'task-failed', entity: task),
        status: SyncStatus.failed,
        attempts: 4,
        lastFailure: const SyncFailure(message: 'Task failed'),
      ),
    );
    await store.put(
      SyncRecord(
        operation: operation(id: 'invoice-failed', entity: invoice),
        status: SyncStatus.failed,
        attempts: 2,
        lastFailure: const SyncFailure(message: 'Invoice failed'),
      ),
    );

    final retried = await engine.retryFailedOperations(
      entity: task,
      resetAttempts: false,
      syncImmediately: false,
    );

    expect(retried, hasLength(1));
    expect(retried.single.operation.id, 'task-failed');
    expect(retried.single.status, SyncStatus.pending);
    expect(retried.single.attempts, 4);
    expect(retried.single.lastFailure, isNull);

    final records = {
      for (final record in await store.readAll()) record.operation.id: record,
    };
    expect(records['task-failed']?.status, SyncStatus.pending);
    expect(records['invoice-failed']?.status, SyncStatus.failed);
    expect(records['invoice-failed']?.attempts, 2);

    await engine.dispose();
  });

  test('failed operation bulk retry can drain immediately', () async {
    final now = DateTime.utc(2026, 7, 1, 12);
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(
      store: store,
      transport: transport,
      clock: () => now,
    );

    await store.put(
      SyncRecord(
        operation: operation(id: 'failed-1', createdAt: now),
        status: SyncStatus.failed,
        lastFailure: const SyncFailure(message: 'Failed 1'),
      ),
    );
    await store.put(
      SyncRecord(
        operation: operation(
          id: 'failed-2',
          createdAt: now.add(const Duration(seconds: 1)),
        ),
        status: SyncStatus.failed,
        lastFailure: const SyncFailure(message: 'Failed 2'),
      ),
    );

    final retried = await engine.retryFailedOperations();

    expect(retried.map((record) => record.operation.id), <String>[
      'failed-1',
      'failed-2',
    ]);
    expect(transport.sent.map((operation) => operation.id), <String>[
      'failed-1',
      'failed-2',
    ]);
    expect(await store.readAll(), isEmpty);

    await engine.dispose();
  });

  test('failed operation retry rejects non-failed records', () async {
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(store: store, transport: transport);

    await engine.enqueue(operation(), syncImmediately: false);

    expect(
      () => engine.retryFailedOperation('op-1', syncImmediately: false),
      throwsA(isA<StateError>()),
    );
    expect(
      await engine.retryFailedOperation('missing', syncImmediately: false),
      isNull,
    );

    await engine.dispose();
  });

  test('discard operation removes queued work and emits synced', () async {
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(store: store, transport: transport);
    final emitted = <SyncStatus>[];
    final subscription = engine.events.listen(
      (record) => emitted.add(record.status),
    );

    await engine.enqueue(operation(), syncImmediately: false);

    final discarded = await engine.discardOperation('op-1');

    expect(discarded?.status, SyncStatus.synced);
    expect(discarded?.lastFailure, isNull);
    expect(discarded?.conflict, isNull);
    expect(await store.readAll(), isEmpty);
    expect(transport.sent, isEmpty);
    expect(emitted, <SyncStatus>[SyncStatus.pending, SyncStatus.synced]);

    await subscription.cancel();
    await engine.dispose();
  });

  test('failed operations can be discarded in bulk', () async {
    final now = DateTime.utc(2026, 7, 1, 12);
    final task = const SyncEntityRef(type: 'task', id: 'task-1');
    final invoice = const SyncEntityRef(type: 'invoice', id: 'invoice-1');
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(
      store: store,
      transport: transport,
      clock: () => now,
    );
    final emitted = engine.events
        .map((record) => '${record.operation.id}:${record.status.name}')
        .take(2);
    final expectation = expectLater(
      emitted,
      emitsInOrder(<String>['task-failed:synced', 'invoice-failed:synced']),
    );

    await store.put(
      SyncRecord(
        operation: operation(id: 'task-failed', entity: task, createdAt: now),
        status: SyncStatus.failed,
        attempts: 2,
        lastFailure: const SyncFailure(message: 'Task failed'),
      ),
    );
    await store.put(
      SyncRecord(
        operation: operation(
          id: 'invoice-failed',
          entity: invoice,
          createdAt: now.add(const Duration(seconds: 1)),
        ),
        status: SyncStatus.failed,
        attempts: 3,
        lastFailure: const SyncFailure(message: 'Invoice failed'),
      ),
    );
    await store.put(
      SyncRecord(
        operation: operation(
          id: 'pending-work',
          createdAt: now.add(const Duration(seconds: 2)),
        ),
      ),
    );
    await store.put(
      SyncRecord(
        operation: operation(
          id: 'conflicted-work',
          createdAt: now.add(const Duration(seconds: 3)),
        ),
        status: SyncStatus.conflicted,
      ),
    );

    final discarded = await engine.discardFailedOperations();

    expect(discarded.map((record) => record.operation.id), <String>[
      'task-failed',
      'invoice-failed',
    ]);
    expect(
      discarded.map((record) => record.status),
      everyElement(SyncStatus.synced),
    );
    expect(discarded.every((record) => record.lastFailure == null), true);
    expect(transport.sent, isEmpty);
    await expectation;

    final records = {
      for (final record in await store.readAll()) record.operation.id: record,
    };
    expect(
      records.keys,
      unorderedEquals(<String>['pending-work', 'conflicted-work']),
    );
    expect(records['pending-work']?.status, SyncStatus.pending);
    expect(records['conflicted-work']?.status, SyncStatus.conflicted);

    await engine.dispose();
  });

  test('failed operation bulk discard can be scoped to one entity', () async {
    final task = const SyncEntityRef(type: 'task', id: 'task-1');
    final invoice = const SyncEntityRef(type: 'invoice', id: 'invoice-1');
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(store: store, transport: transport);

    await store.put(
      SyncRecord(
        operation: operation(id: 'task-failed', entity: task),
        status: SyncStatus.failed,
        attempts: 4,
        lastFailure: const SyncFailure(message: 'Task failed'),
      ),
    );
    await store.put(
      SyncRecord(
        operation: operation(id: 'invoice-failed', entity: invoice),
        status: SyncStatus.failed,
        attempts: 2,
        lastFailure: const SyncFailure(message: 'Invoice failed'),
      ),
    );

    final discarded = await engine.discardFailedOperations(entity: task);

    expect(discarded, hasLength(1));
    expect(discarded.single.operation.id, 'task-failed');
    expect(discarded.single.status, SyncStatus.synced);
    expect(discarded.single.lastFailure, isNull);

    final records = {
      for (final record in await store.readAll()) record.operation.id: record,
    };
    expect(records.keys, <String>['invoice-failed']);
    expect(records['invoice-failed']?.status, SyncStatus.failed);
    expect(records['invoice-failed']?.attempts, 2);

    await engine.dispose();
  });

  test('discard operation rejects syncing records', () async {
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(store: store, transport: transport);

    await store.put(
      SyncRecord(operation: operation(), status: SyncStatus.syncing),
    );

    expect(() => engine.discardOperation('op-1'), throwsA(isA<StateError>()));
    expect(await engine.discardOperation('missing'), isNull);

    await engine.dispose();
  });

  test('discard pending for entity removes only pending entity work', () async {
    final task = const SyncEntityRef(type: 'task', id: 'task-1');
    final invoice = const SyncEntityRef(type: 'invoice', id: 'invoice-1');
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(store: store, transport: transport);
    final emitted = <String>[];
    final subscription = engine.events.listen(
      (record) => emitted.add('${record.operation.id}:${record.status.name}'),
    );

    await engine.enqueue(
      operation(id: 'task-pending-1', entity: task),
      syncImmediately: false,
    );
    await engine.enqueue(
      operation(id: 'task-pending-2', entity: task),
      syncImmediately: false,
    );
    await engine.enqueue(
      operation(id: 'invoice-pending', entity: invoice),
      syncImmediately: false,
    );
    await store.put(
      SyncRecord(
        operation: operation(id: 'task-failed', entity: task),
        status: SyncStatus.failed,
        lastFailure: const SyncFailure(message: 'Needs attention'),
      ),
    );
    await store.put(
      SyncRecord(
        operation: operation(id: 'task-conflicted', entity: task),
        status: SyncStatus.conflicted,
        conflict: const SyncConflict(message: 'Server changed'),
      ),
    );

    final discarded = await engine.discardPendingForEntity(task);
    final records = {
      for (final record in await store.readAll()) record.operation.id: record,
    };

    expect(discarded.map((record) => record.operation.id), <String>[
      'task-pending-1',
      'task-pending-2',
    ]);
    expect(
      discarded.map((record) => record.status),
      everyElement(SyncStatus.synced),
    );
    expect(
      records.keys,
      unorderedEquals(<String>[
        'invoice-pending',
        'task-failed',
        'task-conflicted',
      ]),
    );
    expect(records['invoice-pending']?.status, SyncStatus.pending);
    expect(records['task-failed']?.status, SyncStatus.failed);
    expect(records['task-conflicted']?.status, SyncStatus.conflicted);
    expect(transport.sent, isEmpty);
    expect(
      emitted,
      containsAllInOrder(<String>[
        'task-pending-1:pending',
        'task-pending-2:pending',
        'invoice-pending:pending',
        'task-pending-1:synced',
        'task-pending-2:synced',
      ]),
    );

    await subscription.cancel();
    await engine.dispose();
  });

  test('enqueue skips immediate drain while connectivity is offline', () async {
    final store = InMemorySyncStore();
    final connectivity = ManualSyncConnectivity(SyncConnectivityStatus.offline);
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(
      store: store,
      transport: transport,
      connectivity: connectivity,
    );

    await engine.enqueue(operation());

    final records = await store.readAll();
    expect(records, hasLength(1));
    expect(records.single.status, SyncStatus.pending);
    expect(transport.sent, isEmpty);

    final result = await engine.drain();
    expect(result.status, SyncDrainStatus.skippedOffline);
    expect(result.wasSkipped, isTrue);
    expect(result.didWork, isFalse);
    expect(result.processedCount, 0);
    expect(transport.sent, isEmpty);

    await engine.dispose();
    await connectivity.dispose();
  });

  test('engine drains pending operations when connectivity returns', () async {
    final store = InMemorySyncStore();
    final connectivity = ManualSyncConnectivity(SyncConnectivityStatus.offline);
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(
      store: store,
      transport: transport,
      connectivity: connectivity,
    );

    await engine.enqueue(operation(), syncImmediately: false);
    final synced = engine.events.firstWhere(
      (record) => record.status == SyncStatus.synced,
    );

    connectivity.setOnline();
    await synced;

    expect(transport.sent, hasLength(1));
    expect(await store.readAll(), isEmpty);

    await engine.dispose();
    await connectivity.dispose();
  });

  test('forced drain ignores offline connectivity status', () async {
    final store = InMemorySyncStore();
    final connectivity = ManualSyncConnectivity(SyncConnectivityStatus.offline);
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(
      store: store,
      transport: transport,
      connectivity: connectivity,
    );

    await engine.enqueue(operation(), syncImmediately: false);
    await engine.drain(force: true);

    expect(transport.sent, hasLength(1));
    expect(await store.readAll(), isEmpty);

    await engine.dispose();
    await connectivity.dispose();
  });

  test('auto drain can be disabled for custom schedulers', () async {
    final store = InMemorySyncStore();
    final connectivity = ManualSyncConnectivity(SyncConnectivityStatus.offline);
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(
      store: store,
      transport: transport,
      connectivity: connectivity,
      autoDrainOnConnectivityRestored: false,
    );

    await engine.enqueue(operation(), syncImmediately: false);
    connectivity.setOnline();
    await Future<void>.delayed(Duration.zero);

    expect(transport.sent, isEmpty);
    expect(await store.readAll(), hasLength(1));

    await engine.drain();

    expect(transport.sent, hasLength(1));
    expect(await store.readAll(), isEmpty);

    await engine.dispose();
    await connectivity.dispose();
  });

  test('operation JSON round trip preserves durable fields', () {
    final createdAt = DateTime.utc(2026, 6, 30, 12, 15);
    final source = SyncOperation(
      id: 'op-serialize',
      entity: const SyncEntityRef(type: 'invoice', id: 'invoice-1'),
      type: SyncOperationType.create,
      payload: const <String, Object?>{
        'amount': 4200,
        'tags': <Object?>['paid', 'vip'],
      },
      headers: const <String, Object?>{'endpoint': 'createInvoice'},
      createdAt: createdAt,
    );

    final restored = SyncOperation.fromJson(source.toJson());

    expect(restored.id, source.id);
    expect(restored.entity, source.entity);
    expect(restored.type, source.type);
    expect(restored.payload, source.payload);
    expect(restored.headers, source.headers);
    expect(restored.createdAt, createdAt);
  });

  test('failed record JSON round trip preserves retry state', () {
    final operationCreatedAt = DateTime.utc(2026, 6, 30, 12);
    final updatedAt = DateTime.utc(2026, 6, 30, 12, 1);
    final nextAttemptAt = DateTime.utc(2026, 6, 30, 12, 5);
    final source = SyncRecord(
      operation: operation(createdAt: operationCreatedAt),
      status: SyncStatus.pending,
      attempts: 2,
      nextAttemptAt: nextAttemptAt,
      lastFailure: const SyncFailure(
        message: 'Temporary outage',
        code: 'network',
        retryAfter: Duration(seconds: 30),
      ),
      updatedAt: updatedAt,
    );

    final restored = SyncRecord.fromJson(source.toJson());

    expect(restored.operation.id, source.operation.id);
    expect(restored.status, source.status);
    expect(restored.attempts, source.attempts);
    expect(restored.nextAttemptAt, nextAttemptAt);
    expect(restored.lastFailure?.message, 'Temporary outage');
    expect(restored.lastFailure?.code, 'network');
    expect(restored.lastFailure?.retryAfter, const Duration(seconds: 30));
    expect(restored.updatedAt, updatedAt);
  });

  test('conflicted record JSON round trip preserves conflict details', () {
    final source = SyncRecord(
      operation: operation(createdAt: DateTime.utc(2026, 6, 30, 12)),
      status: SyncStatus.conflicted,
      conflict: const SyncConflict(
        message: 'Server changed',
        local: <String, Object?>{'title': 'Local'},
        remote: <String, Object?>{'title': 'Remote'},
      ),
      updatedAt: DateTime.utc(2026, 6, 30, 12, 2),
    );

    final restored = SyncRecord.fromJson(source.toJson());

    expect(restored.status, SyncStatus.conflicted);
    expect(restored.conflict?.message, 'Server changed');
    expect(restored.conflict?.local, const <String, Object?>{'title': 'Local'});
    expect(restored.conflict?.remote, const <String, Object?>{
      'title': 'Remote',
    });
  });

  test('unknown wire values throw format exceptions', () {
    expect(
      () => SyncOperation.fromJson(<String, Object?>{
        'id': 'op-invalid',
        'entity': const SyncEntityRef(type: 'task', id: 'task-1').toJson(),
        'type': 'merge',
        'payload': const <String, Object?>{},
        'headers': const <String, Object?>{},
        'createdAt': DateTime.utc(2026).toIso8601String(),
      }),
      throwsA(isA<FormatException>()),
    );
    expect(
      () => SyncRecord.fromJson(<String, Object?>{
        'operation': operation(createdAt: DateTime.utc(2026)).toJson(),
        'status': 'paused',
        'attempts': 0,
        'nextAttemptAt': null,
        'lastFailure': null,
        'conflict': null,
        'updatedAt': DateTime.utc(2026).toIso8601String(),
      }),
      throwsA(isA<FormatException>()),
    );
  });

  test('JSON sync store persists records through JSON storage', () async {
    final storage = InMemorySyncJsonStorage();
    final store = JsonSyncStore(storage);
    final older = SyncRecord(
      operation: operation(id: 'older', createdAt: DateTime.utc(2026)),
    );
    final newer = SyncRecord(
      operation: operation(id: 'newer', createdAt: DateTime.utc(2026, 1, 2)),
      attempts: 1,
      lastFailure: const SyncFailure(message: 'Try again'),
    );

    await store.put(newer);
    await store.put(older);

    final records = await store.readAll();
    expect(records.map((record) => record.operation.id), <String>[
      'older',
      'newer',
    ]);

    final restored = await store.read('newer');
    expect(restored?.attempts, 1);
    expect(restored?.lastFailure?.message, 'Try again');
    expect(await storage.read('newer'), isNotNull);

    await store.delete('newer');

    expect(await store.read('newer'), isNull);
    expect(await storage.read('newer'), isNull);
  });

  test('JSON sync store reads only due pending records', () async {
    final store = JsonSyncStore(InMemorySyncJsonStorage());
    final dueAt = DateTime.utc(2026, 6, 30, 12);
    final due = SyncRecord(
      operation: operation(id: 'due', createdAt: DateTime.utc(2026)),
      nextAttemptAt: dueAt,
    );
    final future = SyncRecord(
      operation: operation(id: 'future', createdAt: DateTime.utc(2026, 1, 2)),
      nextAttemptAt: dueAt.add(const Duration(minutes: 1)),
    );
    final failed = SyncRecord(
      operation: operation(id: 'failed', createdAt: DateTime.utc(2026, 1, 3)),
      status: SyncStatus.failed,
    );

    await store.put(future);
    await store.put(failed);
    await store.put(due);

    final pending = await store.readPending(dueAt: dueAt);

    expect(pending.map((record) => record.operation.id), <String>['due']);
  });

  test('JSON sync store can use optimized pending queries', () async {
    final storage = TrackingSyncJsonStorage();
    final store = JsonSyncStore(storage);
    final dueAt = DateTime.utc(2026, 6, 30, 12);

    await store.put(
      SyncRecord(
        operation: operation(id: 'due', createdAt: DateTime.utc(2026)),
        nextAttemptAt: dueAt,
      ),
    );
    await store.put(
      SyncRecord(
        operation: operation(id: 'future', createdAt: DateTime.utc(2026, 1, 2)),
        nextAttemptAt: dueAt.add(const Duration(minutes: 1)),
      ),
    );
    await store.put(
      SyncRecord(
        operation: operation(id: 'failed', createdAt: DateTime.utc(2026, 1, 3)),
        status: SyncStatus.failed,
      ),
    );

    final pending = await store.readPending(dueAt: dueAt);

    expect(pending.map((record) => record.operation.id), <String>['due']);
    expect(storage.readPendingCalls, 1);
    expect(storage.readAllCalls, 0);
  });

  test('sync engine can drain with a JSON sync store', () async {
    final store = JsonSyncStore(InMemorySyncJsonStorage());
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(store: store, transport: transport);

    await engine.enqueue(operation(), syncImmediately: false);
    await engine.drain();

    expect(transport.sent, hasLength(1));
    expect(await store.readAll(), isEmpty);

    await engine.dispose();
  });

  test('read entity state aggregates records by attention priority', () async {
    final store = InMemorySyncStore();
    final engine = SyncEngine(
      store: store,
      transport: FakeTransport((_) async => const SyncResult.success()),
    );

    await store.put(
      SyncRecord(
        operation: operation(id: 'pending', createdAt: DateTime.utc(2026)),
      ),
    );
    await store.put(
      SyncRecord(
        operation: operation(id: 'failed', createdAt: DateTime.utc(2026, 1, 2)),
        status: SyncStatus.failed,
      ),
    );
    await store.put(
      SyncRecord(
        operation: SyncOperation(
          id: 'other',
          entity: const SyncEntityRef(type: 'task', id: 'other-task'),
          type: SyncOperationType.update,
          payload: const <String, Object?>{},
          createdAt: DateTime.utc(2026, 1, 3),
        ),
        status: SyncStatus.conflicted,
      ),
    );

    final state = await engine.readEntityState(
      const SyncEntityRef(type: 'task', id: 'task-1'),
    );
    final otherState = await engine.readEntityState(
      const SyncEntityRef(type: 'task', id: 'missing'),
    );

    expect(state.status, SyncEntityStatus.failed);
    expect(state.primaryRecord?.operation.id, 'failed');
    expect(state.hasQueuedWork, isTrue);
    expect(state.needsAttention, isTrue);
    expect(state.records.map((record) => record.operation.id), <String>[
      'failed',
      'pending',
    ]);
    expect(otherState.status, SyncEntityStatus.synced);
    expect(otherState.hasQueuedWork, isFalse);

    await engine.dispose();
  });

  test('read entity records returns prioritized entity records', () async {
    final store = InMemorySyncStore();
    final engine = SyncEngine(
      store: store,
      transport: FakeTransport((_) async => const SyncResult.success()),
    );
    final entity = const SyncEntityRef(type: 'task', id: 'task-1');
    final other = const SyncEntityRef(type: 'task', id: 'other-task');

    await store.put(
      SyncRecord(
        operation: operation(
          id: 'pending',
          entity: entity,
          createdAt: DateTime.utc(2026),
        ),
      ),
    );
    await store.put(
      SyncRecord(
        operation: operation(
          id: 'failed',
          entity: entity,
          createdAt: DateTime.utc(2026, 1, 2),
        ),
        status: SyncStatus.failed,
      ),
    );
    await store.put(
      SyncRecord(
        operation: operation(
          id: 'conflicted',
          entity: entity,
          createdAt: DateTime.utc(2026, 1, 3),
        ),
        status: SyncStatus.conflicted,
      ),
    );
    await store.put(
      SyncRecord(
        operation: operation(id: 'other', entity: other),
        status: SyncStatus.failed,
      ),
    );

    final records = await engine.readEntityRecords(entity);
    final missing = await engine.readEntityRecords(
      const SyncEntityRef(type: 'task', id: 'missing'),
    );

    expect(records.map((record) => record.operation.id), <String>[
      'conflicted',
      'failed',
      'pending',
    ]);
    expect(missing, isEmpty);

    await engine.dispose();
  });

  test('read records can filter by status and entity', () async {
    final store = InMemorySyncStore();
    final engine = SyncEngine(
      store: store,
      transport: FakeTransport((_) async => const SyncResult.success()),
    );
    const task = SyncEntityRef(type: 'task', id: 'task-1');
    const invoice = SyncEntityRef(type: 'invoice', id: 'invoice-1');

    await store.put(
      SyncRecord(
        operation: operation(
          id: 'pending-task',
          entity: task,
          createdAt: DateTime.utc(2026, 1, 3),
        ),
      ),
    );
    await store.put(
      SyncRecord(
        operation: operation(
          id: 'failed-invoice',
          entity: invoice,
          createdAt: DateTime.utc(2026, 1),
        ),
        status: SyncStatus.failed,
      ),
    );
    await store.put(
      SyncRecord(
        operation: operation(
          id: 'conflicted-task',
          entity: task,
          createdAt: DateTime.utc(2026, 1, 2),
        ),
        status: SyncStatus.conflicted,
      ),
    );
    await store.put(
      SyncRecord(
        operation: operation(
          id: 'failed-task',
          entity: task,
          createdAt: DateTime.utc(2026, 1, 4),
        ),
        status: SyncStatus.failed,
      ),
    );

    final attentionRecords = await engine.readRecords(
      statuses: {SyncStatus.failed, SyncStatus.conflicted},
    );
    final taskFailures = await engine.readRecords(
      entity: task,
      statuses: {SyncStatus.failed},
    );
    final empty = await engine.readRecords(statuses: <SyncStatus>{});

    expect(attentionRecords.map((record) => record.operation.id), <String>[
      'failed-invoice',
      'conflicted-task',
      'failed-task',
    ]);
    expect(taskFailures.map((record) => record.operation.id), <String>[
      'failed-task',
    ]);
    expect(empty, isEmpty);

    await engine.dispose();
  });

  test(
    'watch records refreshes when records leave the status filter',
    () async {
      final store = InMemorySyncStore();
      final transport = FakeTransport((_) async => const SyncResult.success());
      final engine = SyncEngine(store: store, transport: transport);

      await store.put(
        SyncRecord(
          operation: operation(id: 'failed'),
          status: SyncStatus.failed,
          attempts: 1,
          lastFailure: const SyncFailure(message: 'Needs retry'),
        ),
      );

      final records = engine
          .watchRecords(statuses: {SyncStatus.failed})
          .map(
            (records) => records.map((record) => record.operation.id).toList(),
          )
          .take(2);
      final expectation = expectLater(
        records,
        emitsInOrder(<List<String>>[
          <String>['failed'],
          <String>[],
        ]),
      );

      await Future<void>.delayed(Duration.zero);
      await engine.retryFailedOperation('failed', syncImmediately: false);
      await expectation;

      await engine.dispose();
    },
  );

  test(
    'watch records scoped to one entity ignores unrelated entity changes',
    () async {
      final store = InMemorySyncStore();
      final transport = FakeTransport((_) async => const SyncResult.success());
      final engine = SyncEngine(store: store, transport: transport);
      const task = SyncEntityRef(type: 'task', id: 'task-1');
      const invoice = SyncEntityRef(type: 'invoice', id: 'invoice-1');
      final snapshots = engine
          .watchRecords(entity: task)
          .map(
            (records) => records.map((record) => record.operation.id).toList(),
          )
          .take(2);
      final expectation = expectLater(
        snapshots,
        emitsInOrder(<List<String>>[
          <String>[],
          <String>['task-work'],
        ]),
      );

      await Future<void>.delayed(Duration.zero);
      await engine.enqueue(
        operation(id: 'invoice-work', entity: invoice),
        syncImmediately: false,
      );
      await Future<void>.delayed(Duration.zero);

      await engine.enqueue(
        operation(id: 'task-work', entity: task),
        syncImmediately: false,
      );
      await expectation;

      await engine.dispose();
    },
  );

  test('watch entity state emits initial and changed snapshots', () async {
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(store: store, transport: transport);
    final entity = const SyncEntityRef(type: 'task', id: 'task-1');
    final states = engine
        .watchEntityState(entity)
        .map((state) => state.status)
        .take(4);
    final expectation = expectLater(
      states,
      emitsInOrder(<SyncEntityStatus>[
        SyncEntityStatus.synced,
        SyncEntityStatus.pending,
        SyncEntityStatus.syncing,
        SyncEntityStatus.synced,
      ]),
    );

    await Future<void>.delayed(Duration.zero);
    await engine.enqueue(operation(), syncImmediately: false);
    await engine.drain();
    await expectation;

    await engine.dispose();
  });

  test('watch entity records emits initial and changed records', () async {
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(store: store, transport: transport);
    final entity = const SyncEntityRef(type: 'task', id: 'task-1');
    final records = engine
        .watchEntityRecords(entity)
        .map((records) => records.map((record) => record.status).toList())
        .take(4);
    final expectation = expectLater(
      records,
      emitsInOrder(<List<SyncStatus>>[
        <SyncStatus>[],
        <SyncStatus>[SyncStatus.pending],
        <SyncStatus>[SyncStatus.syncing],
        <SyncStatus>[],
      ]),
    );

    await Future<void>.delayed(Duration.zero);
    await engine.enqueue(operation(entity: entity), syncImmediately: false);
    await engine.drain();
    await expectation;

    await engine.dispose();
  });

  test(
    'read queue snapshot counts records and chooses global status',
    () async {
      final store = InMemorySyncStore();
      final engine = SyncEngine(
        store: store,
        transport: FakeTransport((_) async => const SyncResult.success()),
      );
      final firstRetry = DateTime.utc(2026, 6, 30, 12);
      final secondRetry = firstRetry.add(const Duration(minutes: 1));

      await store.put(
        SyncRecord(
          operation: operation(id: 'pending-1', createdAt: DateTime.utc(2026)),
          nextAttemptAt: secondRetry,
        ),
      );
      await store.put(
        SyncRecord(
          operation: operation(
            id: 'pending-2',
            createdAt: DateTime.utc(2026, 1, 2),
          ),
          nextAttemptAt: firstRetry,
        ),
      );
      await store.put(
        SyncRecord(
          operation: operation(
            id: 'syncing',
            createdAt: DateTime.utc(2026, 1, 3),
          ),
          status: SyncStatus.syncing,
        ),
      );
      await store.put(
        SyncRecord(
          operation: operation(
            id: 'failed',
            createdAt: DateTime.utc(2026, 1, 4),
          ),
          status: SyncStatus.failed,
        ),
      );
      await store.put(
        SyncRecord(
          operation: operation(
            id: 'conflicted',
            createdAt: DateTime.utc(2026, 1, 5),
          ),
          status: SyncStatus.conflicted,
        ),
      );

      final snapshot = await engine.readQueueSnapshot();

      expect(snapshot.status, SyncQueueStatus.conflicted);
      expect(snapshot.totalCount, 5);
      expect(snapshot.pendingCount, 2);
      expect(snapshot.syncingCount, 1);
      expect(snapshot.failedCount, 1);
      expect(snapshot.conflictedCount, 1);
      expect(snapshot.syncedCount, 0);
      expect(snapshot.nextAttemptAt, firstRetry);
      expect(snapshot.hasPendingWork, isTrue);
      expect(snapshot.isSyncing, isTrue);
      expect(snapshot.needsAttention, isTrue);

      await engine.dispose();
    },
  );

  test('watch queue snapshot emits initial and changed snapshots', () async {
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(store: store, transport: transport);
    final snapshots = engine
        .watchQueueSnapshot()
        .map((snapshot) => snapshot.status)
        .take(4);
    final expectation = expectLater(
      snapshots,
      emitsInOrder(<SyncQueueStatus>[
        SyncQueueStatus.idle,
        SyncQueueStatus.pending,
        SyncQueueStatus.syncing,
        SyncQueueStatus.idle,
      ]),
    );

    await Future<void>.delayed(Duration.zero);
    await engine.enqueue(operation(), syncImmediately: false);
    await engine.drain();
    await expectation;

    await engine.dispose();
  });

  test('read sync state combines queue and engine snapshots', () async {
    final store = InMemorySyncStore();
    final engine = SyncEngine(
      store: store,
      transport: FakeTransport((_) async => const SyncResult.success()),
    );

    await store.put(
      SyncRecord(
        operation: operation(id: 'failed'),
        status: SyncStatus.failed,
        lastFailure: const SyncFailure(message: 'Needs attention'),
      ),
    );

    final snapshot = await engine.readSyncState();

    expect(snapshot.engine.status, SyncEngineStatus.idle);
    expect(snapshot.queue.status, SyncQueueStatus.failed);
    expect(snapshot.isIdle, isFalse);
    expect(snapshot.isSyncing, isFalse);
    expect(snapshot.hasPendingWork, isFalse);
    expect(snapshot.needsAttention, isTrue);
    expect(snapshot.lastDrainWasSkipped, isFalse);

    await engine.dispose();
  });

  test('watch sync state emits queue and engine lifecycle changes', () async {
    final store = InMemorySyncStore();
    final connectivity = ManualSyncConnectivity(SyncConnectivityStatus.offline);
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(
      store: store,
      transport: transport,
      connectivity: connectivity,
    );
    final states = engine
        .watchSyncState()
        .map(
          (state) => '${state.queue.status.name}:${state.lastDrainWasSkipped}',
        )
        .take(3);
    final expectation = expectLater(
      states,
      emitsInOrder(<String>['idle:false', 'pending:false', 'pending:true']),
    );

    await Future<void>.delayed(Duration.zero);
    await engine.enqueue(operation(), syncImmediately: false);
    await Future<void>.delayed(Duration.zero);
    await engine.drain();
    await expectation;

    final finalState = await engine.readSyncState();
    expect(finalState.queue.status, SyncQueueStatus.pending);
    expect(finalState.lastDrainWasSkipped, isTrue);

    await engine.dispose();
    await connectivity.dispose();
  });

  test('watch sync state can complete before engine disposal', () async {
    final store = InMemorySyncStore();
    final transport = FakeTransport((_) async => const SyncResult.success());
    final engine = SyncEngine(store: store, transport: transport);
    final states = engine
        .watchSyncState()
        .map((state) => state.queue.status)
        .take(1);
    final expectation = expectLater(
      states,
      emitsInOrder(<Object>[SyncQueueStatus.idle, emitsDone]),
    );

    await expectation;
    await engine.enqueue(operation(), syncImmediately: false);

    expect(
      (await engine.readSyncState()).queue.status,
      SyncQueueStatus.pending,
    );

    await engine.dispose();
  });

  test('conflict retry updates operation and returns it to pending', () async {
    var sendCount = 0;
    final store = InMemorySyncStore();
    final transport = FakeTransport((operation) async {
      sendCount += 1;
      if (sendCount == 1) {
        return const SyncResult.conflict(
          message: 'Server version changed',
          local: <String, Object?>{'title': 'Local'},
          remote: <String, Object?>{'title': 'Remote'},
        );
      }

      return const SyncResult.success();
    });
    final engine = SyncEngine(store: store, transport: transport);

    await engine.enqueue(operation(), syncImmediately: false);
    await engine.drain();

    final conflicted = (await store.readAll()).single;
    expect(conflicted.status, SyncStatus.conflicted);
    expect(conflicted.attempts, 1);

    final resolved = await engine.resolveConflict(
      'op-1',
      const SyncConflictResolution.retry(
        payload: <String, Object?>{'title': 'Merged'},
      ),
      syncImmediately: false,
    );

    expect(resolved?.status, SyncStatus.pending);
    expect(resolved?.attempts, 0);
    expect(resolved?.conflict, isNull);
    expect(resolved?.operation.payload, const <String, Object?>{
      'title': 'Merged',
    });

    await engine.drain();

    expect(transport.sent, hasLength(2));
    expect(transport.sent.last.payload, const <String, Object?>{
      'title': 'Merged',
    });
    expect(await store.readAll(), isEmpty);
    await engine.dispose();
  });

  test('conflict discard removes local operation and emits synced', () async {
    final store = InMemorySyncStore();
    final transport = FakeTransport(
      (_) async => const SyncResult.conflict(message: 'Server wins'),
    );
    final engine = SyncEngine(store: store, transport: transport);
    final emitted = <SyncStatus>[];
    final subscription = engine.events.listen(
      (record) => emitted.add(record.status),
    );

    await engine.enqueue(operation(), syncImmediately: false);
    await engine.drain();

    final resolved = await engine.resolveConflict(
      'op-1',
      const SyncConflictResolution.discard(),
    );

    expect(resolved?.status, SyncStatus.synced);
    expect(await store.readAll(), isEmpty);
    expect(emitted, contains(SyncStatus.synced));

    await subscription.cancel();
    await engine.dispose();
  });

  test('conflict fail keeps operation with final failure', () async {
    final store = InMemorySyncStore();
    final transport = FakeTransport(
      (_) async => const SyncResult.conflict(message: 'Cannot merge'),
    );
    final engine = SyncEngine(store: store, transport: transport);

    await engine.enqueue(operation(), syncImmediately: false);
    await engine.drain();

    final resolved = await engine.resolveConflict(
      'op-1',
      const SyncConflictResolution.fail(
        SyncFailure(
          message: 'Manual merge rejected',
          code: 'merge_rejected',
          isRetryable: false,
        ),
      ),
    );
    final record = (await store.readAll()).single;

    expect(resolved?.status, SyncStatus.failed);
    expect(record.status, SyncStatus.failed);
    expect(record.conflict, isNull);
    expect(record.lastFailure?.code, 'merge_rejected');
    expect(record.lastFailure?.isRetryable, isFalse);

    await engine.dispose();
  });

  test('conflict resolution rejects non-conflicted operations', () async {
    final store = InMemorySyncStore();
    final engine = SyncEngine(
      store: store,
      transport: FakeTransport((_) async => const SyncResult.success()),
    );

    await engine.enqueue(operation(), syncImmediately: false);

    expect(
      () => engine.resolveConflict(
        'op-1',
        const SyncConflictResolution.discard(),
      ),
      throwsA(isA<StateError>()),
    );
    expect(
      await engine.resolveConflict(
        'missing',
        const SyncConflictResolution.discard(),
      ),
      isNull,
    );

    await engine.dispose();
  });
}
