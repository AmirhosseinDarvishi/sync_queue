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
  SyncOperation operation({String id = 'op-1', DateTime? createdAt}) {
    return SyncOperation(
      id: id,
      entity: const SyncEntityRef(type: 'task', id: 'task-1'),
      type: SyncOperationType.update,
      payload: const <String, Object?>{'title': 'Ship package'},
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
