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
    await engine.drain();

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
}
