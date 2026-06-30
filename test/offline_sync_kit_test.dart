import 'package:flutter_test/flutter_test.dart';
import 'package:offline_sync_kit/offline_sync_kit.dart';

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
  SyncOperation operation({String id = 'op-1'}) {
    return SyncOperation(
      id: id,
      entity: const SyncEntityRef(type: 'task', id: 'task-1'),
      type: SyncOperationType.update,
      payload: const <String, Object?>{'title': 'Ship package'},
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
}
