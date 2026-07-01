import 'dart:async';
import 'dart:developer' as developer;

import 'package:sync_queue/sync_queue.dart';

Future<void> main() async {
  final api = FakeTaskApi();
  final connectivity = ManualSyncConnectivity(SyncConnectivityStatus.offline);
  final engine = SyncEngine(
    store: InMemorySyncStore(),
    transport: api,
    connectivity: connectivity,
  );
  final localTasks = <String, String>{'task-1': 'Draft title'};
  final stateSubscription = engine.watchSyncState().listen((state) {
    developer.log(
      'engine=${state.engine.status.name}, queue=${state.queue.status.name}',
      name: 'sync_queue_example',
    );
  });

  try {
    await SyncOptimistic.run<SyncRecord>(
      apply: () {
        localTasks['task-1'] = 'Offline title';
      },
      commit: () => engine.enqueueUpdate(
        entity: const SyncEntityRef(type: 'task', id: 'task-1'),
        payload: <String, Object?>{'title': localTasks['task-1']},
      ),
      rollback: (_, _) {
        localTasks['task-1'] = 'Draft title';
      },
    );

    final queuedWhileOffline = await engine.readRecords(
      statuses: {SyncStatus.pending},
    );
    developer.log(
      'queued while offline: ${queuedWhileOffline.length}',
      name: 'sync_queue_example',
    );

    final synced = engine.events.firstWhere(
      (record) =>
          record.status == SyncStatus.synced &&
          record.operation.entity.id == 'task-1',
    );
    connectivity.setOnline();
    await synced;

    api.rejectNext = true;
    await engine.enqueueUpdate(
      entity: const SyncEntityRef(type: 'task', id: 'task-1'),
      payload: const <String, Object?>{'title': 'Rejected title'},
    );
    final failed = (await engine.readRecords(
      statuses: {SyncStatus.failed},
    )).single;

    await engine.retryFailedOperation(
      failed.operation.id,
      payload: const <String, Object?>{'title': 'Retried title'},
    );

    api.conflictNext = true;
    await engine.enqueueUpdate(
      entity: const SyncEntityRef(type: 'task', id: 'task-1'),
      payload: const <String, Object?>{'title': 'Local conflict title'},
    );
    final conflicted = (await engine.readRecords(
      statuses: {SyncStatus.conflicted},
    )).single;

    await engine.resolveConflict(
      conflicted.operation.id,
      const SyncConflictResolution.retry(
        payload: <String, Object?>{'title': 'Merged title'},
      ),
    );

    developer.log('remote tasks: ${api.tasks}', name: 'sync_queue_example');
  } finally {
    await stateSubscription.cancel();
    await engine.dispose();
    await connectivity.dispose();
  }
}

class FakeTaskApi implements SyncTransport {
  final tasks = <String, String>{};
  var rejectNext = false;
  var conflictNext = false;

  @override
  Future<SyncResult> send(SyncOperation operation) async {
    if (rejectNext) {
      rejectNext = false;
      return const SyncResult.failure(
        SyncFailure(
          message: 'The server rejected this task title.',
          code: 'invalid_title',
          isRetryable: false,
        ),
      );
    }

    if (conflictNext) {
      conflictNext = false;
      return SyncResult.conflict(
        message: 'The server has a newer task version.',
        local: operation.payload,
        remote: const <String, Object?>{'title': 'Server title'},
      );
    }

    switch (operation.type) {
      case SyncOperationType.create:
      case SyncOperationType.update:
      case SyncOperationType.custom:
        return _upsertTask(operation);
      case SyncOperationType.delete:
        tasks.remove(operation.entity.id);
        return const SyncResult.success();
    }
  }

  SyncResult _upsertTask(SyncOperation operation) {
    final title = operation.payload['title'];
    if (title is! String || title.isEmpty) {
      return const SyncResult.failure(
        SyncFailure(
          message: 'Task title is required.',
          code: 'missing_title',
          isRetryable: false,
        ),
      );
    }

    tasks[operation.entity.id] = title;
    return SyncResult.success(data: operation.entity.id);
  }
}
