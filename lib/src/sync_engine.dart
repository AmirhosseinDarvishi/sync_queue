import 'dart:async';

import 'models/sync_entity_ref.dart';
import 'models/sync_failure.dart';
import 'models/sync_operation.dart';
import 'models/sync_record.dart';
import 'models/sync_result.dart';
import 'retry_policy.dart';
import 'sync_connectivity.dart';
import 'sync_store.dart';
import 'sync_transport.dart';

typedef Clock = DateTime Function();

/// Coordinates queue persistence, retries, conflict surfacing, and UI status.
class SyncEngine {
  SyncEngine({
    required this.store,
    required this.transport,
    SyncConnectivity? connectivity,
    this.retryPolicy = const RetryPolicy(),
    this.autoDrainOnConnectivityRestored = true,
    Clock? clock,
  }) : connectivity = connectivity ?? const AlwaysOnlineSyncConnectivity(),
       _clock = clock ?? DateTime.now {
    if (autoDrainOnConnectivityRestored) {
      _connectivitySubscription = this.connectivity.changes.listen((status) {
        if (status.isOnline) {
          unawaited(drain());
        }
      });
    }
  }

  final SyncStore store;
  final SyncTransport transport;
  final SyncConnectivity connectivity;
  final RetryPolicy retryPolicy;
  final bool autoDrainOnConnectivityRestored;
  final Clock _clock;
  final _events = StreamController<SyncRecord>.broadcast();
  StreamSubscription<SyncConnectivityStatus>? _connectivitySubscription;
  var _isDraining = false;
  var _isDisposed = false;

  /// Emits every record transition processed by this engine.
  Stream<SyncRecord> get events => _events.stream;

  /// Emits transitions for a specific domain entity.
  Stream<SyncRecord> watchEntity(SyncEntityRef entity) {
    return events.where((record) => record.operation.entity == entity);
  }

  /// Adds an operation to the durable queue.
  Future<SyncRecord> enqueue(
    SyncOperation operation, {
    bool syncImmediately = true,
  }) async {
    final record = SyncRecord(operation: operation, updatedAt: _clock());
    await _saveAndEmit(record);

    if (syncImmediately) {
      await drain();
    }

    return record;
  }

  /// Sends due operations until the queue is caught up for the current moment.
  Future<void> drain({bool force = false}) async {
    if (_isDisposed || _isDraining) {
      return;
    }

    if (!force && !await _canDrain()) {
      return;
    }

    _isDraining = true;
    try {
      final dueAt = _clock();
      final records = await store.readPending(dueAt: dueAt);

      for (final record in records) {
        await _process(record);
      }
    } finally {
      _isDraining = false;
    }
  }

  Future<void> dispose() async {
    _isDisposed = true;
    await _connectivitySubscription?.cancel();
    await _events.close();
  }

  Future<bool> _canDrain() async {
    return (await connectivity.status).isOnline;
  }

  Future<void> _process(SyncRecord record) async {
    final attempt = record.copyWith(
      status: SyncStatus.syncing,
      attempts: record.attempts + 1,
      clearNextAttemptAt: true,
      clearLastFailure: true,
      clearConflict: true,
      updatedAt: _clock(),
    );
    await _saveAndEmit(attempt);

    try {
      final result = await transport.send(attempt.operation);
      await _applyResult(attempt, result);
    } on Object catch (error) {
      await _handleFailure(
        attempt,
        SyncFailure(message: error.toString(), cause: error),
      );
    }
  }

  Future<void> _applyResult(SyncRecord attempt, SyncResult result) async {
    switch (result) {
      case SyncSuccess():
        final synced = attempt.copyWith(
          status: SyncStatus.synced,
          clearNextAttemptAt: true,
          clearLastFailure: true,
          clearConflict: true,
          updatedAt: _clock(),
        );
        _events.add(synced);
        await store.delete(attempt.operation.id);
      case SyncFailureResult(:final failure):
        await _handleFailure(attempt, failure);
      case SyncConflict():
        final conflicted = attempt.copyWith(
          status: SyncStatus.conflicted,
          conflict: result,
          clearNextAttemptAt: true,
          clearLastFailure: true,
          updatedAt: _clock(),
        );
        await _saveAndEmit(conflicted);
    }
  }

  Future<void> _handleFailure(SyncRecord attempt, SyncFailure failure) async {
    if (!retryPolicy.canRetry(
      attempts: attempt.attempts,
      isRetryable: failure.isRetryable,
    )) {
      final failed = attempt.copyWith(
        status: SyncStatus.failed,
        lastFailure: failure,
        clearNextAttemptAt: true,
        updatedAt: _clock(),
      );
      await _saveAndEmit(failed);
      return;
    }

    final retryAt = _clock().add(retryPolicy.delayForAttempt(attempt.attempts));
    final pending = attempt.copyWith(
      status: SyncStatus.pending,
      nextAttemptAt: retryAt,
      lastFailure: failure,
      updatedAt: _clock(),
    );
    await _saveAndEmit(pending);
  }

  Future<void> _saveAndEmit(SyncRecord record) async {
    await store.put(record);
    if (!_isDisposed) {
      _events.add(record);
    }
  }
}
