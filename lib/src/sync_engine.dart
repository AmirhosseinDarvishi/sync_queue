import 'dart:async';
import 'dart:math' as math;

import 'models/sync_conflict_resolution.dart';
import 'models/sync_drain_result.dart';
import 'models/sync_engine_state.dart';
import 'models/sync_entity_ref.dart';
import 'models/sync_entity_state.dart';
import 'models/sync_failure.dart';
import 'models/sync_operation.dart';
import 'models/sync_queue_snapshot.dart';
import 'models/sync_record.dart';
import 'models/sync_result.dart';
import 'retry_policy.dart';
import 'sync_connectivity.dart';
import 'sync_operation_id_generator.dart';
import 'sync_store.dart';
import 'sync_transport.dart';

typedef Clock = DateTime Function();

/// Creates timers used by [SyncEngine] for scheduled retries.
typedef SyncTimerFactory =
    Timer Function(Duration duration, void Function() callback);

/// Coordinates queue persistence, retries, conflict surfacing, and UI status.
class SyncEngine {
  SyncEngine({
    required this.store,
    required this.transport,
    SyncConnectivity? connectivity,
    this.operationIdGenerator = SyncOperationIds.generate,
    this.retryPolicy = const RetryPolicy(),
    this.autoDrainOnConnectivityRestored = true,
    this.autoDrainOnRetry = true,
    SyncTimerFactory? timerFactory,
    RetryJitter? retryJitter,
    Clock? clock,
  }) : connectivity = connectivity ?? const AlwaysOnlineSyncConnectivity(),
       _timerFactory = timerFactory ?? Timer.new,
       _retryJitter = retryJitter ?? math.Random().nextDouble,
       _clock = clock ?? DateTime.now {
    if (autoDrainOnConnectivityRestored) {
      _connectivitySubscription = this.connectivity.changes.listen((status) {
        if (status.isOnline) {
          unawaited(drain());
        }
      });
    }

    if (autoDrainOnRetry) {
      unawaited(_scheduleNextPendingDrain());
    }
  }

  final SyncStore store;
  final SyncTransport transport;
  final SyncConnectivity connectivity;
  final SyncOperationIdGenerator operationIdGenerator;
  final RetryPolicy retryPolicy;
  final bool autoDrainOnConnectivityRestored;
  final bool autoDrainOnRetry;
  final SyncTimerFactory _timerFactory;
  final RetryJitter _retryJitter;
  final Clock _clock;
  final _events = StreamController<SyncRecord>.broadcast();
  final _engineStates = StreamController<SyncEngineState>.broadcast();
  var _engineState = const SyncEngineState.idle();
  StreamSubscription<SyncConnectivityStatus>? _connectivitySubscription;
  Timer? _retryTimer;
  DateTime? _scheduledRetryAt;
  var _isDraining = false;
  var _needsDrainRerun = false;
  var _isDisposed = false;

  /// Emits every record transition processed by this engine.
  Stream<SyncRecord> get events => _events.stream;

  /// Current lifecycle state for this engine.
  SyncEngineState get engineState => _engineState;

  /// Emits lifecycle transitions for this engine.
  Stream<SyncEngineState> get engineStates => _engineStates.stream;

  /// Emits the current engine state, then future lifecycle transitions.
  Stream<SyncEngineState> watchEngineState() async* {
    yield engineState;
    yield* engineStates;
  }

  /// Emits transitions for a specific domain entity.
  Stream<SyncRecord> watchEntity(SyncEntityRef entity) {
    return events.where((record) => record.operation.entity == entity);
  }

  /// Reads an aggregated state snapshot for one entity.
  Future<SyncEntityState> readEntityState(SyncEntityRef entity) async {
    return SyncEntityState.fromRecords(entity, await readEntityRecords(entity));
  }

  /// Reads queued records for one entity in UI priority order.
  Future<List<SyncRecord>> readEntityRecords(SyncEntityRef entity) async {
    final records = await store.readAll();
    return SyncEntityState.fromRecords(
      entity,
      records.where((record) => record.operation.entity == entity),
    ).records;
  }

  /// Emits an initial entity state, then emits a fresh snapshot on each change.
  Stream<SyncEntityState> watchEntityState(SyncEntityRef entity) async* {
    yield await readEntityState(entity);

    await for (final _ in watchEntity(entity)) {
      yield await readEntityState(entity);
    }
  }

  /// Emits initial entity records, then fresh records on each entity change.
  Stream<List<SyncRecord>> watchEntityRecords(SyncEntityRef entity) async* {
    yield await readEntityRecords(entity);

    await for (final _ in watchEntity(entity)) {
      yield await readEntityRecords(entity);
    }
  }

  /// Reads an aggregated snapshot for the whole queue.
  Future<SyncQueueSnapshot> readQueueSnapshot() async {
    return SyncQueueSnapshot.fromRecords(await store.readAll());
  }

  /// Emits an initial queue snapshot, then emits a fresh snapshot on each change.
  Stream<SyncQueueSnapshot> watchQueueSnapshot() async* {
    yield await readQueueSnapshot();

    await for (final _ in events) {
      yield await readQueueSnapshot();
    }
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

  /// Creates and enqueues a mutation with an operation id generated by the engine.
  Future<SyncRecord> enqueueMutation({
    required SyncEntityRef entity,
    required SyncOperationType type,
    required Map<String, Object?> payload,
    Map<String, Object?> headers = const <String, Object?>{},
    bool syncImmediately = true,
  }) async {
    final operation = SyncOperation(
      id: operationIdGenerator(),
      entity: entity,
      type: type,
      payload: payload,
      headers: headers,
      createdAt: _clock(),
    );
    return enqueue(operation, syncImmediately: syncImmediately);
  }

  /// Enqueues a mutation after removing older pending work for the same entity.
  ///
  /// Failed, conflicted, and syncing records are preserved because they need a
  /// separate user or app-level decision.
  Future<SyncRecord> enqueueLatestMutation({
    required SyncEntityRef entity,
    required SyncOperationType type,
    required Map<String, Object?> payload,
    Map<String, Object?> headers = const <String, Object?>{},
    bool syncImmediately = true,
  }) async {
    final operation = SyncOperation(
      id: operationIdGenerator(),
      entity: entity,
      type: type,
      payload: payload,
      headers: headers,
      createdAt: _clock(),
    );
    await _deletePendingForEntity(entity);
    return enqueue(operation, syncImmediately: syncImmediately);
  }

  /// Updates a pending operation before it is sent.
  ///
  /// Returns `null` when the operation is no longer in the queue.
  Future<SyncRecord?> updatePendingOperation(
    String operationId, {
    Map<String, Object?>? payload,
    Map<String, Object?>? headers,
    bool resetAttempts = true,
    bool syncImmediately = true,
  }) async {
    final record = await store.read(operationId);
    if (record == null) {
      return null;
    }

    if (record.status != SyncStatus.pending) {
      throw StateError('Operation "$operationId" is not pending.');
    }

    final pending = record.copyWith(
      operation: record.operation.copyWith(
        payload: payload ?? record.operation.payload,
        headers: headers ?? record.operation.headers,
      ),
      attempts: resetAttempts ? 0 : record.attempts,
      clearNextAttemptAt: true,
      clearLastFailure: true,
      clearConflict: true,
      updatedAt: _clock(),
    );
    await _saveAndEmit(pending);

    if (syncImmediately) {
      await drain();
    }

    return pending;
  }

  /// Resolves a conflicted operation using an app-provided decision.
  ///
  /// Returns `null` when the operation is no longer in the queue.
  Future<SyncRecord?> resolveConflict(
    String operationId,
    SyncConflictResolution resolution, {
    bool syncImmediately = true,
  }) async {
    final record = await store.read(operationId);
    if (record == null) {
      return null;
    }

    if (record.status != SyncStatus.conflicted) {
      throw StateError('Operation "$operationId" is not conflicted.');
    }

    switch (resolution) {
      case SyncConflictRetry(
        :final payload,
        :final headers,
        :final resetAttempts,
      ):
        final pending = record.copyWith(
          operation: record.operation.copyWith(
            payload: payload ?? record.operation.payload,
            headers: headers ?? record.operation.headers,
          ),
          status: SyncStatus.pending,
          attempts: resetAttempts ? 0 : record.attempts,
          clearNextAttemptAt: true,
          clearLastFailure: true,
          clearConflict: true,
          updatedAt: _clock(),
        );
        await _saveAndEmit(pending);

        if (syncImmediately) {
          await drain();
        }

        return pending;
      case SyncConflictDiscard():
        final synced = record.copyWith(
          status: SyncStatus.synced,
          clearNextAttemptAt: true,
          clearLastFailure: true,
          clearConflict: true,
          updatedAt: _clock(),
        );
        await store.delete(operationId);
        _emit(synced);
        return synced;
      case SyncConflictFail(:final failure):
        final failed = record.copyWith(
          status: SyncStatus.failed,
          lastFailure: failure,
          clearNextAttemptAt: true,
          clearConflict: true,
          updatedAt: _clock(),
        );
        await _saveAndEmit(failed);
        return failed;
    }
  }

  /// Returns a failed operation to the pending queue for a user-triggered retry.
  ///
  /// Returns `null` when the operation is no longer in the queue.
  Future<SyncRecord?> retryFailedOperation(
    String operationId, {
    Map<String, Object?>? payload,
    Map<String, Object?>? headers,
    bool resetAttempts = true,
    bool syncImmediately = true,
  }) async {
    final record = await store.read(operationId);
    if (record == null) {
      return null;
    }

    if (record.status != SyncStatus.failed) {
      throw StateError('Operation "$operationId" is not failed.');
    }

    final pending = record.copyWith(
      operation: record.operation.copyWith(
        payload: payload ?? record.operation.payload,
        headers: headers ?? record.operation.headers,
      ),
      status: SyncStatus.pending,
      attempts: resetAttempts ? 0 : record.attempts,
      clearNextAttemptAt: true,
      clearLastFailure: true,
      clearConflict: true,
      updatedAt: _clock(),
    );
    await _saveAndEmit(pending);

    if (syncImmediately) {
      await drain();
    }

    return pending;
  }

  /// Removes a queued operation without sending it.
  ///
  /// Returns a transient synced record for UI streams, or `null` when the
  /// operation is no longer in the queue.
  Future<SyncRecord?> discardOperation(String operationId) async {
    final record = await store.read(operationId);
    if (record == null) {
      return null;
    }

    if (record.status == SyncStatus.syncing) {
      throw StateError('Operation "$operationId" is currently syncing.');
    }

    final discarded = record.copyWith(
      status: SyncStatus.synced,
      clearNextAttemptAt: true,
      clearLastFailure: true,
      clearConflict: true,
      updatedAt: _clock(),
    );
    await store.delete(operationId);
    _emit(discarded);
    await _scheduleNextPendingDrain();
    return discarded;
  }

  /// Removes all pending operations for [entity] without sending them.
  ///
  /// Failed, conflicted, and syncing records are preserved because they need a
  /// separate user or app-level decision.
  Future<List<SyncRecord>> discardPendingForEntity(SyncEntityRef entity) async {
    final records = await store.readAll();
    final discarded = <SyncRecord>[];

    for (final record in records) {
      if (record.status != SyncStatus.pending ||
          record.operation.entity != entity) {
        continue;
      }

      final synced = record.copyWith(
        status: SyncStatus.synced,
        clearNextAttemptAt: true,
        clearLastFailure: true,
        clearConflict: true,
        updatedAt: _clock(),
      );
      await store.delete(record.operation.id);
      _emit(synced);
      discarded.add(synced);
    }

    await _scheduleNextPendingDrain();
    return discarded;
  }

  /// Sends due operations until the queue is caught up for the current moment.
  ///
  /// Set [maxOperations] to process a bounded batch and continue later when
  /// the returned result has `reachedLimit`.
  Future<SyncDrainResult> drain({
    bool force = false,
    int? maxOperations,
  }) async {
    _validateMaxOperations(maxOperations);
    return _drainMatching(
      force: force,
      include: (_) => true,
      rerunWhenBusy: true,
      maxOperations: maxOperations,
    );
  }

  /// Sends due operations for one entity without draining unrelated work.
  Future<SyncDrainResult> drainEntity(
    SyncEntityRef entity, {
    bool force = false,
    int? maxOperations,
  }) async {
    _validateMaxOperations(maxOperations);
    return _drainMatching(
      force: force,
      include: (record) => record.operation.entity == entity,
      maxOperations: maxOperations,
    );
  }

  /// Sends one due operation without draining unrelated work.
  Future<SyncDrainResult> drainOperation(
    String operationId, {
    bool force = false,
  }) async {
    return _drainMatching(
      force: force,
      include: (record) => record.operation.id == operationId,
    );
  }

  Future<SyncDrainResult> _drainMatching({
    required bool force,
    required bool Function(SyncRecord record) include,
    bool rerunWhenBusy = false,
    int? maxOperations,
  }) async {
    if (_isDisposed) {
      return _skipDrain(SyncDrainStatus.skippedDisposed);
    }

    if (_isDraining) {
      if (rerunWhenBusy) {
        _needsDrainRerun = true;
      }

      return _skipDrain(SyncDrainStatus.skippedAlreadyDraining);
    }

    if (!force && !await _canDrain()) {
      return _skipDrain(SyncDrainStatus.skippedOffline);
    }

    _isDraining = true;
    SyncDrainResult? result;
    _setEngineState(
      SyncEngineState.draining(lastDrainResult: _engineState.lastDrainResult),
    );
    try {
      var succeededCount = 0;
      var retryScheduledCount = 0;
      var failedCount = 0;
      var conflictedCount = 0;
      var processedCount = 0;
      var reachedLimit = false;
      var currentInclude = include;

      do {
        _needsDrainRerun = false;
        final remaining = maxOperations == null
            ? null
            : maxOperations - processedCount;
        if (remaining != null && remaining <= 0) {
          reachedLimit = true;
          break;
        }

        final dueAt = _clock();
        final dueRecords = (await store.readPending(
          dueAt: dueAt,
        )).where(currentInclude).toList(growable: false);
        final records = remaining == null || dueRecords.length <= remaining
            ? dueRecords
            : dueRecords.take(remaining).toList(growable: false);
        reachedLimit = remaining != null && dueRecords.length > remaining;
        processedCount += records.length;

        for (final record in records) {
          switch (await _process(record)) {
            case _SyncProcessOutcome.succeeded:
              succeededCount += 1;
            case _SyncProcessOutcome.retryScheduled:
              retryScheduledCount += 1;
            case _SyncProcessOutcome.failed:
              failedCount += 1;
            case _SyncProcessOutcome.conflicted:
              conflictedCount += 1;
          }
        }

        if (_needsDrainRerun) {
          currentInclude = (_) => true;
        }
      } while (_needsDrainRerun && !_isDisposed && !reachedLimit);

      result = SyncDrainResult.completed(
        processedCount: processedCount,
        succeededCount: succeededCount,
        retryScheduledCount: retryScheduledCount,
        failedCount: failedCount,
        conflictedCount: conflictedCount,
        reachedLimit: reachedLimit,
      );
      return result;
    } finally {
      _isDraining = false;
      if (!_isDisposed) {
        _setEngineState(
          SyncEngineState.idle(
            lastDrainResult: result ?? _engineState.lastDrainResult,
          ),
        );
      }
      await _scheduleNextPendingDrain();
    }
  }

  Future<void> dispose() async {
    if (_isDisposed) {
      return;
    }

    _isDisposed = true;
    await _connectivitySubscription?.cancel();
    _cancelRetryTimer();
    _setEngineState(
      SyncEngineState.disposed(lastDrainResult: _engineState.lastDrainResult),
    );
    await _engineStates.close();
    await _events.close();
  }

  Future<bool> _canDrain() async {
    return (await connectivity.status).isOnline;
  }

  void _validateMaxOperations(int? maxOperations) {
    if (maxOperations != null && maxOperations < 1) {
      throw ArgumentError.value(
        maxOperations,
        'maxOperations',
        'Must be greater than zero.',
      );
    }
  }

  Future<_SyncProcessOutcome> _process(SyncRecord record) async {
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
      return await _applyResult(attempt, result);
    } on Object catch (error) {
      return await _handleFailure(
        attempt,
        SyncFailure(message: error.toString(), cause: error),
      );
    }
  }

  Future<_SyncProcessOutcome> _applyResult(
    SyncRecord attempt,
    SyncResult result,
  ) async {
    switch (result) {
      case SyncSuccess():
        final synced = attempt.copyWith(
          status: SyncStatus.synced,
          clearNextAttemptAt: true,
          clearLastFailure: true,
          clearConflict: true,
          updatedAt: _clock(),
        );
        await store.delete(attempt.operation.id);
        _emit(synced);
        return _SyncProcessOutcome.succeeded;
      case SyncFailureResult(:final failure):
        return await _handleFailure(attempt, failure);
      case SyncConflict():
        final conflicted = attempt.copyWith(
          status: SyncStatus.conflicted,
          conflict: result,
          clearNextAttemptAt: true,
          clearLastFailure: true,
          updatedAt: _clock(),
        );
        await _saveAndEmit(conflicted);
        return _SyncProcessOutcome.conflicted;
    }
  }

  Future<_SyncProcessOutcome> _handleFailure(
    SyncRecord attempt,
    SyncFailure failure,
  ) async {
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
      return _SyncProcessOutcome.failed;
    }

    final retryDelay =
        failure.retryAfter ??
        retryPolicy.delayForAttempt(
          attempt.attempts,
          jitter: retryPolicy.jitterFactor == 0 ? 1 : _retryJitter(),
        );
    final retryAt = _clock().add(retryDelay);
    final pending = attempt.copyWith(
      status: SyncStatus.pending,
      nextAttemptAt: retryAt,
      lastFailure: failure,
      updatedAt: _clock(),
    );
    await _saveAndEmit(pending);
    _scheduleRetryAt(retryAt);
    return _SyncProcessOutcome.retryScheduled;
  }

  Future<void> _saveAndEmit(SyncRecord record) async {
    await store.put(record);
    _emit(record);
  }

  Future<void> _deletePendingForEntity(SyncEntityRef entity) async {
    final records = await store.readAll();

    for (final record in records) {
      if (record.status == SyncStatus.pending &&
          record.operation.entity == entity) {
        await store.delete(record.operation.id);
      }
    }

    await _scheduleNextPendingDrain();
  }

  void _emit(SyncRecord record) {
    if (!_isDisposed) {
      _events.add(record);
    }
  }

  SyncDrainResult _skipDrain(SyncDrainStatus status) {
    final result = SyncDrainResult.skipped(status);
    _setEngineState(_engineState.copyWith(lastDrainResult: result));
    return result;
  }

  void _setEngineState(SyncEngineState state) {
    _engineState = state;
    if (!_engineStates.isClosed) {
      _engineStates.add(state);
    }
  }

  Future<void> _scheduleNextPendingDrain() async {
    if (!autoDrainOnRetry || _isDisposed) {
      return;
    }

    final records = await store.readAll();
    DateTime? nextAttemptAt;

    for (final record in records) {
      if (record.status != SyncStatus.pending) {
        continue;
      }

      final attemptAt = record.nextAttemptAt;
      if (attemptAt == null) {
        continue;
      }

      if (nextAttemptAt == null || attemptAt.isBefore(nextAttemptAt)) {
        nextAttemptAt = attemptAt;
      }
    }

    if (nextAttemptAt == null) {
      _cancelRetryTimer();
      return;
    }

    _scheduleRetryAt(nextAttemptAt);
  }

  void _scheduleRetryAt(DateTime retryAt) {
    if (!autoDrainOnRetry || _isDisposed) {
      return;
    }

    final scheduled = _scheduledRetryAt;
    final activeTimer = _retryTimer;
    if (scheduled != null &&
        activeTimer != null &&
        activeTimer.isActive &&
        !retryAt.isBefore(scheduled)) {
      return;
    }

    _cancelRetryTimer();

    final now = _clock();
    final delay = retryAt.isAfter(now)
        ? retryAt.difference(now)
        : Duration.zero;
    _scheduledRetryAt = retryAt;
    _retryTimer = _timerFactory(delay, () {
      _retryTimer = null;
      _scheduledRetryAt = null;
      unawaited(drain());
    });
  }

  void _cancelRetryTimer() {
    _retryTimer?.cancel();
    _retryTimer = null;
    _scheduledRetryAt = null;
  }
}

enum _SyncProcessOutcome { succeeded, retryScheduled, failed, conflicted }
