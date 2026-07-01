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
import 'models/sync_state_snapshot.dart';
import 'retry_policy.dart';
import 'sync_connectivity.dart';
import 'sync_operation_id_generator.dart';
import 'sync_store.dart';
import 'sync_transport.dart';

/// Supplies the current time for records, retries, and deterministic tests.
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
    this.sendTimeout,
    this.autoDrainOnConnectivityRestored = true,
    this.autoDrainOnRetry = true,
    SyncTimerFactory? timerFactory,
    RetryJitter? retryJitter,
    Clock? clock,
  }) : connectivity = connectivity ?? const AlwaysOnlineSyncConnectivity(),
       _timerFactory = timerFactory ?? Timer.new,
       _retryJitter = retryJitter ?? math.Random().nextDouble,
       _clock = clock ?? DateTime.now {
    final timeout = sendTimeout;
    assert(timeout == null || !timeout.isNegative);

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

  /// Maximum time to wait for one transport send before treating it as failed.
  final Duration? sendTimeout;

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

  /// Reads queued records filtered by entity and/or lifecycle status.
  Future<List<SyncRecord>> readRecords({
    SyncEntityRef? entity,
    Set<SyncStatus>? statuses,
  }) async {
    final records = (await store.readAll())
        .where((record) {
          if (entity != null && record.operation.entity != entity) {
            return false;
          }

          if (statuses != null && !statuses.contains(record.status)) {
            return false;
          }

          return true;
        })
        .toList(growable: false);
    records.sort(_compareRecordsByCreatedAt);
    return records;
  }

  /// Emits initial filtered records, then refreshes on matching entity changes.
  ///
  /// Status transitions are not filtered before refresh, so records leaving the
  /// requested [statuses] still cause a fresh list to be emitted.
  Stream<List<SyncRecord>> watchRecords({
    SyncEntityRef? entity,
    Set<SyncStatus>? statuses,
  }) async* {
    yield await readRecords(entity: entity, statuses: statuses);

    final source = entity == null ? events : watchEntity(entity);
    await for (final _ in source) {
      yield await readRecords(entity: entity, statuses: statuses);
    }
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

  /// Reads a combined queue and engine lifecycle snapshot.
  Future<SyncStateSnapshot> readSyncState() async {
    return SyncStateSnapshot(
      queue: await readQueueSnapshot(),
      engine: engineState,
    );
  }

  /// Reads the next runnable retry time for custom schedulers.
  ///
  /// Returns `null` when there are no retryable pending records that are not
  /// blocked by older work for the same entity.
  Future<DateTime?> readNextRetryAt() {
    return _readNextRunnableRetryAt();
  }

  /// Emits initial sync state, then refreshes when queue or engine state changes.
  Stream<SyncStateSnapshot> watchSyncState() async* {
    yield await readSyncState();

    final changes = StreamController<void>();
    var openSources = 2;

    void emitChange() {
      if (!changes.isClosed) {
        changes.add(null);
      }
    }

    void markSourceDone() {
      openSources -= 1;
      if (openSources == 0 && !changes.isClosed) {
        unawaited(changes.close());
      }
    }

    final eventSubscription = events.listen(
      (_) => emitChange(),
      onDone: markSourceDone,
    );
    final engineSubscription = engineStates.listen(
      (_) => emitChange(),
      onDone: markSourceDone,
    );

    try {
      await for (final _ in changes.stream) {
        yield await readSyncState();
      }
    } finally {
      await eventSubscription.cancel();
      await engineSubscription.cancel();
      if (!changes.isClosed) {
        unawaited(changes.close());
      }
    }
  }

  /// Adds an operation to the durable queue.
  Future<SyncRecord> enqueue(
    SyncOperation operation, {
    bool syncImmediately = true,
  }) async {
    await _ensureOperationIdAvailable(operation.id);
    final record = SyncRecord(operation: operation, updatedAt: _clock());
    await _saveAndEmit(record);

    if (syncImmediately) {
      await drain();
    }

    return record;
  }

  Future<void> _ensureOperationIdAvailable(String operationId) async {
    if (await store.read(operationId) != null) {
      throw StateError('Operation "$operationId" is already queued.');
    }
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

  /// Creates and enqueues a create mutation.
  Future<SyncRecord> enqueueCreate({
    required SyncEntityRef entity,
    required Map<String, Object?> payload,
    Map<String, Object?> headers = const <String, Object?>{},
    bool syncImmediately = true,
  }) {
    return enqueueMutation(
      entity: entity,
      type: SyncOperationType.create,
      payload: payload,
      headers: headers,
      syncImmediately: syncImmediately,
    );
  }

  /// Creates and enqueues an update mutation.
  Future<SyncRecord> enqueueUpdate({
    required SyncEntityRef entity,
    required Map<String, Object?> payload,
    Map<String, Object?> headers = const <String, Object?>{},
    bool syncImmediately = true,
  }) {
    return enqueueMutation(
      entity: entity,
      type: SyncOperationType.update,
      payload: payload,
      headers: headers,
      syncImmediately: syncImmediately,
    );
  }

  /// Creates and enqueues a delete mutation.
  Future<SyncRecord> enqueueDelete({
    required SyncEntityRef entity,
    Map<String, Object?> payload = const <String, Object?>{},
    Map<String, Object?> headers = const <String, Object?>{},
    bool syncImmediately = true,
  }) {
    return enqueueMutation(
      entity: entity,
      type: SyncOperationType.delete,
      payload: payload,
      headers: headers,
      syncImmediately: syncImmediately,
    );
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

  /// Returns interrupted syncing operations to the pending queue.
  ///
  /// Use this on app startup when a durable store may contain operations left
  /// in `syncing` after the app or process stopped mid-send. Pass [staleAfter]
  /// to avoid touching operations that may still belong to an active engine.
  Future<List<SyncRecord>> recoverInterruptedOperations({
    Duration? staleAfter,
    bool syncImmediately = true,
  }) async {
    if (_isDraining) {
      throw StateError('Cannot recover interrupted operations while draining.');
    }

    final now = _clock();
    final records = await store.readAll();
    final recovered = <SyncRecord>[];

    for (final record in records) {
      if (record.status != SyncStatus.syncing) {
        continue;
      }

      if (staleAfter != null && record.updatedAt.add(staleAfter).isAfter(now)) {
        continue;
      }

      final pending = record.copyWith(
        status: SyncStatus.pending,
        clearNextAttemptAt: true,
        clearLastFailure: true,
        clearConflict: true,
        updatedAt: now,
      );
      await _saveAndEmit(pending);
      recovered.add(pending);
    }

    if (syncImmediately && recovered.isNotEmpty) {
      await drain();
    }

    return recovered;
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

  /// Returns failed operations to the pending queue for a bulk retry action.
  ///
  /// Pass [entity] to retry only failed operations for one domain entity.
  Future<List<SyncRecord>> retryFailedOperations({
    SyncEntityRef? entity,
    bool resetAttempts = true,
    bool syncImmediately = true,
  }) async {
    final records = await store.readAll();
    final retried = <SyncRecord>[];

    for (final record in records) {
      if (record.status != SyncStatus.failed) {
        continue;
      }

      if (entity != null && record.operation.entity != entity) {
        continue;
      }

      final pending = record.copyWith(
        status: SyncStatus.pending,
        attempts: resetAttempts ? 0 : record.attempts,
        clearNextAttemptAt: true,
        clearLastFailure: true,
        clearConflict: true,
        updatedAt: _clock(),
      );
      await _saveAndEmit(pending);
      retried.add(pending);
    }

    if (syncImmediately && retried.isNotEmpty) {
      await drain();
    }

    return retried;
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

  /// Removes failed operations without sending them.
  ///
  /// Pass [entity] to discard only failed operations for one domain entity.
  Future<List<SyncRecord>> discardFailedOperations({
    SyncEntityRef? entity,
  }) async {
    final records = await store.readAll();
    final discarded = <SyncRecord>[];

    for (final record in records) {
      if (record.status != SyncStatus.failed) {
        continue;
      }

      if (entity != null && record.operation.entity != entity) {
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
        final dueRecords = await _readRunnablePendingRecords(
          dueAt: dueAt,
          include: currentInclude,
        );
        final records = remaining == null || dueRecords.length <= remaining
            ? dueRecords
            : dueRecords.take(remaining).toList(growable: false);
        reachedLimit = remaining != null && dueRecords.length > remaining;

        final blockedEntities = <SyncEntityRef>{};
        for (final record in records) {
          if (blockedEntities.contains(record.operation.entity)) {
            continue;
          }

          final outcome = await _process(record);
          processedCount += 1;
          switch (outcome) {
            case _SyncProcessOutcome.succeeded:
              succeededCount += 1;
            case _SyncProcessOutcome.retryScheduled:
              retryScheduledCount += 1;
            case _SyncProcessOutcome.failed:
              failedCount += 1;
            case _SyncProcessOutcome.conflicted:
              conflictedCount += 1;
          }

          if (outcome != _SyncProcessOutcome.succeeded) {
            blockedEntities.add(record.operation.entity);
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

  Future<List<SyncRecord>> _readRunnablePendingRecords({
    required DateTime dueAt,
    required bool Function(SyncRecord record) include,
  }) async {
    final duePendingRecords = await store.readPending(dueAt: dueAt);
    if (duePendingRecords.isEmpty) {
      return const <SyncRecord>[];
    }

    final duePendingIds = {
      for (final record in duePendingRecords) record.operation.id,
    };
    final records = await store.readAll();
    final blockedEntities = <SyncEntityRef>{};
    final runnable = <SyncRecord>[];

    for (final record in records) {
      final entity = record.operation.entity;
      if (blockedEntities.contains(entity)) {
        continue;
      }

      switch (record.status) {
        case SyncStatus.pending:
          if (!duePendingIds.contains(record.operation.id)) {
            blockedEntities.add(entity);
            continue;
          }

          if (!include(record)) {
            blockedEntities.add(entity);
            continue;
          }

          runnable.add(record);
        case SyncStatus.syncing:
        case SyncStatus.failed:
        case SyncStatus.conflicted:
          blockedEntities.add(entity);
        case SyncStatus.synced:
          break;
      }
    }

    return runnable;
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
      final result = await _send(attempt.operation);
      return await _applyResult(attempt, result);
    } on Object catch (error) {
      return await _handleFailure(
        attempt,
        SyncFailure(message: error.toString(), cause: error),
      );
    }
  }

  Future<SyncResult> _send(SyncOperation operation) {
    final send = transport.send(operation);
    final timeout = sendTimeout;
    if (timeout == null) {
      return send;
    }

    return send.timeout(
      timeout,
      onTimeout: () => SyncResult.failure(
        SyncFailure(
          message: 'Sync operation timed out.',
          code: 'timeout',
          isRetryable: true,
        ),
      ),
    );
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

    final nextAttemptAt = await _readNextRunnableRetryAt();

    if (nextAttemptAt == null) {
      _cancelRetryTimer();
      return;
    }

    _scheduleRetryAt(nextAttemptAt);
  }

  Future<DateTime?> _readNextRunnableRetryAt() async {
    final records = await store.readAll();
    final blockedEntities = <SyncEntityRef>{};
    DateTime? nextAttemptAt;

    for (final record in records) {
      final entity = record.operation.entity;
      if (blockedEntities.contains(entity)) {
        continue;
      }

      switch (record.status) {
        case SyncStatus.pending:
          blockedEntities.add(entity);
          final attemptAt = record.nextAttemptAt;
          if (attemptAt == null) {
            continue;
          }

          if (nextAttemptAt == null || attemptAt.isBefore(nextAttemptAt)) {
            nextAttemptAt = attemptAt;
          }
        case SyncStatus.syncing:
        case SyncStatus.failed:
        case SyncStatus.conflicted:
          blockedEntities.add(entity);
        case SyncStatus.synced:
          break;
      }
    }

    return nextAttemptAt;
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

  int _compareRecordsByCreatedAt(SyncRecord left, SyncRecord right) {
    return left.operation.createdAt.compareTo(right.operation.createdAt);
  }
}

enum _SyncProcessOutcome { succeeded, retryScheduled, failed, conflicted }
