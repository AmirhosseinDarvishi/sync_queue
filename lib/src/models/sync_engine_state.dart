import 'sync_drain_result.dart';

/// Runtime lifecycle status for a [SyncEngine].
enum SyncEngineStatus {
  /// The engine is not currently sending queued work.
  idle,

  /// The engine is currently draining due queue records.
  draining,

  /// The engine has been disposed and will not process more work.
  disposed,
}

/// UI and diagnostics snapshot for the sync engine itself.
class SyncEngineState {
  const SyncEngineState({required this.status, this.lastDrainResult});

  const SyncEngineState.idle({this.lastDrainResult})
    : status = SyncEngineStatus.idle;

  const SyncEngineState.draining({this.lastDrainResult})
    : status = SyncEngineStatus.draining;

  const SyncEngineState.disposed({this.lastDrainResult})
    : status = SyncEngineStatus.disposed;

  final SyncEngineStatus status;
  final SyncDrainResult? lastDrainResult;

  bool get isIdle => status == SyncEngineStatus.idle;

  bool get isDraining => status == SyncEngineStatus.draining;

  bool get isDisposed => status == SyncEngineStatus.disposed;

  bool get hasLastDrainResult => lastDrainResult != null;

  bool get lastDrainWasSkipped => lastDrainResult?.wasSkipped ?? false;

  SyncEngineState copyWith({
    SyncEngineStatus? status,
    SyncDrainResult? lastDrainResult,
    bool clearLastDrainResult = false,
  }) {
    return SyncEngineState(
      status: status ?? this.status,
      lastDrainResult: clearLastDrainResult
          ? null
          : lastDrainResult ?? this.lastDrainResult,
    );
  }
}
