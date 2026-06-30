import 'sync_failure.dart';
import 'sync_operation.dart';
import 'sync_result.dart';

/// The lifecycle state for a queued sync operation.
enum SyncStatus { pending, syncing, synced, failed, conflicted }

/// A persisted queue item plus runtime state needed for retries and UI.
class SyncRecord {
  SyncRecord({
    required this.operation,
    this.status = SyncStatus.pending,
    this.attempts = 0,
    this.nextAttemptAt,
    this.lastFailure,
    this.conflict,
    DateTime? updatedAt,
  }) : updatedAt = updatedAt ?? operation.createdAt;

  final SyncOperation operation;
  final SyncStatus status;
  final int attempts;
  final DateTime? nextAttemptAt;
  final SyncFailure? lastFailure;
  final SyncConflict? conflict;
  final DateTime updatedAt;

  bool get isDue {
    final next = nextAttemptAt;
    return next == null || !next.isAfter(DateTime.now());
  }

  SyncRecord copyWith({
    SyncOperation? operation,
    SyncStatus? status,
    int? attempts,
    DateTime? nextAttemptAt,
    bool clearNextAttemptAt = false,
    SyncFailure? lastFailure,
    bool clearLastFailure = false,
    SyncConflict? conflict,
    bool clearConflict = false,
    DateTime? updatedAt,
  }) {
    return SyncRecord(
      operation: operation ?? this.operation,
      status: status ?? this.status,
      attempts: attempts ?? this.attempts,
      nextAttemptAt: clearNextAttemptAt
          ? null
          : nextAttemptAt ?? this.nextAttemptAt,
      lastFailure: clearLastFailure ? null : lastFailure ?? this.lastFailure,
      conflict: clearConflict ? null : conflict ?? this.conflict,
      updatedAt: updatedAt ?? this.updatedAt,
    );
  }
}
