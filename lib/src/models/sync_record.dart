import '../sync_json.dart';
import 'sync_failure.dart';
import 'sync_operation.dart';
import 'sync_result.dart';

/// The lifecycle state for a queued sync operation.
enum SyncStatus { pending, syncing, synced, failed, conflicted }

extension SyncStatusX on SyncStatus {
  String get wireName {
    return switch (this) {
      SyncStatus.pending => 'pending',
      SyncStatus.syncing => 'syncing',
      SyncStatus.synced => 'synced',
      SyncStatus.failed => 'failed',
      SyncStatus.conflicted => 'conflicted',
    };
  }
}

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

  factory SyncRecord.fromJson(SyncJsonMap json) {
    final lastFailureJson = readOptionalJsonMap(json, 'lastFailure');
    final conflictJson = readOptionalJsonMap(json, 'conflict');

    return SyncRecord(
      operation: SyncOperation.fromJson(readJsonMap(json, 'operation')),
      status: _syncStatusFromWireName(readString(json, 'status')),
      attempts: readInt(json, 'attempts'),
      nextAttemptAt: readOptionalDateTime(json, 'nextAttemptAt'),
      lastFailure: lastFailureJson == null
          ? null
          : SyncFailure.fromJson(lastFailureJson),
      conflict: conflictJson == null
          ? null
          : SyncConflict.fromJson(conflictJson),
      updatedAt: readDateTime(json, 'updatedAt'),
    );
  }

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

  SyncJsonMap toJson() {
    return <String, Object?>{
      'operation': operation.toJson(),
      'status': status.wireName,
      'attempts': attempts,
      'nextAttemptAt': nextAttemptAt == null
          ? null
          : writeDateTime(nextAttemptAt!),
      'lastFailure': lastFailure?.toJson(),
      'conflict': conflict?.toJson(),
      'updatedAt': writeDateTime(updatedAt),
    };
  }
}

SyncStatus _syncStatusFromWireName(String value) {
  return switch (value) {
    'pending' => SyncStatus.pending,
    'syncing' => SyncStatus.syncing,
    'synced' => SyncStatus.synced,
    'failed' => SyncStatus.failed,
    'conflicted' => SyncStatus.conflicted,
    _ => throw FormatException('Unknown sync status "$value".'),
  };
}
