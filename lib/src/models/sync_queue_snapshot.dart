import 'sync_record.dart';

/// Aggregated lifecycle status for the whole sync queue.
enum SyncQueueStatus {
  /// No queued work currently requires sync action.
  idle,

  /// One or more operations are waiting to be sent.
  pending,

  /// One or more operations are currently being sent.
  syncing,

  /// One or more operations failed and need attention or another retry.
  failed,

  /// One or more operations are conflicted and need app-level resolution.
  conflicted,
}

/// UI and diagnostics snapshot for every record currently stored in the queue.
class SyncQueueSnapshot {
  SyncQueueSnapshot({
    required this.status,
    required this.totalCount,
    required this.pendingCount,
    required this.syncingCount,
    required this.syncedCount,
    required this.failedCount,
    required this.conflictedCount,
    this.nextAttemptAt,
  });

  factory SyncQueueSnapshot.fromRecords(Iterable<SyncRecord> records) {
    var totalCount = 0;
    var pendingCount = 0;
    var syncingCount = 0;
    var syncedCount = 0;
    var failedCount = 0;
    var conflictedCount = 0;
    DateTime? nextAttemptAt;

    for (final record in records) {
      totalCount += 1;

      switch (record.status) {
        case SyncStatus.pending:
          pendingCount += 1;
          final attemptAt = record.nextAttemptAt;
          if (attemptAt != null &&
              (nextAttemptAt == null || attemptAt.isBefore(nextAttemptAt))) {
            nextAttemptAt = attemptAt;
          }
        case SyncStatus.syncing:
          syncingCount += 1;
        case SyncStatus.synced:
          syncedCount += 1;
        case SyncStatus.failed:
          failedCount += 1;
        case SyncStatus.conflicted:
          conflictedCount += 1;
      }
    }

    return SyncQueueSnapshot(
      status: _statusFor(
        pendingCount: pendingCount,
        syncingCount: syncingCount,
        failedCount: failedCount,
        conflictedCount: conflictedCount,
      ),
      totalCount: totalCount,
      pendingCount: pendingCount,
      syncingCount: syncingCount,
      syncedCount: syncedCount,
      failedCount: failedCount,
      conflictedCount: conflictedCount,
      nextAttemptAt: nextAttemptAt,
    );
  }

  final SyncQueueStatus status;
  final int totalCount;
  final int pendingCount;
  final int syncingCount;
  final int syncedCount;
  final int failedCount;
  final int conflictedCount;
  final DateTime? nextAttemptAt;

  bool get isIdle => status == SyncQueueStatus.idle;

  bool get isSyncing => syncingCount > 0;

  bool get hasPendingWork => pendingCount > 0 || syncingCount > 0;

  bool get needsAttention => failedCount > 0 || conflictedCount > 0;
}

SyncQueueStatus _statusFor({
  required int pendingCount,
  required int syncingCount,
  required int failedCount,
  required int conflictedCount,
}) {
  if (conflictedCount > 0) {
    return SyncQueueStatus.conflicted;
  }

  if (failedCount > 0) {
    return SyncQueueStatus.failed;
  }

  if (syncingCount > 0) {
    return SyncQueueStatus.syncing;
  }

  if (pendingCount > 0) {
    return SyncQueueStatus.pending;
  }

  return SyncQueueStatus.idle;
}
