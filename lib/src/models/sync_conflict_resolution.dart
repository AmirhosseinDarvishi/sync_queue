import 'sync_failure.dart';

/// App decision for resolving one conflicted operation.
sealed class SyncConflictResolution {
  const SyncConflictResolution();

  /// Retry the operation, optionally replacing payload or headers first.
  const factory SyncConflictResolution.retry({
    Map<String, Object?>? payload,
    Map<String, Object?>? headers,
    bool resetAttempts,
  }) = SyncConflictRetry;

  /// Drop the local operation, commonly used for server-wins conflict handling.
  const factory SyncConflictResolution.discard() = SyncConflictDiscard;

  /// Move the operation to failed with a final failure reason.
  const factory SyncConflictResolution.fail(SyncFailure failure) =
      SyncConflictFail;
}

/// Retry the operation after optional app-level merge edits.
class SyncConflictRetry extends SyncConflictResolution {
  const SyncConflictRetry({
    this.payload,
    this.headers,
    this.resetAttempts = true,
  });

  final Map<String, Object?>? payload;
  final Map<String, Object?>? headers;
  final bool resetAttempts;
}

/// Discard the local operation.
class SyncConflictDiscard extends SyncConflictResolution {
  const SyncConflictDiscard();
}

/// Mark the operation as failed.
class SyncConflictFail extends SyncConflictResolution {
  const SyncConflictFail(this.failure);

  final SyncFailure failure;
}
