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
  /// Creates a retry decision for a conflicted operation.
  const SyncConflictRetry({
    this.payload,
    this.headers,
    this.resetAttempts = true,
  });

  /// Replacement payload to send on the next retry.
  final Map<String, Object?>? payload;

  /// Replacement headers to send on the next retry.
  final Map<String, Object?>? headers;

  /// Whether retry attempts should restart from zero after resolution.
  final bool resetAttempts;
}

/// Discard the local operation.
class SyncConflictDiscard extends SyncConflictResolution {
  /// Creates a discard decision for a conflicted operation.
  const SyncConflictDiscard();
}

/// Mark the operation as failed.
class SyncConflictFail extends SyncConflictResolution {
  /// Creates a final failure decision for a conflicted operation.
  const SyncConflictFail(this.failure);

  /// Failure reason to persist on the operation.
  final SyncFailure failure;
}
