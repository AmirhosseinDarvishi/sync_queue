/// High-level outcome for a single drain request.
enum SyncDrainStatus {
  /// The engine checked the queue and processed every due operation it found.
  completed,

  /// The engine ignored the request because it has already been disposed.
  skippedDisposed,

  /// The engine ignored the request because another drain is already running.
  skippedAlreadyDraining,

  /// The engine ignored the request because connectivity is currently offline.
  skippedOffline,
}

/// Summary returned after a drain request finishes or gets skipped.
class SyncDrainResult {
  const SyncDrainResult({
    required this.status,
    this.processedCount = 0,
    this.succeededCount = 0,
    this.retryScheduledCount = 0,
    this.failedCount = 0,
    this.conflictedCount = 0,
    this.reachedLimit = false,
  });

  /// Creates a completed drain summary.
  const SyncDrainResult.completed({
    this.processedCount = 0,
    this.succeededCount = 0,
    this.retryScheduledCount = 0,
    this.failedCount = 0,
    this.conflictedCount = 0,
    this.reachedLimit = false,
  }) : status = SyncDrainStatus.completed;

  /// Creates a skipped drain summary.
  const SyncDrainResult.skipped(this.status)
    : processedCount = 0,
      succeededCount = 0,
      retryScheduledCount = 0,
      failedCount = 0,
      conflictedCount = 0,
      reachedLimit = false,
      assert(status != SyncDrainStatus.completed);

  final SyncDrainStatus status;
  final int processedCount;
  final int succeededCount;
  final int retryScheduledCount;
  final int failedCount;
  final int conflictedCount;
  final bool reachedLimit;

  bool get isCompleted => status == SyncDrainStatus.completed;

  bool get wasSkipped => !isCompleted;

  bool get didWork => processedCount > 0;

  bool get needsAttention => failedCount > 0 || conflictedCount > 0;

  bool get shouldContinue => isCompleted && reachedLimit;
}
