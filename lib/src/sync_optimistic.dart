import 'dart:async';

/// Applies a local optimistic change.
typedef SyncOptimisticApply = FutureOr<void> Function();

/// Commits the durable sync work after an optimistic local change.
typedef SyncOptimisticCommit<T> = Future<T> Function();

/// Rolls back an optimistic local change when commit fails.
typedef SyncOptimisticRollback =
    FutureOr<void> Function(Object error, StackTrace stackTrace);

/// Helpers for pairing optimistic local updates with durable queue commits.
class SyncOptimistic {
  const SyncOptimistic._();

  /// Runs [apply], then [commit], and calls [rollback] if commit fails.
  ///
  /// Use this when local UI/cache state should update immediately, but must be
  /// restored if the operation cannot be persisted to the sync queue.
  static Future<T> run<T>({
    required SyncOptimisticApply apply,
    required SyncOptimisticCommit<T> commit,
    required SyncOptimisticRollback rollback,
  }) async {
    await apply();

    try {
      return await commit();
    } on Object catch (error, stackTrace) {
      await rollback(error, stackTrace);
      Error.throwWithStackTrace(error, stackTrace);
    }
  }
}
