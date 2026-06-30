import 'models/sync_record.dart';

/// Persistence boundary for the operation queue.
abstract interface class SyncStore {
  /// Inserts or replaces one queue record.
  Future<void> put(SyncRecord record);

  /// Reads one record by operation id.
  Future<SyncRecord?> read(String operationId);

  /// Reads pending operations that are ready to run.
  Future<List<SyncRecord>> readPending({DateTime? dueAt});

  /// Deletes one record from the queue.
  Future<void> delete(String operationId);

  /// Reads all records. Mostly useful for diagnostics and tests.
  Future<List<SyncRecord>> readAll();
}
