import '../models/sync_record.dart';
import '../sync_store.dart';

/// Non-persistent store for tests, prototypes, and examples.
class InMemorySyncStore implements SyncStore {
  final Map<String, SyncRecord> _records = <String, SyncRecord>{};

  @override
  Future<void> put(SyncRecord record) async {
    _records[record.operation.id] = record;
  }

  @override
  Future<SyncRecord?> read(String operationId) async {
    return _records[operationId];
  }

  @override
  Future<List<SyncRecord>> readPending({DateTime? dueAt}) async {
    final now = dueAt ?? DateTime.now();
    return _records.values
        .where((record) {
          if (record.status != SyncStatus.pending) {
            return false;
          }

          final nextAttemptAt = record.nextAttemptAt;
          return nextAttemptAt == null || !nextAttemptAt.isAfter(now);
        })
        .toList(growable: false)
      ..sort((left, right) {
        return left.operation.createdAt.compareTo(right.operation.createdAt);
      });
  }

  @override
  Future<void> delete(String operationId) async {
    _records.remove(operationId);
  }

  @override
  Future<List<SyncRecord>> readAll() async {
    final records = _records.values.toList(growable: false);
    records.sort((left, right) {
      return left.operation.createdAt.compareTo(right.operation.createdAt);
    });
    return records;
  }
}
