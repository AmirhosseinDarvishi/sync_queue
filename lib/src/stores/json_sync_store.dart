import '../models/sync_record.dart';
import '../sync_json.dart';
import '../sync_store.dart';

/// Low-level JSON persistence boundary used by [JsonSyncStore].
abstract interface class SyncJsonStorage {
  /// Inserts or replaces one encoded queue record.
  Future<void> put(String operationId, SyncJsonMap record);

  /// Reads one encoded queue record by operation id.
  Future<SyncJsonMap?> read(String operationId);

  /// Deletes one encoded queue record by operation id.
  Future<void> delete(String operationId);

  /// Reads every encoded queue record.
  Future<List<SyncJsonMap>> readAll();
}

/// Optional optimized query boundary for JSON-like persistence adapters.
abstract interface class SyncJsonQueryStorage implements SyncJsonStorage {
  /// Reads pending records that should run at or before [dueAt].
  Future<List<SyncJsonMap>> readPending({required DateTime dueAt});
}

/// Sync store backed by a JSON-like storage adapter.
///
/// Use this when your app already has a persistence layer that can store maps,
/// strings, or encoded JSON blobs. Dedicated database adapters can build on top
/// of the same model serializers without depending on this class.
class JsonSyncStore implements SyncStore {
  const JsonSyncStore(this.storage);

  final SyncJsonStorage storage;

  @override
  Future<void> put(SyncRecord record) async {
    await storage.put(record.operation.id, record.toJson());
  }

  @override
  Future<SyncRecord?> read(String operationId) async {
    final json = await storage.read(operationId);
    return json == null ? null : SyncRecord.fromJson(json);
  }

  @override
  Future<List<SyncRecord>> readPending({DateTime? dueAt}) async {
    final now = dueAt ?? DateTime.now();
    final storage = this.storage;

    if (storage is SyncJsonQueryStorage) {
      return _decodeAndSort(await storage.readPending(dueAt: now));
    }

    final records = await readAll();
    return records
        .where((record) {
          if (record.status != SyncStatus.pending) {
            return false;
          }

          final nextAttemptAt = record.nextAttemptAt;
          return nextAttemptAt == null || !nextAttemptAt.isAfter(now);
        })
        .toList(growable: false);
  }

  @override
  Future<void> delete(String operationId) async {
    await storage.delete(operationId);
  }

  @override
  Future<List<SyncRecord>> readAll() async {
    return _decodeAndSort(await storage.readAll());
  }

  List<SyncRecord> _decodeAndSort(List<SyncJsonMap> jsonRecords) {
    final records = jsonRecords
        .map(SyncRecord.fromJson)
        .toList(growable: false);
    records.sort(_compareByCreatedAt);
    return records;
  }

  int _compareByCreatedAt(SyncRecord left, SyncRecord right) {
    return left.operation.createdAt.compareTo(right.operation.createdAt);
  }
}

/// In-memory JSON storage for tests, demos, and custom prototypes.
class InMemorySyncJsonStorage implements SyncJsonQueryStorage {
  /// Creates an empty in-memory JSON storage adapter.
  InMemorySyncJsonStorage();

  final _records = <String, SyncJsonMap>{};

  @override
  Future<void> put(String operationId, SyncJsonMap record) async {
    _records[operationId] = Map<String, Object?>.from(record);
  }

  @override
  Future<SyncJsonMap?> read(String operationId) async {
    final record = _records[operationId];
    return record == null ? null : Map<String, Object?>.from(record);
  }

  @override
  Future<void> delete(String operationId) async {
    _records.remove(operationId);
  }

  @override
  Future<List<SyncJsonMap>> readAll() async {
    return _records.values
        .map((record) => Map<String, Object?>.from(record))
        .toList(growable: false);
  }

  @override
  Future<List<SyncJsonMap>> readPending({required DateTime dueAt}) async {
    return _records.values
        .where((record) => _isDuePendingRecord(record, dueAt))
        .map((record) => Map<String, Object?>.from(record))
        .toList(growable: false);
  }

  bool _isDuePendingRecord(SyncJsonMap record, DateTime dueAt) {
    if (readString(record, 'status') != SyncStatus.pending.wireName) {
      return false;
    }

    final nextAttemptAt = readOptionalDateTime(record, 'nextAttemptAt');
    return nextAttemptAt == null || !nextAttemptAt.isAfter(dueAt);
  }
}
