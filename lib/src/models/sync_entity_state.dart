import 'sync_entity_ref.dart';
import 'sync_record.dart';

/// Aggregated sync status for one domain entity.
enum SyncEntityStatus {
  /// No queued work is currently stored for the entity.
  synced,

  /// One or more operations are waiting to be sent.
  pending,

  /// One or more operations are currently being sent.
  syncing,

  /// One or more operations failed and require attention or another retry.
  failed,

  /// One or more operations conflicted with remote state.
  conflicted,
}

/// UI-friendly state for one entity derived from all queued records.
class SyncEntityState {
  SyncEntityState({
    required this.entity,
    required this.status,
    List<SyncRecord> records = const <SyncRecord>[],
  }) : records = List<SyncRecord>.unmodifiable(records);

  factory SyncEntityState.synced(SyncEntityRef entity) {
    return SyncEntityState(entity: entity, status: SyncEntityStatus.synced);
  }

  factory SyncEntityState.fromRecords(
    SyncEntityRef entity,
    Iterable<SyncRecord> records,
  ) {
    final sorted = records.toList(growable: false)..sort(_compareRecords);
    if (sorted.isEmpty) {
      return SyncEntityState.synced(entity);
    }

    return SyncEntityState(
      entity: entity,
      status: _entityStatusFor(sorted.first.status),
      records: sorted,
    );
  }

  final SyncEntityRef entity;
  final SyncEntityStatus status;
  final List<SyncRecord> records;

  /// Highest-priority record driving [status], when any record exists.
  SyncRecord? get primaryRecord {
    return records.isEmpty ? null : records.first;
  }

  /// Whether the entity still has local queue work.
  bool get hasQueuedWork => records.isNotEmpty;

  /// Whether the entity requires user or app-level intervention.
  bool get needsAttention {
    return status == SyncEntityStatus.failed ||
        status == SyncEntityStatus.conflicted;
  }
}

int _compareRecords(SyncRecord left, SyncRecord right) {
  final priority = _statusPriority(
    right.status,
  ).compareTo(_statusPriority(left.status));
  if (priority != 0) {
    return priority;
  }

  return left.operation.createdAt.compareTo(right.operation.createdAt);
}

int _statusPriority(SyncStatus status) {
  return switch (status) {
    SyncStatus.conflicted => 4,
    SyncStatus.failed => 3,
    SyncStatus.syncing => 2,
    SyncStatus.pending => 1,
    SyncStatus.synced => 0,
  };
}

SyncEntityStatus _entityStatusFor(SyncStatus status) {
  return switch (status) {
    SyncStatus.conflicted => SyncEntityStatus.conflicted,
    SyncStatus.failed => SyncEntityStatus.failed,
    SyncStatus.syncing => SyncEntityStatus.syncing,
    SyncStatus.pending => SyncEntityStatus.pending,
    SyncStatus.synced => SyncEntityStatus.synced,
  };
}
