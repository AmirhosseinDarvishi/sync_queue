import 'sync_entity_ref.dart';

/// The type of mutation represented by a queued sync operation.
enum SyncOperationType { create, update, delete, custom }

/// A durable local-first mutation that can be replayed against an API.
class SyncOperation {
  SyncOperation({
    required this.id,
    required this.entity,
    required this.type,
    required this.payload,
    this.headers = const <String, Object?>{},
    DateTime? createdAt,
  }) : createdAt = createdAt ?? DateTime.now();

  /// Globally unique id for this queued operation.
  final String id;

  /// Entity affected by this operation.
  final SyncEntityRef entity;

  /// Mutation kind.
  final SyncOperationType type;

  /// Serializable request body or command data.
  final Map<String, Object?> payload;

  /// Extra structured metadata for adapters, such as endpoint names.
  final Map<String, Object?> headers;

  /// When the operation entered the queue.
  final DateTime createdAt;

  SyncOperation copyWith({
    String? id,
    SyncEntityRef? entity,
    SyncOperationType? type,
    Map<String, Object?>? payload,
    Map<String, Object?>? headers,
    DateTime? createdAt,
  }) {
    return SyncOperation(
      id: id ?? this.id,
      entity: entity ?? this.entity,
      type: type ?? this.type,
      payload: payload ?? this.payload,
      headers: headers ?? this.headers,
      createdAt: createdAt ?? this.createdAt,
    );
  }
}
