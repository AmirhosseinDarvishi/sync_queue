import '../sync_json.dart';

/// Identifies one domain object across local storage, the queue, and the UI.
class SyncEntityRef {
  const SyncEntityRef({required this.type, required this.id});

  factory SyncEntityRef.fromJson(SyncJsonMap json) {
    return SyncEntityRef(
      type: readString(json, 'type'),
      id: readString(json, 'id'),
    );
  }

  /// Domain type, such as `task`, `invoice`, or `profile`.
  final String type;

  /// Stable local or server id for the entity.
  final String id;

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        other is SyncEntityRef && other.type == type && other.id == id;
  }

  @override
  int get hashCode => Object.hash(type, id);

  SyncJsonMap toJson() {
    return <String, Object?>{'type': type, 'id': id};
  }

  @override
  String toString() => '$type/$id';
}
