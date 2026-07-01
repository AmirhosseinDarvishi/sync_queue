import '../sync_json.dart';
import 'sync_failure.dart';

/// Result returned after a transport attempts to send one operation.
sealed class SyncResult {
  const SyncResult();

  const factory SyncResult.success({Object? data}) = SyncSuccess;

  const factory SyncResult.failure(SyncFailure failure) = SyncFailureResult;

  const factory SyncResult.conflict({
    required String message,
    Object? local,
    Object? remote,
  }) = SyncConflict;
}

/// A successful remote write.
class SyncSuccess extends SyncResult {
  const SyncSuccess({this.data});

  final Object? data;
}

/// A transport failure.
class SyncFailureResult extends SyncResult {
  const SyncFailureResult(this.failure);

  final SyncFailure failure;
}

/// A conflict that needs app-specific resolution.
class SyncConflict extends SyncResult {
  /// Creates a conflict result with optional local and remote payloads.
  const SyncConflict({required this.message, this.local, this.remote});

  /// Decodes a persisted conflict from JSON.
  factory SyncConflict.fromJson(SyncJsonMap json) {
    return SyncConflict(
      message: readString(json, 'message'),
      local: json['local'],
      remote: json['remote'],
    );
  }

  /// Human-readable reason the remote write could not be accepted.
  final String message;

  /// Local payload that caused or contributed to the conflict.
  final Object? local;

  /// Remote payload or state that should be used for merge decisions.
  final Object? remote;

  /// Encodes the conflict for durable queue storage.
  SyncJsonMap toJson() {
    return <String, Object?>{
      'message': message,
      'local': local,
      'remote': remote,
    };
  }
}
