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
  const SyncConflict({required this.message, this.local, this.remote});

  final String message;
  final Object? local;
  final Object? remote;
}
