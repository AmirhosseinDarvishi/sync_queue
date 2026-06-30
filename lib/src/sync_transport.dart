import 'models/sync_operation.dart';
import 'models/sync_result.dart';

/// Sends queued operations to a remote API.
abstract interface class SyncTransport {
  Future<SyncResult> send(SyncOperation operation);
}
