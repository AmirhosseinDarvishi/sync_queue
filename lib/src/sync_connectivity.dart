import 'dart:async';

/// Network availability as understood by the host app.
enum SyncConnectivityStatus { online, offline }

extension SyncConnectivityStatusX on SyncConnectivityStatus {
  bool get isOnline => this == SyncConnectivityStatus.online;
}

/// Connectivity boundary used by [SyncEngine] to avoid draining while offline.
abstract interface class SyncConnectivity {
  /// Current connectivity status.
  Future<SyncConnectivityStatus> get status;

  /// Emits status changes after startup.
  Stream<SyncConnectivityStatus> get changes;
}

/// Default connectivity source for apps that want the engine to drain normally.
class AlwaysOnlineSyncConnectivity implements SyncConnectivity {
  /// Creates a connectivity source that always reports online.
  const AlwaysOnlineSyncConnectivity();

  @override
  Future<SyncConnectivityStatus> get status async =>
      SyncConnectivityStatus.online;

  @override
  Stream<SyncConnectivityStatus> get changes =>
      const Stream<SyncConnectivityStatus>.empty();
}

/// Test and prototype connectivity source controlled by the caller.
class ManualSyncConnectivity implements SyncConnectivity {
  /// Creates a manually controlled connectivity source.
  ManualSyncConnectivity([
    SyncConnectivityStatus initialStatus = SyncConnectivityStatus.online,
  ]) : _status = initialStatus;

  SyncConnectivityStatus _status;
  final _changes = StreamController<SyncConnectivityStatus>.broadcast();

  @override
  Future<SyncConnectivityStatus> get status async => _status;

  @override
  Stream<SyncConnectivityStatus> get changes => _changes.stream;

  void setOnline() {
    setStatus(SyncConnectivityStatus.online);
  }

  void setOffline() {
    setStatus(SyncConnectivityStatus.offline);
  }

  void setStatus(SyncConnectivityStatus status) {
    if (_status == status) {
      return;
    }

    _status = status;
    _changes.add(status);
  }

  Future<void> dispose() async {
    await _changes.close();
  }
}
