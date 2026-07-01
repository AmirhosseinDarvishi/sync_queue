import 'sync_engine_state.dart';
import 'sync_queue_snapshot.dart';

/// Combined UI and diagnostics snapshot for the queue and engine lifecycle.
class SyncStateSnapshot {
  const SyncStateSnapshot({required this.queue, required this.engine});

  final SyncQueueSnapshot queue;
  final SyncEngineState engine;

  bool get isIdle => engine.isIdle && queue.isIdle;

  bool get isSyncing => engine.isDraining || queue.isSyncing;

  bool get hasPendingWork => queue.hasPendingWork;

  bool get needsAttention => queue.needsAttention;

  bool get lastDrainWasSkipped => engine.lastDrainWasSkipped;
}
