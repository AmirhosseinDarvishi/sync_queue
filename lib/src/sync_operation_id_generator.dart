import 'dart:math';

/// Generates operation ids for newly enqueued mutations.
typedef SyncOperationIdGenerator = String Function();

/// Default operation id utilities.
class SyncOperationIds {
  SyncOperationIds._();

  static final _random = Random.secure();
  static var _counter = 0;

  /// Generates a compact id with timestamp, process-local counter, and entropy.
  static String generate() {
    final timestamp = DateTime.now()
        .toUtc()
        .microsecondsSinceEpoch
        .toRadixString(36);
    final counter = _nextCounter().toRadixString(36);
    final entropy = _randomBase36(length: 8);
    return 'op_${timestamp}_${counter}_$entropy';
  }

  static int _nextCounter() {
    _counter = (_counter + 1) & 0x3fffffff;
    return _counter;
  }

  static String _randomBase36({required int length}) {
    final buffer = StringBuffer();
    for (var i = 0; i < length; i += 1) {
      buffer.write(_random.nextInt(36).toRadixString(36));
    }
    return buffer.toString();
  }
}
