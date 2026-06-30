/// A failure returned by the transport layer or captured from an exception.
class SyncFailure {
  const SyncFailure({
    required this.message,
    this.code,
    this.isRetryable = true,
    this.cause,
  });

  /// Human-readable summary suitable for logs and debug surfaces.
  final String message;

  /// Optional machine-readable failure code.
  final String? code;

  /// Whether the operation can be retried.
  final bool isRetryable;

  /// Original exception or response object, when available.
  final Object? cause;

  @override
  String toString() {
    if (code == null) {
      return message;
    }

    return '$code: $message';
  }
}
