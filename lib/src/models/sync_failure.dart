import '../sync_json.dart';

/// A failure returned by the transport layer or captured from an exception.
class SyncFailure {
  const SyncFailure({
    required this.message,
    this.code,
    this.isRetryable = true,
    this.retryAfter,
    this.cause,
  });

  /// Human-readable summary suitable for logs and debug surfaces.
  final String message;

  /// Optional machine-readable failure code.
  final String? code;

  /// Whether the operation can be retried.
  final bool isRetryable;

  /// Optional transport-provided delay before the next retry attempt.
  final Duration? retryAfter;

  /// Original exception or response object, when available.
  final Object? cause;

  factory SyncFailure.fromJson(SyncJsonMap json) {
    return SyncFailure(
      message: readString(json, 'message'),
      code: readOptionalString(json, 'code'),
      isRetryable: readBool(json, 'isRetryable'),
      retryAfter: _readOptionalDuration(json, 'retryAfterMs'),
    );
  }

  SyncJsonMap toJson() {
    return <String, Object?>{
      'message': message,
      'code': code,
      'isRetryable': isRetryable,
      'retryAfterMs': retryAfter?.inMilliseconds,
    };
  }

  @override
  String toString() {
    if (code == null) {
      return message;
    }

    return '$code: $message';
  }
}

Duration? _readOptionalDuration(SyncJsonMap json, String key) {
  final value = json[key];
  if (value == null) {
    return null;
  }

  if (value is int) {
    return Duration(milliseconds: value);
  }

  throw FormatException('Expected "$key" to be an integer or null.');
}
