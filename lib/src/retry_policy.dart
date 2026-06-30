/// Calculates retry delays for failed operations.
class RetryPolicy {
  const RetryPolicy({
    this.maxAttempts = 3,
    this.baseDelay = const Duration(seconds: 2),
    this.maxDelay = const Duration(minutes: 2),
    this.multiplier = 2,
  }) : assert(maxAttempts > 0),
       assert(multiplier >= 1);

  /// Total attempts, including the first immediate try.
  final int maxAttempts;

  /// Delay after the first failed attempt.
  final Duration baseDelay;

  /// Upper bound for exponential backoff.
  final Duration maxDelay;

  /// Exponential multiplier applied per failure.
  final int multiplier;

  bool canRetry({required int attempts, required bool isRetryable}) {
    return isRetryable && attempts < maxAttempts;
  }

  Duration delayForAttempt(int attempts) {
    if (attempts <= 1) {
      return baseDelay;
    }

    final factor = _pow(multiplier, attempts - 1);
    final delay = baseDelay * factor;
    return delay > maxDelay ? maxDelay : delay;
  }

  int _pow(int value, int exponent) {
    var result = 1;
    for (var i = 0; i < exponent; i += 1) {
      result *= value;
    }
    return result;
  }
}
