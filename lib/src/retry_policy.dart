/// Provides a normalized value used to spread retry delays.
typedef RetryJitter = double Function();

/// Calculates retry delays for failed operations.
class RetryPolicy {
  /// Creates an exponential backoff policy for retryable failures.
  const RetryPolicy({
    this.maxAttempts = 3,
    this.baseDelay = const Duration(seconds: 2),
    this.maxDelay = const Duration(minutes: 2),
    this.multiplier = 2,
    this.jitterFactor = 0,
  }) : assert(maxAttempts > 0),
       assert(multiplier >= 1),
       assert(jitterFactor >= 0 && jitterFactor <= 1);

  /// Total attempts, including the first immediate try.
  final int maxAttempts;

  /// Delay after the first failed attempt.
  final Duration baseDelay;

  /// Upper bound for exponential backoff.
  final Duration maxDelay;

  /// Exponential multiplier applied per failure.
  final int multiplier;

  /// Percentage of the calculated delay that can be randomized downward.
  ///
  /// A value of `0.2` spreads retries between 80% and 100% of the calculated
  /// delay. The default `0` keeps retry scheduling deterministic.
  final double jitterFactor;

  /// Returns whether a failure with [attempts] should be retried.
  bool canRetry({required int attempts, required bool isRetryable}) {
    return isRetryable && attempts < maxAttempts;
  }

  /// Calculates the delay before retrying after [attempts] failed attempts.
  Duration delayForAttempt(int attempts, {double jitter = 1}) {
    final delay = _baseDelayForAttempt(attempts);
    if (jitterFactor == 0 || delay == Duration.zero) {
      return delay;
    }

    final boundedJitter = jitter.clamp(0, 1).toDouble();
    final delayMicros = delay.inMicroseconds;
    final spreadMicros = (delayMicros * jitterFactor).round();
    final minMicros = delayMicros - spreadMicros;
    final jitteredMicros = minMicros + (spreadMicros * boundedJitter).round();
    return Duration(microseconds: jitteredMicros);
  }

  Duration _baseDelayForAttempt(int attempts) {
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
