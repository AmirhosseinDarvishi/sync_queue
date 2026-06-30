/// JSON-like map used by sync_queue model serializers.
typedef SyncJsonMap = Map<String, Object?>;

String readString(SyncJsonMap json, String key) {
  final value = json[key];
  if (value is String) {
    return value;
  }

  throw FormatException('Expected "$key" to be a string.');
}

String? readOptionalString(SyncJsonMap json, String key) {
  final value = json[key];
  if (value == null) {
    return null;
  }

  if (value is String) {
    return value;
  }

  throw FormatException('Expected "$key" to be a string or null.');
}

bool readBool(SyncJsonMap json, String key) {
  final value = json[key];
  if (value is bool) {
    return value;
  }

  throw FormatException('Expected "$key" to be a boolean.');
}

int readInt(SyncJsonMap json, String key) {
  final value = json[key];
  if (value is int) {
    return value;
  }

  throw FormatException('Expected "$key" to be an integer.');
}

DateTime readDateTime(SyncJsonMap json, String key) {
  final value = json[key];
  if (value is String) {
    return DateTime.parse(value);
  }

  throw FormatException('Expected "$key" to be an ISO-8601 string.');
}

DateTime? readOptionalDateTime(SyncJsonMap json, String key) {
  final value = json[key];
  if (value == null) {
    return null;
  }

  if (value is String) {
    return DateTime.parse(value);
  }

  throw FormatException('Expected "$key" to be an ISO-8601 string or null.');
}

SyncJsonMap readJsonMap(SyncJsonMap json, String key) {
  final value = json[key];
  if (value is Map<Object?, Object?>) {
    return toSyncJsonMap(value);
  }

  throw FormatException('Expected "$key" to be an object.');
}

SyncJsonMap? readOptionalJsonMap(SyncJsonMap json, String key) {
  final value = json[key];
  if (value == null) {
    return null;
  }

  if (value is Map<Object?, Object?>) {
    return toSyncJsonMap(value);
  }

  throw FormatException('Expected "$key" to be an object or null.');
}

SyncJsonMap toSyncJsonMap(Map<Object?, Object?> value) {
  final result = <String, Object?>{};
  for (final entry in value.entries) {
    final key = entry.key;
    if (key is! String) {
      throw const FormatException('Expected object keys to be strings.');
    }

    result[key] = entry.value;
  }

  return result;
}

String writeDateTime(DateTime value) {
  return value.toUtc().toIso8601String();
}
