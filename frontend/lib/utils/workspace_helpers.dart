import 'package:elg6131/models/workspace_models.dart';
import 'package:flutter/material.dart';

Map<String, Map<String, List<LoadedSourceEntry>>> groupLoadedSources(
  List<LoadedSourceEntry> items,
) {
  final grouped = <String, Map<String, List<LoadedSourceEntry>>>{};
  for (final item in items) {
    final hostKey = (item.port.isEmpty || item.port == '0')
        ? item.host
        : '${item.host}:${item.port}';
    grouped.putIfAbsent(hostKey, () => <String, List<LoadedSourceEntry>>{});
    grouped[hostKey]!.putIfAbsent(item.database, () => <LoadedSourceEntry>[]);
    grouped[hostKey]![item.database]!.add(item);
  }
  return grouped;
}

(List<LoadedSourceEntry>, List<LoadedSourceEntry>) splitTrackedAndInferred(
  List<LoadedSourceEntry> entries,
) {
  final tracked = <LoadedSourceEntry>[];
  final inferred = <LoadedSourceEntry>[];
  for (final entry in entries) {
    if (entry.inferred) {
      inferred.add(entry);
    } else {
      tracked.add(entry);
    }
  }
  return (tracked, inferred);
}

List<LoadedSourceEntry> dedupeLatestEntries(List<LoadedSourceEntry> entries) {
  final latest = <String, LoadedSourceEntry>{};
  for (final entry in entries) {
    final key = entry.sourceTable.isNotEmpty
        ? entry.sourceTable
        : entry.targetTable;
    final current = latest[key];
    if (current == null) {
      latest[key] = entry;
      continue;
    }
    final currentTime =
        DateTime.tryParse(current.loadedAt ?? '')?.millisecondsSinceEpoch ?? 0;
    final nextTime =
        DateTime.tryParse(entry.loadedAt ?? '')?.millisecondsSinceEpoch ?? 0;
    if (nextTime >= currentTime) {
      latest[key] = entry;
    }
  }
  final values = latest.values.toList();
  values.sort((LoadedSourceEntry a, LoadedSourceEntry b) {
    final aTime =
        DateTime.tryParse(a.loadedAt ?? '')?.millisecondsSinceEpoch ?? 0;
    final bTime =
        DateTime.tryParse(b.loadedAt ?? '')?.millisecondsSinceEpoch ?? 0;
    return bTime.compareTo(aTime);
  });
  return values;
}

String? formatTimestamp(String? value) {
  if (value == null || value.isEmpty) {
    return null;
  }
  final parsed = DateTime.tryParse(value);
  if (parsed == null) {
    return value;
  }
  return parsed.toLocal().toString();
}

String pluralSuffix(int count) => count > 1 ? 's' : '';

String fillTemplate(String template, Map<String, String> values) {
  var output = template;
  values.forEach((String key, String value) {
    output = output.replaceAll('{$key}', value);
  });
  return output;
}

String formatChartTick(num value, String format) {
  switch (format) {
    case 'currency':
      return '\$${value.toStringAsFixed(0)}';
    case 'percent':
      return '${value.toStringAsFixed(1)}%';
    default:
      return value.toString();
  }
}

const sectionLabelStyle = TextStyle(
  fontSize: 13,
  fontWeight: FontWeight.w700,
  color: Color(0xFF334155),
);
