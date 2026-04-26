class ChatMessage {
  ChatMessage.user({required this.id, required this.text})
    : role = 'user',
      answer = null,
      chart = null,
      sqlUsed = null,
      rows = null,
      sources = null,
      qualityFlags = null,
      insight = null;

  ChatMessage.assistant({
    required this.id,
    required this.answer,
    this.chart,
    this.sqlUsed,
    this.rows,
    this.sources,
    this.qualityFlags,
    this.insight,
  }) : role = 'assistant',
       text = null;

  final String id;
  final String role;
  final String? text;
  final String? answer;
  final Map<String, dynamic>? chart;
  final String? sqlUsed;
  final dynamic rows;
  final List<String>? sources;
  final List<String>? qualityFlags;
  final String? insight;
}

class LoadedSourceEntry {
  const LoadedSourceEntry({
    required this.sourceType,
    required this.host,
    required this.port,
    required this.database,
    required this.sourceTable,
    required this.targetTable,
    required this.loadedAt,
    required this.inferred,
  });

  factory LoadedSourceEntry.fromJson(Map<String, dynamic> json) {
    return LoadedSourceEntry(
      sourceType: json['source_type']?.toString() ?? 'mysql',
      host: json['host']?.toString() ?? '',
      port: json['port']?.toString() ?? '3306',
      database: json['database']?.toString() ?? '',
      sourceTable: json['source_table']?.toString() ?? '',
      targetTable: json['target_table']?.toString() ?? '',
      loadedAt: json['loaded_at']?.toString(),
      inferred: json['inferred'] == true,
    );
  }

  final String sourceType;
  final String host;
  final String port;
  final String database;
  final String sourceTable;
  final String targetTable;
  final String? loadedAt;
  final bool inferred;
}

class ChartData {
  const ChartData({
    required this.type,
    required this.x,
    required this.y,
    required this.title,
    required this.seriesLabel,
    required this.xLabel,
    required this.yLabel,
    required this.valueFormat,
  });

  factory ChartData.fromJson(Map<String, dynamic> json) {
    return ChartData(
      type: json['type']?.toString() ?? 'line',
      x: (json['x'] as List<dynamic>? ?? <dynamic>[])
          .map((dynamic item) => '$item')
          .toList(),
      y: (json['y'] as List<dynamic>? ?? <dynamic>[])
          .map((dynamic item) => num.tryParse('$item') ?? 0)
          .toList(),
      title: json['title']?.toString() ?? '',
      seriesLabel: json['series_label']?.toString() ?? '',
      xLabel: json['x_label']?.toString() ?? '',
      yLabel: json['y_label']?.toString() ?? '',
      valueFormat: json['value_format']?.toString() ?? '',
    );
  }

  final String type;
  final List<String> x;
  final List<num> y;
  final String title;
  final String seriesLabel;
  final String xLabel;
  final String yLabel;
  final String valueFormat;
}

class Credentials {
  const Credentials({required this.user, required this.password});

  final String user;
  final String password;
}

class ApiException implements Exception {
  ApiException({
    required this.statusCode,
    required this.message,
    required this.detailMap,
  });

  final int statusCode;
  final String? message;
  final Map<String, dynamic> detailMap;

  @override
  String toString() => message ?? 'Request failed';
}
