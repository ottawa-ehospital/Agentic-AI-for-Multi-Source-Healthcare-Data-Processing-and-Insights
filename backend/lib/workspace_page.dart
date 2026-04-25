import 'package:elg6131/localization/index.dart' as l10n;
import 'package:elg6131/models/workspace_models.dart';
import 'package:elg6131/services/api_client.dart';
import 'package:elg6131/utils/workspace_helpers.dart';
import 'package:elg6131/widgets/workspace_sections.dart';
import 'package:flutter/material.dart';

class WorkspacePage extends StatefulWidget {
  const WorkspacePage({super.key, required this.baseUrl});

  final String baseUrl;

  @override
  State<WorkspacePage> createState() => _WorkspacePageState();
}

class _WorkspacePageState extends State<WorkspacePage> {
  String _sourceType = 'mysql';
  final _hostController = TextEditingController();
  final _portController = TextEditingController(text: '3306');
  final _userController = TextEditingController();
  final _passwordController = TextEditingController();
  final _apiBaseUrlController = TextEditingController();
  final _apiTokenController = TextEditingController();
  final _tableSearchController = TextEditingController();
  final _questionController = TextEditingController();
  final _tableListScrollController = ScrollController();
  final _chatScrollController = ScrollController();
  final _api = ApiClient();

  String _language = 'en';
  bool _showConnectorPanel = true;
  bool _showLoadedPanel = true;
  bool _showDebug = false;
  bool _isConnected = false;
  bool _loginLoading = false;
  bool _tablesLoading = false;
  bool _ingestLoading = false;
  bool _chatLoading = false;

  String _sourceDatabase = '';
  String? _loginError;
  String? _tablesError;
  String? _ingestError;
  String? _loadedSourcesError;
  String? _chatError;
  String? _ingestSummary;
  String? _refreshingKey;

  List<String> _databases = <String>[];
  List<String> _availableTables = <String>[];
  final List<String> _selectedTables = <String>[];
  final List<LoadedSourceEntry> _loadedSources = <LoadedSourceEntry>[];
  final List<ChatMessage> _chatHistory = <ChatMessage>[];
  final Map<String, Credentials> _sessionCredentials = <String, Credentials>{};

  @override
  void initState() {
    super.initState();
    _refreshLoadedSources();
  }

  @override
  void dispose() {
    _hostController.dispose();
    _portController.dispose();
    _userController.dispose();
    _passwordController.dispose();
    _apiBaseUrlController.dispose();
    _apiTokenController.dispose();
    _tableSearchController.dispose();
    _questionController.dispose();
    _tableListScrollController.dispose();
    _chatScrollController.dispose();
    super.dispose();
  }

  void _scrollChatToBottom() {
    WidgetsBinding.instance.addPostFrameCallback((_) {
      if (!_chatScrollController.hasClients) {
        return;
      }
      _chatScrollController.animateTo(
        _chatScrollController.position.maxScrollExtent,
        duration: const Duration(milliseconds: 220),
        curve: Curves.easeOut,
      );
    });
  }

  l10n.AppStrings get t => l10n.localized[_language] ?? l10n.localized['en']!;

  String get _host => _hostController.text.trim();

  String get _port => _portController.text.trim().isEmpty
      ? '3306'
      : _portController.text.trim();

  String get _user => _userController.text.trim();

  String get _password => _passwordController.text;
  String get _apiBaseUrl => _apiBaseUrlController.text.trim();
  String get _apiToken => _apiTokenController.text.trim();

  Uri? get _apiUri => Uri.tryParse(_apiBaseUrl);

  String _credentialKey(String host, String port, String database) {
    return '$host:${port.isEmpty ? '3306' : port}/$database';
  }

  void _rememberCredentials({
    required String host,
    required String port,
    required String database,
    required String user,
    required String password,
  }) {
    if (host.isEmpty || password.isEmpty) {
      return;
    }
    _sessionCredentials[_credentialKey(host, port, database)] = Credentials(
      user: user,
      password: password,
    );
  }

  Credentials? _getRememberedCredentials({
    required String host,
    required String port,
    required String database,
  }) {
    return _sessionCredentials[_credentialKey(host, port, database)] ??
        _sessionCredentials[_credentialKey(host, port, '')];
  }

  Future<void> _refreshLoadedSources() async {
    try {
      final data = await _api.getJson(
        '${widget.baseUrl}/api/pipeline/loaded_sources',
      );
      final items = (data['items'] as List<dynamic>? ?? <dynamic>[])
          .whereType<Map<String, dynamic>>()
          .map(LoadedSourceEntry.fromJson)
          .toList();
      if (!mounted) {
        return;
      }
      setState(() {
        _loadedSourcesError = null;
        _loadedSources
          ..clear()
          ..addAll(items);
      });
    } catch (error) {
      if (!mounted) {
        return;
      }
      setState(() {
        _loadedSourcesError = error.toString();
      });
    }
  }

  Set<String> get _currentLoadedTableNames {
    if (_sourceType == 'table_api') {
      final uri = _apiUri;
      if (uri == null || uri.host.isEmpty) {
        return <String>{};
      }
      final port =
          (uri.hasPort
                  ? uri.port
                  : uri.scheme == 'https'
                  ? 443
                  : 80)
              .toString();
      final databaseKey = _apiBaseUrl.replaceAll(RegExp(r'/$'), '');
      return _loadedSources
          .where(
            (LoadedSourceEntry entry) =>
                entry.sourceType == 'table_api' &&
                entry.host == uri.host &&
                entry.port == port &&
                entry.database == databaseKey,
          )
          .map((LoadedSourceEntry entry) => entry.sourceTable)
          .toSet();
    }
    return _loadedSources
        .where(
          (LoadedSourceEntry entry) =>
              entry.host == _host &&
              entry.port == _port &&
              entry.database == _sourceDatabase,
        )
        .map((LoadedSourceEntry entry) => entry.sourceTable)
        .toSet();
  }

  Future<void> _loginAndLoadDatabases() async {
    setState(() {
      _loginLoading = true;
      _loginError = null;
      _databases = <String>[];
      _sourceDatabase = '';
      _availableTables = <String>[];
      _selectedTables.clear();
      _tablesError = null;
      _ingestError = null;
      _ingestSummary = null;
    });

    try {
      late final Map<String, dynamic> data;
      if (_sourceType == 'table_api') {
        data = await _api.postJson(
          '${widget.baseUrl}/api/pipeline/list_tables',
          <String, dynamic>{
            'source': <String, dynamic>{
              'source_type': 'table_api',
              'base_url': _apiBaseUrl,
              'api_token': _apiToken.isEmpty ? null : _apiToken,
              'database': _apiBaseUrl,
            },
          },
        );
      } else {
        data = await _api.postJson(
          '${widget.baseUrl}/api/pipeline/list_databases',
          <String, dynamic>{
            'source': <String, dynamic>{
              'host': _host,
              'port': int.tryParse(_port) ?? 3306,
              'user': _user,
              'password': _password,
              'database': 'information_schema',
            },
          },
        );
      }

      if (!mounted) {
        return;
      }
      setState(() {
        _loginLoading = false;
        _isConnected = true;
        if (_sourceType == 'table_api') {
          _availableTables = (data['tables'] as List<dynamic>? ?? <dynamic>[])
              .map((dynamic value) => '$value')
              .toList();
        } else {
          _databases = (data['databases'] as List<dynamic>? ?? <dynamic>[])
              .map((dynamic value) => '$value')
              .toList();
        }
      });
      if (_sourceType == 'table_api') {
        final uri = _apiUri;
        if (uri != null && _apiToken.isNotEmpty) {
          _rememberCredentials(
            host: uri.host,
            port:
                (uri.hasPort
                        ? uri.port
                        : uri.scheme == 'https'
                        ? 443
                        : 80)
                    .toString(),
            database: _apiBaseUrl.replaceAll(RegExp(r'/$'), ''),
            user: '',
            password: _apiToken,
          );
        }
      } else {
        _rememberCredentials(
          host: _host,
          port: _port,
          database: '',
          user: _user,
          password: _password,
        );
      }
    } catch (error) {
      if (!mounted) {
        return;
      }
      setState(() {
        _loginLoading = false;
        _loginError = error.toString().isEmpty
            ? t.connectionFailed
            : error.toString();
      });
    }
  }

  void _logout() {
    setState(() {
      _isConnected = false;
      _databases = <String>[];
      _sourceDatabase = '';
      _availableTables = <String>[];
      _selectedTables.clear();
      _loginError = null;
      _tablesError = null;
      _ingestError = null;
      _ingestSummary = null;
    });
  }

  Future<void> _fetchTables() async {
    if (_sourceType == 'table_api') {
      setState(() {
        _tablesLoading = true;
        _tablesError = null;
        _availableTables = <String>[];
        _selectedTables.clear();
        _ingestSummary = null;
      });

      try {
        final data = await _api.postJson(
          '${widget.baseUrl}/api/pipeline/list_tables',
          <String, dynamic>{
            'source': <String, dynamic>{
              'source_type': 'table_api',
              'base_url': _apiBaseUrl,
              'api_token': _apiToken.isEmpty ? null : _apiToken,
              'database': _apiBaseUrl,
            },
          },
        );
        if (!mounted) {
          return;
        }
        setState(() {
          _tablesLoading = false;
          _availableTables = (data['tables'] as List<dynamic>? ?? <dynamic>[])
              .map((dynamic value) => '$value')
              .toList();
        });
        final uri = _apiUri;
        if (uri != null && _apiToken.isNotEmpty) {
          _rememberCredentials(
            host: uri.host,
            port:
                (uri.hasPort
                        ? uri.port
                        : uri.scheme == 'https'
                        ? 443
                        : 80)
                    .toString(),
            database: _apiBaseUrl.replaceAll(RegExp(r'/$'), ''),
            user: '',
            password: _apiToken,
          );
        }
      } catch (error) {
        if (!mounted) {
          return;
        }
        setState(() {
          _tablesLoading = false;
          _tablesError = error.toString().isEmpty
              ? t.couldNotLoadTables
              : error.toString();
        });
      }
      return;
    }
    if (_sourceDatabase.isEmpty) {
      setState(() {
        _tablesError = t.chooseDatabaseFirst;
      });
      return;
    }

    setState(() {
      _tablesLoading = true;
      _tablesError = null;
      _availableTables = <String>[];
      _selectedTables.clear();
      _ingestSummary = null;
    });

    try {
      final data = await _api.postJson(
        '${widget.baseUrl}/api/pipeline/list_tables',
        <String, dynamic>{
          'source': <String, dynamic>{
            'host': _host,
            'port': int.tryParse(_port) ?? 3306,
            'user': _user,
            'password': _password,
            'database': _sourceDatabase,
          },
        },
      );

      if (!mounted) {
        return;
      }
      setState(() {
        _tablesLoading = false;
        _availableTables = (data['tables'] as List<dynamic>? ?? <dynamic>[])
            .map((dynamic value) => '$value')
            .toList();
      });
      _rememberCredentials(
        host: _host,
        port: _port,
        database: _sourceDatabase,
        user: _user,
        password: _password,
      );
    } catch (error) {
      if (!mounted) {
        return;
      }
      setState(() {
        _tablesLoading = false;
        _tablesError = error.toString().isEmpty
            ? t.couldNotLoadTables
            : error.toString();
      });
    }
  }

  Future<void> _ingest() async {
    if (_sourceType == 'mysql' &&
        (_sourceDatabase.isEmpty || _selectedTables.isEmpty)) {
      setState(() {
        _ingestError = t.selectTableBeforeLoad;
      });
      return;
    }
    if (_sourceType == 'table_api' &&
        (_apiBaseUrl.isEmpty || _selectedTables.isEmpty)) {
      setState(() {
        _ingestError = _apiBaseUrl.isEmpty
            ? t.connectionFailed
            : t.selectTableBeforeLoad;
      });
      return;
    }

    setState(() {
      _ingestLoading = true;
      _ingestError = null;
      _ingestSummary = null;
    });

    try {
      final data = await _api.postJson(
        '${widget.baseUrl}/api/pipeline/ingest',
        <String, dynamic>{
          'source': <String, dynamic>{
            'source_type': _sourceType,
            'host': _sourceType == 'mysql' ? _host : null,
            'port': int.tryParse(_port) ?? 3306,
            'user': _sourceType == 'mysql' ? _user : null,
            'password': _sourceType == 'mysql' ? _password : null,
            'database': _sourceType == 'mysql' ? _sourceDatabase : null,
            'base_url': _sourceType == 'table_api' ? _apiBaseUrl : null,
            'api_token': _sourceType == 'table_api' ? _apiToken : null,
          },
          'tables': _selectedTables,
        },
      );

      final loaded = (data['tables'] as List<dynamic>? ?? <dynamic>[]);
      final count = loaded.isNotEmpty ? loaded.length : _selectedTables.length;

      if (!mounted) {
        return;
      }
      setState(() {
        _ingestLoading = false;
        _ingestSummary = loaded.isNotEmpty
            ? fillTemplate(t.loadedSummary, <String, String>{
                'count': '$count',
                'suffix': pluralSuffix(count),
                'suffix2': _language == 'fr' && count > 1 ? 's' : '',
              })
            : fillTemplate(t.processedSummary, <String, String>{
                'count': '$count',
                'suffix': pluralSuffix(count),
                'suffix2': _language == 'fr' && count > 1 ? 's' : '',
              });
      });
      if (_sourceType == 'mysql') {
        _rememberCredentials(
          host: _host,
          port: _port,
          database: _sourceDatabase,
          user: _user,
          password: _password,
        );
      }
      await _refreshLoadedSources();
    } catch (error) {
      if (!mounted) {
        return;
      }
      setState(() {
        _ingestLoading = false;
        _ingestError = error.toString().isEmpty
            ? t.ingestionFailed
            : error.toString();
      });
    }
  }

  Future<void> _runRefresh({
    required String host,
    required String port,
    required String database,
    String? table,
    Credentials? overrideCredentials,
  }) async {
    final refreshKey = table == null
        ? 'host:$host:$port'
        : 'table:$host:$port:$database:$table';

    setState(() {
      _refreshingKey = refreshKey;
      _loadedSourcesError = null;
    });

    try {
      final credentials =
          overrideCredentials ??
          _getRememberedCredentials(host: host, port: port, database: database);

      await _api.postJson(
        '${widget.baseUrl}/api/pipeline/refresh',
        <String, dynamic>{
          'source': <String, dynamic>{
            'host': host,
            'port': int.tryParse(port) ?? 3306,
            'user': credentials?.user ?? '',
            'password': credentials?.password ?? '',
            'database': database,
          },
          'table': table,
        },
      );

      if (credentials != null) {
        _rememberCredentials(
          host: host,
          port: port,
          database: database,
          user: credentials.user,
          password: credentials.password,
        );
      }
      await _refreshLoadedSources();
    } on ApiException catch (error) {
      final detail = error.detailMap;
      if (error.statusCode == 409 && detail['reconnect_required'] == true) {
        final credentials = await _showReconnectDialog(
          host: host,
          port: port,
          database: database,
          table: table,
        );
        if (credentials != null) {
          await _runRefresh(
            host: host,
            port: port,
            database: database,
            table: table,
            overrideCredentials: credentials,
          );
        }
      } else {
        if (!mounted) {
          return;
        }
        setState(() {
          _loadedSourcesError =
              detail['message']?.toString() ?? error.message ?? t.refreshFailed;
        });
      }
    } catch (error) {
      if (!mounted) {
        return;
      }
      setState(() {
        _loadedSourcesError = error.toString();
      });
    } finally {
      if (mounted) {
        setState(() {
          _refreshingKey = null;
        });
      }
    }
  }

  Future<void> _deleteLoadedTable(LoadedSourceEntry entry) async {
    try {
      await _api.postJson(
        '${widget.baseUrl}/api/pipeline/delete_loaded_table',
        <String, dynamic>{
          'source': <String, dynamic>{
            'host': entry.host,
            'port': int.tryParse(entry.port) ?? 3306,
            'user': '',
            'password': '',
            'database': entry.database,
          },
          'table': entry.sourceTable,
        },
      );
      await _refreshLoadedSources();
    } catch (error) {
      if (!mounted) {
        return;
      }
      setState(() {
        _loadedSourcesError = error.toString().isEmpty
            ? t.couldNotDeleteLoadedTable
            : error.toString();
      });
    }
  }

  Future<void> _deleteLoadedGroup(String hostPort, String database) async {
    final shouldDelete = await _confirmDelete(
      fillTemplate(t.confirmDeleteGroup, <String, String>{
        'label': database.isEmpty ? hostPort : '$hostPort / $database',
      }),
    );
    if (!shouldDelete) {
      return;
    }

    final parts = hostPort.split(':');
    try {
      await _api.postJson(
        '${widget.baseUrl}/api/pipeline/delete_loaded_group',
        <String, dynamic>{
          'source': <String, dynamic>{
            'host': parts.first,
            'port': int.tryParse(parts.length > 1 ? parts[1] : '3306') ?? 3306,
            'user': '',
            'password': '',
            'database': database,
          },
        },
      );
      await _refreshLoadedSources();
    } catch (error) {
      if (!mounted) {
        return;
      }
      setState(() {
        _loadedSourcesError = error.toString().isEmpty
            ? t.couldNotDeleteLoadedGroup
            : error.toString();
      });
    }
  }

  Future<void> _ask([String? prefillQuestion]) async {
    final finalQuestion = (prefillQuestion ?? _questionController.text).trim();
    if (finalQuestion.isEmpty) {
      return;
    }
    if (_loadedSources.isEmpty) {
      await _showLoadDataDialog();
      return;
    }

    final userMessage = ChatMessage.user(
      id: '${DateTime.now().microsecondsSinceEpoch}-user',
      text: finalQuestion,
    );

    setState(() {
      _chatLoading = true;
      _chatError = null;
      _questionController.clear();
      _chatHistory.add(userMessage);
    });
    _scrollChatToBottom();

    try {
      final historyPayload = _chatHistory
          .take(_chatHistory.length - 1)
          .toList()
          .reversed
          .take(6)
          .toList()
          .reversed
          .map(
            (ChatMessage message) => <String, dynamic>{
              'role': message.role,
              'content': message.role == 'user'
                  ? message.text ?? ''
                  : message.answer ?? '',
            },
          )
          .toList();

      final data = await _api.postJson(
        '${widget.baseUrl}/api/ask',
        <String, dynamic>{
          'question': finalQuestion,
          'history': historyPayload,
          'locale': _language,
        },
      );

      final assistantMessage = ChatMessage.assistant(
        id: '${DateTime.now().microsecondsSinceEpoch}-assistant',
        answer: data['answer']?.toString() ?? '',
        chart: data['chart'] as Map<String, dynamic>?,
        sqlUsed: data['sql_used']?.toString(),
        rows: data['rows'],
        sources: (data['sources'] as List<dynamic>? ?? <dynamic>[])
            .map((dynamic item) => '$item')
            .toList(),
        qualityFlags: (data['quality_flags'] as List<dynamic>? ?? <dynamic>[])
            .map((dynamic item) => '$item')
            .toList(),
        insight: data['insight']?.toString(),
      );

      if (!mounted) {
        return;
      }
      setState(() {
        _chatLoading = false;
        _chatHistory.add(assistantMessage);
      });
      _scrollChatToBottom();
    } catch (error) {
      if (!mounted) {
        return;
      }
      setState(() {
        _chatLoading = false;
        _chatError = error.toString().isEmpty ? t.askFailed : error.toString();
        _chatHistory.add(
          ChatMessage.assistant(
            id: '${DateTime.now().microsecondsSinceEpoch}-assistant-error',
            answer: t.askFailedAnswer,
            qualityFlags: <String>['request_failed'],
            insight: _chatError,
          ),
        );
      });
      _scrollChatToBottom();
    }
  }

  Future<void> _switchLanguage(String language) async {
    setState(() {
      _language = language;
      _chatHistory.clear();
      _chatError = null;
      _questionController.clear();
      _loginError = null;
      _tablesError = null;
      _ingestError = null;
      _ingestSummary = null;
    });
    await _refreshLoadedSources();
  }

  void _switchSourceType(String value) {
    setState(() {
      _sourceType = value;
      _isConnected = false;
      _loginError = null;
      _tablesError = null;
      _ingestError = null;
      _ingestSummary = null;
      _sourceDatabase = '';
      _databases = <String>[];
      _availableTables = <String>[];
      _selectedTables.clear();
      _tableSearchController.clear();
    });
  }

  Future<void> _showLoadDataDialog() {
    return showDialog<void>(
      context: context,
      builder: (BuildContext context) {
        return AlertDialog(
          title: Text(t.modalLoadDataFirst),
          content: Text(
            '${t.modalLoadDataFirstCopy}\n\n${t.modalLoadDataFirstHelper}',
          ),
          actions: <Widget>[
            TextButton(
              onPressed: () => Navigator.of(context).pop(),
              child: Text(t.close),
            ),
          ],
        );
      },
    );
  }

  Future<bool> _confirmDelete(String message) async {
    final result = await showDialog<bool>(
      context: context,
      builder: (BuildContext context) {
        return AlertDialog(
          content: Text(message),
          actions: <Widget>[
            TextButton(
              onPressed: () => Navigator.of(context).pop(false),
              child: Text(t.cancel),
            ),
            FilledButton(
              onPressed: () => Navigator.of(context).pop(true),
              child: Text(t.deleteAll),
            ),
          ],
        );
      },
    );
    return result ?? false;
  }

  Future<Credentials?> _showReconnectDialog({
    required String host,
    required String port,
    required String database,
    String? table,
  }) async {
    final remembered = _getRememberedCredentials(
      host: host,
      port: port,
      database: database,
    );
    final userController = TextEditingController(text: remembered?.user ?? '');
    final passwordController = TextEditingController();

    final credentials = await showDialog<Credentials>(
      context: context,
      barrierDismissible: false,
      builder: (BuildContext context) {
        return AlertDialog(
          title: Text(t.modalEnterCredentials),
          content: SizedBox(
            width: 420,
            child: Column(
              mainAxisSize: MainAxisSize.min,
              crossAxisAlignment: CrossAxisAlignment.start,
              children: <Widget>[
                Text(t.modalEnterCredentialsCopy),
                const SizedBox(height: 12),
                Text(
                  table == null
                      ? fillTemplate(t.refreshAllFrom, <String, String>{
                          'host': host,
                          'port': port,
                        })
                      : fillTemplate(t.refreshOneFrom, <String, String>{
                          'table': table,
                          'host': host,
                          'port': port,
                          'database': database,
                        }),
                  style: const TextStyle(
                    fontSize: 13,
                    color: Color(0xFF475569),
                  ),
                ),
                const SizedBox(height: 16),
                TextField(
                  controller: userController,
                  decoration: InputDecoration(labelText: t.userPlaceholder),
                ),
                const SizedBox(height: 12),
                TextField(
                  controller: passwordController,
                  obscureText: true,
                  decoration: InputDecoration(labelText: t.passwordPlaceholder),
                ),
              ],
            ),
          ),
          actions: <Widget>[
            TextButton(
              onPressed: () => Navigator.of(context).pop(),
              child: Text(t.cancel),
            ),
            FilledButton(
              onPressed: () {
                Navigator.of(context).pop(
                  Credentials(
                    user: userController.text.trim(),
                    password: passwordController.text,
                  ),
                );
              },
              child: Text(t.retryRefresh),
            ),
          ],
        );
      },
    );

    userController.dispose();
    passwordController.dispose();
    return credentials;
  }

  @override
  Widget build(BuildContext context) {
    final width = MediaQuery.of(context).size.width;
    final isWide = width >= 1180;
    final groupedSources = groupLoadedSources(_loadedSources);

    return Scaffold(
      body: SelectionArea(
        child: Container(
          decoration: const BoxDecoration(
            gradient: RadialGradient(
              center: Alignment.topLeft,
              radius: 1.4,
              colors: <Color>[
                Color(0xFFEFF6FF),
                Color(0xFFF8FAFC),
                Color(0xFFEEF2FF),
              ],
            ),
          ),
          child: SafeArea(
            child: Padding(
              padding: const EdgeInsets.all(24),
              child: Center(
                child: ConstrainedBox(
                  constraints: const BoxConstraints(maxWidth: 1500),
                  child: isWide
                      ? SizedBox(
                          height: MediaQuery.of(context).size.height - 48,
                          child: Row(
                            crossAxisAlignment: CrossAxisAlignment.stretch,
                            children: <Widget>[
                              SizedBox(
                                width: 360,
                                child: SingleChildScrollView(
                                  child: _buildSidebar(groupedSources),
                                ),
                              ),
                              const SizedBox(width: 20),
                              Expanded(child: _buildChatPanel()),
                            ],
                          ),
                        )
                      : SingleChildScrollView(
                          child: Column(
                            children: <Widget>[
                              _buildSidebar(groupedSources),
                              const SizedBox(height: 20),
                              _buildChatPanel(),
                            ],
                          ),
                        ),
                ),
              ),
            ),
          ),
        ),
      ),
    );
  }

  Widget _buildSidebar(
    Map<String, Map<String, List<LoadedSourceEntry>>> groupedSources,
  ) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.stretch,
      children: <Widget>[
        WorkspaceHeroCard(
          language: _language,
          strings: t,
          onLanguageChanged: _switchLanguage,
        ),
        const SizedBox(height: 16),
        ConnectorCard(
          strings: t,
          language: _language,
          sourceType: _sourceType,
          expanded: _showConnectorPanel,
          onToggleExpanded: () =>
              setState(() => _showConnectorPanel = !_showConnectorPanel),
          isConnected: _isConnected,
          loginLoading: _loginLoading,
          tablesLoading: _tablesLoading,
          ingestLoading: _ingestLoading,
          loginError: _loginError,
          tablesError: _tablesError,
          ingestError: _ingestError,
          ingestSummary: _ingestSummary,
          hostController: _hostController,
          portController: _portController,
          userController: _userController,
          passwordController: _passwordController,
          apiBaseUrlController: _apiBaseUrlController,
          apiTokenController: _apiTokenController,
          tableSearchController: _tableSearchController,
          tableListScrollController: _tableListScrollController,
          databases: _databases,
          sourceDatabase: _sourceDatabase,
          availableTables: _availableTables,
          selectedTables: _selectedTables,
          loadedTableNames: _currentLoadedTableNames,
          onConnect: _loginAndLoadDatabases,
          onLogout: _logout,
          onDatabaseChanged: (String? value) {
            setState(() {
              _sourceDatabase = value ?? '';
              _availableTables = <String>[];
              _selectedTables.clear();
              _tableSearchController.clear();
              _ingestSummary = null;
            });
          },
          onLoadTables: _fetchTables,
          onToggleTable: (String table) {
            setState(() {
              if (_selectedTables.contains(table)) {
                _selectedTables.remove(table);
              } else {
                _selectedTables.add(table);
              }
            });
          },
          onIngest: _ingest,
          onTableSearchChanged: (_) => setState(() {}),
          onSourceTypeChanged: _switchSourceType,
        ),
        const SizedBox(height: 16),
        LoadedSourcesCard(
          strings: t,
          expanded: _showLoadedPanel,
          onToggleExpanded: () =>
              setState(() => _showLoadedPanel = !_showLoadedPanel),
          loadedSources: _loadedSources,
          loadedSourcesError: _loadedSourcesError,
          groupedSources: groupedSources,
          refreshingKey: _refreshingKey,
          onRefreshHost: (String hostPort) {
            final parts = hostPort.split(':');
            _runRefresh(
              host: parts.first,
              port: parts.length > 1 ? parts[1] : '3306',
              database: '',
            );
          },
          onDeleteHost: (String hostPort) => _deleteLoadedGroup(hostPort, ''),
          onDeleteDatabase: _deleteLoadedGroup,
          onRefreshEntry: (LoadedSourceEntry entry) => _runRefresh(
            host: entry.host,
            port: entry.port,
            database: entry.database,
            table: entry.sourceTable,
          ),
          onDeleteEntry: _deleteLoadedTable,
        ),
      ],
    );
  }

  Widget _buildChatPanel() {
    return ChatPanel(
      strings: t,
      chatHistory: _chatHistory,
      chatError: _chatError,
      showDebug: _showDebug,
      chatLoading: _chatLoading,
      questionController: _questionController,
      chatScrollController: _chatScrollController,
      onShowDebugChanged: (bool value) {
        setState(() => _showDebug = value);
      },
      onAsk: _ask,
    );
  }
}
