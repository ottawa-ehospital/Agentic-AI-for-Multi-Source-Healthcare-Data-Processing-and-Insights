import 'dart:convert';

import 'package:elg6131/localization/app_strings.dart';
import 'package:elg6131/models/workspace_models.dart';
import 'package:elg6131/utils/workspace_helpers.dart';
import 'package:elg6131/widgets/chart_card.dart';
import 'package:elg6131/widgets/common_widgets.dart';
import 'package:flutter/gestures.dart';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';

class WorkspaceHeroCard extends StatelessWidget {
  const WorkspaceHeroCard({
    super.key,
    required this.language,
    required this.strings,
    required this.onLanguageChanged,
  });

  final String language;
  final AppStrings strings;
  final ValueChanged<String> onLanguageChanged;

  @override
  Widget build(BuildContext context) {
    return GlassCard(
      padding: EdgeInsets.zero,
      child: Container(
        padding: const EdgeInsets.all(22),
        decoration: const BoxDecoration(
          borderRadius: BorderRadius.all(Radius.circular(24)),
          gradient: LinearGradient(
            begin: Alignment.topLeft,
            end: Alignment.bottomRight,
            colors: <Color>[
              Color(0xFF0F172A),
              Color(0xFF1E293B),
              Color(0xFF14532D),
            ],
          ),
        ),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: <Widget>[
            Row(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: <Widget>[
                Expanded(
                  child: Text(
                    strings.appEyebrow.toUpperCase(),
                    style: const TextStyle(
                      color: Color(0xB3FFFFFF),
                      fontSize: 12,
                      letterSpacing: 1.4,
                      fontWeight: FontWeight.w700,
                    ),
                  ),
                ),
                PopupMenuButton<String>(
                  tooltip: strings.languageEnglish,
                  color: const Color(0xFF1E293B),
                  shape: RoundedRectangleBorder(
                    borderRadius: BorderRadius.circular(14),
                  ),
                  onSelected: (String value) {
                    if (value != language) {
                      onLanguageChanged(value);
                    }
                  },
                  itemBuilder: (BuildContext context) =>
                      <PopupMenuEntry<String>>[
                        PopupMenuItem<String>(
                          value: 'en',
                          child: Text(
                            strings.languageEnglish,
                            style: TextStyle(
                              color: language == 'en'
                                  ? const Color(0xFF93C5FD)
                                  : Colors.white,
                              fontWeight: language == 'en'
                                  ? FontWeight.w700
                                  : FontWeight.w500,
                            ),
                          ),
                        ),
                        PopupMenuItem<String>(
                          value: 'fr',
                          child: Text(
                            strings.languageFrench,
                            style: TextStyle(
                              color: language == 'fr'
                                  ? const Color(0xFF93C5FD)
                                  : Colors.white,
                              fontWeight: language == 'fr'
                                  ? FontWeight.w700
                                  : FontWeight.w500,
                            ),
                          ),
                        ),
                      ],
                  child: Container(
                    padding: const EdgeInsets.symmetric(
                      horizontal: 12,
                      vertical: 8,
                    ),
                    decoration: BoxDecoration(
                      color: const Color(0x1FFFFFFF),
                      borderRadius: BorderRadius.circular(12),
                    ),
                    child: Row(
                      mainAxisSize: MainAxisSize.min,
                      children: <Widget>[
                        Text(
                          language == 'en'
                              ? strings.languageEnglish
                              : strings.languageFrench,
                          style: const TextStyle(
                            color: Colors.white,
                            fontSize: 13,
                            fontWeight: FontWeight.w700,
                          ),
                        ),
                        const SizedBox(width: 6),
                        const Icon(
                          Icons.expand_more,
                          size: 18,
                          color: Colors.white,
                        ),
                      ],
                    ),
                  ),
                ),
              ],
            ),
            const SizedBox(height: 12),
            Text(
              strings.appTitle,
              style: const TextStyle(
                color: Colors.white,
                fontSize: 28,
                height: 1.1,
                fontWeight: FontWeight.w800,
              ),
            ),
            const SizedBox(height: 10),
            Text(
              strings.appCopy,
              style: const TextStyle(
                color: Color(0xCCFFFFFF),
                fontSize: 14,
                height: 1.5,
              ),
            ),
          ],
        ),
      ),
    );
  }
}

class ConnectorCard extends StatelessWidget {
  const ConnectorCard({
    super.key,
    required this.strings,
    required this.language,
    required this.sourceType,
    required this.expanded,
    required this.onToggleExpanded,
    required this.isConnected,
    required this.loginLoading,
    required this.tablesLoading,
    required this.ingestLoading,
    required this.loginError,
    required this.tablesError,
    required this.ingestError,
    required this.ingestSummary,
    required this.hostController,
    required this.portController,
    required this.userController,
    required this.passwordController,
    required this.apiBaseUrlController,
    required this.apiTokenController,
    required this.tableSearchController,
    required this.tableListScrollController,
    required this.databases,
    required this.sourceDatabase,
    required this.availableTables,
    required this.selectedTables,
    required this.loadedTableNames,
    required this.onConnect,
    required this.onLogout,
    required this.onDatabaseChanged,
    required this.onLoadTables,
    required this.onToggleTable,
    required this.onIngest,
    required this.onTableSearchChanged,
    required this.onSourceTypeChanged,
  });

  final AppStrings strings;
  final String language;
  final String sourceType;
  final bool expanded;
  final VoidCallback onToggleExpanded;
  final bool isConnected;
  final bool loginLoading;
  final bool tablesLoading;
  final bool ingestLoading;
  final String? loginError;
  final String? tablesError;
  final String? ingestError;
  final String? ingestSummary;
  final TextEditingController hostController;
  final TextEditingController portController;
  final TextEditingController userController;
  final TextEditingController passwordController;
  final TextEditingController apiBaseUrlController;
  final TextEditingController apiTokenController;
  final TextEditingController tableSearchController;
  final ScrollController tableListScrollController;
  final List<String> databases;
  final String sourceDatabase;
  final List<String> availableTables;
  final List<String> selectedTables;
  final Set<String> loadedTableNames;
  final VoidCallback onConnect;
  final VoidCallback onLogout;
  final ValueChanged<String?> onDatabaseChanged;
  final VoidCallback onLoadTables;
  final ValueChanged<String> onToggleTable;
  final VoidCallback onIngest;
  final ValueChanged<String> onTableSearchChanged;
  final ValueChanged<String> onSourceTypeChanged;

  String get _host => hostController.text.trim();
  String get _port =>
      portController.text.trim().isEmpty ? '3306' : portController.text.trim();
  String get _user => userController.text.trim();

  @override
  Widget build(BuildContext context) {
    return GlassCard(
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: <Widget>[
          CardHeader(
            title: strings.connectorTitle,
            subtitle: strings.connectorSubtitle,
            expanded: expanded,
            onTap: onToggleExpanded,
          ),
          if (expanded) ...<Widget>[
            const SizedBox(height: 12),
            Text(strings.sourceTypeLabel, style: sectionLabelStyle),
            const SizedBox(height: 8),
            SegmentedButton<String>(
              segments: <ButtonSegment<String>>[
                ButtonSegment<String>(
                  value: 'mysql',
                  label: Text(strings.sourceTypeDatabase),
                ),
                ButtonSegment<String>(
                  value: 'table_api',
                  label: Text(strings.sourceTypeTableApi),
                ),
              ],
              selected: <String>{sourceType},
              onSelectionChanged: (Set<String> selection) {
                if (selection.isNotEmpty) {
                  onSourceTypeChanged(selection.first);
                }
              },
            ),
            const SizedBox(height: 16),
            if (sourceType == 'table_api') ...<Widget>[
              if (!isConnected) ...<Widget>[
                Wrap(
                  spacing: 10,
                  runSpacing: 10,
                  children: <Widget>[
                    InputFieldBox(
                      controller: apiBaseUrlController,
                      hint: strings.apiBaseUrlPlaceholder,
                      width: 306,
                    ),
                    InputFieldBox(
                      controller: apiTokenController,
                      hint: strings.apiTokenPlaceholder,
                      width: 306,
                      obscure: true,
                    ),
                  ],
                ),
                const SizedBox(height: 10),
                Text(
                  strings.tableApiHelper,
                  style: const TextStyle(
                    fontSize: 13,
                    color: Color(0xFF64748B),
                  ),
                ),
                const SizedBox(height: 14),
                FilledButton(
                  onPressed: loginLoading ? null : onConnect,
                  child: Text(
                    loginLoading ? strings.connecting : strings.connectSource,
                  ),
                ),
                if (loginError != null) ...<Widget>[
                  const SizedBox(height: 10),
                  StatusBox(
                    text: loginError!,
                    background: const Color(0xFFFEF2F2),
                    foreground: const Color(0xFFB91C1C),
                  ),
                ],
              ] else ...<Widget>[
                Row(
                  children: <Widget>[
                    Container(
                      padding: const EdgeInsets.symmetric(
                        horizontal: 12,
                        vertical: 8,
                      ),
                      decoration: BoxDecoration(
                        color: const Color(0xFFECFDF5),
                        borderRadius: BorderRadius.circular(999),
                      ),
                      child: Row(
                        mainAxisSize: MainAxisSize.min,
                        children: const <Widget>[
                          Icon(Icons.circle, size: 9, color: Color(0xFF22C55E)),
                          SizedBox(width: 8),
                        ],
                      ),
                    ),
                    Text(
                      strings.connected,
                      style: const TextStyle(
                        color: Color(0xFF166534),
                        fontWeight: FontWeight.w700,
                      ),
                    ),
                    const Spacer(),
                    OutlinedButton(
                      onPressed: onLogout,
                      child: Text(strings.disconnect),
                    ),
                  ],
                ),
                const SizedBox(height: 10),
                Text(
                  apiBaseUrlController.text.trim(),
                  style: const TextStyle(
                    fontSize: 13,
                    color: Color(0xFF64748B),
                  ),
                ),
                const SizedBox(height: 18),
                Text(strings.loadTablesLabel, style: sectionLabelStyle),
                const SizedBox(height: 8),
                OutlinedButton(
                  onPressed: tablesLoading ? null : onLoadTables,
                  child: Text(
                    tablesLoading ? strings.loadingTables : strings.loadTables,
                  ),
                ),
                if (tablesError != null) ...<Widget>[
                  const SizedBox(height: 10),
                  StatusBox(
                    text: tablesError!,
                    background: const Color(0xFFFEF2F2),
                    foreground: const Color(0xFFB91C1C),
                  ),
                ],
                if (availableTables.isNotEmpty) ...<Widget>[
                  const SizedBox(height: 18),
                  Text(strings.selectTablesLabel, style: sectionLabelStyle),
                  const SizedBox(height: 8),
                  TextField(
                    controller: tableSearchController,
                    onChanged: onTableSearchChanged,
                    decoration: workspaceInputDecoration(strings.filterTables),
                  ),
                  const SizedBox(height: 12),
                  Container(
                    constraints: const BoxConstraints(maxHeight: 260),
                    padding: const EdgeInsets.only(right: 4),
                    child: Listener(
                      onPointerSignal: (PointerSignalEvent event) {
                        if (event is! PointerScrollEvent) {
                          return;
                        }
                        GestureBinding.instance.pointerSignalResolver.register(
                          event,
                          (PointerSignalEvent resolvedEvent) {
                            if (!tableListScrollController.hasClients) {
                              return;
                            }
                            final position = tableListScrollController.position;
                            final nextOffset =
                                (position.pixels + event.scrollDelta.dy).clamp(
                                  position.minScrollExtent,
                                  position.maxScrollExtent,
                                );
                            tableListScrollController.jumpTo(nextOffset);
                          },
                        );
                      },
                      child: Scrollbar(
                        controller: tableListScrollController,
                        thumbVisibility: true,
                        child: SingleChildScrollView(
                          controller: tableListScrollController,
                          child: Wrap(
                            spacing: 8,
                            runSpacing: 8,
                            children: availableTables
                                .where(
                                  (String table) =>
                                      table.toLowerCase().contains(
                                        tableSearchController.text
                                            .toLowerCase(),
                                      ),
                                )
                                .map((String table) {
                                  final isSelected = selectedTables.contains(
                                    table,
                                  );
                                  final isLoaded = loadedTableNames.contains(
                                    table,
                                  );
                                  return FilterChip(
                                    label: Text(
                                      isLoaded
                                          ? '$table • ${strings.loadedDot}'
                                          : table,
                                    ),
                                    selected: isSelected,
                                    onSelected: isLoaded
                                        ? null
                                        : (_) => onToggleTable(table),
                                  );
                                })
                                .toList(),
                          ),
                        ),
                      ),
                    ),
                  ),
                  const SizedBox(height: 10),
                  Text(
                    selectedTables.isEmpty
                        ? strings.chooseTableFirst
                        : fillTemplate(
                            strings.selectedTablesSummary,
                            <String, String>{
                              'count': '${selectedTables.length}',
                              'suffix': pluralSuffix(selectedTables.length),
                              'suffix2':
                                  language == 'fr' && selectedTables.length > 1
                                  ? 's'
                                  : '',
                            },
                          ),
                    style: const TextStyle(
                      fontSize: 13,
                      color: Color(0xFF64748B),
                    ),
                  ),
                  if (selectedTables.isNotEmpty) ...<Widget>[
                    const SizedBox(height: 14),
                    FilledButton(
                      onPressed: ingestLoading ? null : onIngest,
                      child: Text(
                        ingestLoading
                            ? strings.loadingWarehouse
                            : strings.ingestAndRefresh,
                      ),
                    ),
                  ],
                ],
                if (ingestError != null) ...<Widget>[
                  const SizedBox(height: 10),
                  StatusBox(
                    text: ingestError!,
                    background: const Color(0xFFFEF2F2),
                    foreground: const Color(0xFFB91C1C),
                  ),
                ],
                if (ingestSummary != null) ...<Widget>[
                  const SizedBox(height: 10),
                  StatusBox(
                    text: ingestSummary!,
                    background: const Color(0xFFEFF6FF),
                    foreground: const Color(0xFF1D4ED8),
                  ),
                ],
              ],
            ] else if (!isConnected) ...<Widget>[
              Wrap(
                spacing: 10,
                runSpacing: 10,
                children: <Widget>[
                  InputFieldBox(
                    controller: hostController,
                    hint: strings.hostPlaceholder,
                    width: 148,
                  ),
                  InputFieldBox(
                    controller: portController,
                    hint: strings.portPlaceholder,
                    width: 148,
                  ),
                  InputFieldBox(
                    controller: userController,
                    hint: strings.userPlaceholder,
                    width: 148,
                  ),
                  InputFieldBox(
                    controller: passwordController,
                    hint: strings.passwordPlaceholder,
                    width: 148,
                    obscure: true,
                  ),
                ],
              ),
              const SizedBox(height: 10),
              Text(
                strings.connectorHelper,
                style: const TextStyle(fontSize: 13, color: Color(0xFF64748B)),
              ),
              const SizedBox(height: 14),
              FilledButton(
                onPressed: loginLoading ? null : onConnect,
                child: Text(
                  loginLoading ? strings.connecting : strings.connectSource,
                ),
              ),
              if (loginError != null) ...<Widget>[
                const SizedBox(height: 10),
                StatusBox(
                  text: loginError!,
                  background: const Color(0xFFFEF2F2),
                  foreground: const Color(0xFFB91C1C),
                ),
              ],
            ] else ...<Widget>[
              Row(
                children: <Widget>[
                  Container(
                    padding: const EdgeInsets.symmetric(
                      horizontal: 12,
                      vertical: 8,
                    ),
                    decoration: BoxDecoration(
                      color: const Color(0xFFECFDF5),
                      borderRadius: BorderRadius.circular(999),
                    ),
                    child: Row(
                      mainAxisSize: MainAxisSize.min,
                      children: const <Widget>[
                        Icon(Icons.circle, size: 9, color: Color(0xFF22C55E)),
                        SizedBox(width: 8),
                      ],
                    ),
                  ),
                  Text(
                    strings.connected,
                    style: const TextStyle(
                      color: Color(0xFF166534),
                      fontWeight: FontWeight.w700,
                    ),
                  ),
                  const Spacer(),
                  OutlinedButton(
                    onPressed: onLogout,
                    child: Text(strings.disconnect),
                  ),
                ],
              ),
              const SizedBox(height: 10),
              Text(
                '$_user@$_host:$_port',
                style: const TextStyle(fontSize: 13, color: Color(0xFF64748B)),
              ),
              const SizedBox(height: 18),
              Text(strings.databaseLabel, style: sectionLabelStyle),
              const SizedBox(height: 8),
              DropdownButtonFormField<String>(
                initialValue: sourceDatabase.isEmpty ? null : sourceDatabase,
                decoration: workspaceInputDecoration(strings.selectDatabase),
                items: databases
                    .map(
                      (String database) => DropdownMenuItem<String>(
                        value: database,
                        child: Text(database),
                      ),
                    )
                    .toList(),
                onChanged: onDatabaseChanged,
              ),
              const SizedBox(height: 18),
              Text(strings.loadTablesLabel, style: sectionLabelStyle),
              const SizedBox(height: 8),
              OutlinedButton(
                onPressed: tablesLoading || sourceDatabase.isEmpty
                    ? null
                    : onLoadTables,
                child: Text(
                  tablesLoading ? strings.loadingTables : strings.loadTables,
                ),
              ),
              if (tablesError != null) ...<Widget>[
                const SizedBox(height: 10),
                StatusBox(
                  text: tablesError!,
                  background: const Color(0xFFFEF2F2),
                  foreground: const Color(0xFFB91C1C),
                ),
              ],
              if (availableTables.isNotEmpty) ...<Widget>[
                const SizedBox(height: 18),
                Text(strings.selectTablesLabel, style: sectionLabelStyle),
                const SizedBox(height: 8),
                TextField(
                  controller: tableSearchController,
                  onChanged: onTableSearchChanged,
                  decoration: workspaceInputDecoration(strings.filterTables),
                ),
                const SizedBox(height: 12),
                Container(
                  constraints: const BoxConstraints(maxHeight: 260),
                  padding: const EdgeInsets.only(right: 4),
                  child: Listener(
                    onPointerSignal: (PointerSignalEvent event) {
                      if (event is! PointerScrollEvent) {
                        return;
                      }
                      GestureBinding.instance.pointerSignalResolver.register(
                        event,
                        (PointerSignalEvent resolvedEvent) {
                          if (!tableListScrollController.hasClients) {
                            return;
                          }
                          final position = tableListScrollController.position;
                          final nextOffset =
                              (position.pixels + event.scrollDelta.dy).clamp(
                                position.minScrollExtent,
                                position.maxScrollExtent,
                              );
                          tableListScrollController.jumpTo(nextOffset);
                        },
                      );
                    },
                    child: Scrollbar(
                      controller: tableListScrollController,
                      thumbVisibility: true,
                      child: SingleChildScrollView(
                        controller: tableListScrollController,
                        child: Wrap(
                          spacing: 8,
                          runSpacing: 8,
                          children: availableTables
                              .where(
                                (String table) => table.toLowerCase().contains(
                                  tableSearchController.text.toLowerCase(),
                                ),
                              )
                              .map((String table) {
                                final isSelected = selectedTables.contains(
                                  table,
                                );
                                final isLoaded = loadedTableNames.contains(
                                  table,
                                );
                                return FilterChip(
                                  label: Text(
                                    isLoaded
                                        ? '$table • ${strings.loadedDot}'
                                        : table,
                                  ),
                                  selected: isSelected,
                                  onSelected: isLoaded
                                      ? null
                                      : (_) => onToggleTable(table),
                                );
                              })
                              .toList(),
                        ),
                      ),
                    ),
                  ),
                ),
                const SizedBox(height: 10),
                Text(
                  selectedTables.isEmpty
                      ? strings.chooseTableFirst
                      : fillTemplate(
                          strings.selectedTablesSummary,
                          <String, String>{
                            'count': '${selectedTables.length}',
                            'suffix': pluralSuffix(selectedTables.length),
                            'suffix2':
                                language == 'fr' && selectedTables.length > 1
                                ? 's'
                                : '',
                          },
                        ),
                  style: const TextStyle(
                    fontSize: 13,
                    color: Color(0xFF64748B),
                  ),
                ),
                if (selectedTables.isNotEmpty) ...<Widget>[
                  const SizedBox(height: 14),
                  FilledButton(
                    onPressed: ingestLoading ? null : onIngest,
                    child: Text(
                      ingestLoading
                          ? strings.loadingWarehouse
                          : strings.ingestAndRefresh,
                    ),
                  ),
                ],
              ],
              if (ingestError != null) ...<Widget>[
                const SizedBox(height: 10),
                StatusBox(
                  text: ingestError!,
                  background: const Color(0xFFFEF2F2),
                  foreground: const Color(0xFFB91C1C),
                ),
              ],
              if (ingestSummary != null) ...<Widget>[
                const SizedBox(height: 10),
                StatusBox(
                  text: ingestSummary!,
                  background: const Color(0xFFEFF6FF),
                  foreground: const Color(0xFF1D4ED8),
                ),
              ],
            ],
          ],
        ],
      ),
    );
  }
}

class LoadedSourcesCard extends StatelessWidget {
  const LoadedSourcesCard({
    super.key,
    required this.strings,
    required this.expanded,
    required this.onToggleExpanded,
    required this.loadedSources,
    required this.loadedSourcesError,
    required this.groupedSources,
    required this.refreshingKey,
    required this.onRefreshHost,
    required this.onDeleteHost,
    required this.onDeleteDatabase,
    required this.onRefreshEntry,
    required this.onDeleteEntry,
  });

  final AppStrings strings;
  final bool expanded;
  final VoidCallback onToggleExpanded;
  final List<LoadedSourceEntry> loadedSources;
  final String? loadedSourcesError;
  final Map<String, Map<String, List<LoadedSourceEntry>>> groupedSources;
  final String? refreshingKey;
  final ValueChanged<String> onRefreshHost;
  final ValueChanged<String> onDeleteHost;
  final void Function(String hostPort, String database) onDeleteDatabase;
  final ValueChanged<LoadedSourceEntry> onRefreshEntry;
  final ValueChanged<LoadedSourceEntry> onDeleteEntry;

  @override
  Widget build(BuildContext context) {
    return GlassCard(
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: <Widget>[
          CardHeader(
            title: strings.loadedSourcesTitle,
            subtitle: strings.loadedSourcesSubtitle,
            expanded: expanded,
            onTap: onToggleExpanded,
          ),
          if (expanded) ...<Widget>[
            const SizedBox(height: 12),
            if (loadedSources.isEmpty)
              Text(
                loadedSourcesError ?? strings.noLoadInventory,
                style: const TextStyle(fontSize: 13, color: Color(0xFF64748B)),
              )
            else
              ...groupedSources.entries.map((
                MapEntry<String, Map<String, List<LoadedSourceEntry>>>
                hostEntry,
              ) {
                final hostPort = hostEntry.key;
                return Container(
                  margin: const EdgeInsets.only(bottom: 12),
                  padding: const EdgeInsets.all(14),
                  decoration: BoxDecoration(
                    color: const Color(0xFFF8FAFC),
                    borderRadius: BorderRadius.circular(16),
                    border: Border.all(color: const Color(0xFFE2E8F0)),
                  ),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: <Widget>[
                      Row(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: <Widget>[
                          Expanded(
                            child: Text(
                              '${strings.sourceHost}: $hostPort',
                              style: const TextStyle(
                                fontSize: 14,
                                fontWeight: FontWeight.w700,
                              ),
                            ),
                          ),
                          Column(
                            crossAxisAlignment: CrossAxisAlignment.end,
                            children: <Widget>[
                              OutlinedButton(
                                style: OutlinedButton.styleFrom(
                                  padding: const EdgeInsets.symmetric(
                                    horizontal: 10,
                                    vertical: 8,
                                  ),
                                  minimumSize: Size.zero,
                                  tapTargetSize:
                                      MaterialTapTargetSize.shrinkWrap,
                                  visualDensity: VisualDensity.compact,
                                ),
                                onPressed: refreshingKey == 'host:$hostPort'
                                    ? null
                                    : () => onRefreshHost(hostPort),
                                child: Text(
                                  refreshingKey == 'host:$hostPort'
                                      ? strings.refreshing
                                      : strings.refreshAll,
                                ),
                              ),
                              const SizedBox(height: 8),
                              TextButton(
                                style: TextButton.styleFrom(
                                  padding: const EdgeInsets.symmetric(
                                    horizontal: 10,
                                    vertical: 6,
                                  ),
                                  minimumSize: Size.zero,
                                  tapTargetSize:
                                      MaterialTapTargetSize.shrinkWrap,
                                  visualDensity: VisualDensity.compact,
                                ),
                                onPressed: () => onDeleteHost(hostPort),
                                child: Text(strings.deleteAll),
                              ),
                            ],
                          ),
                        ],
                      ),
                      const SizedBox(height: 8),
                      ...hostEntry.value.entries.map((
                        MapEntry<String, List<LoadedSourceEntry>> dbEntry,
                      ) {
                        final split = splitTrackedAndInferred(dbEntry.value);
                        final tracked = dedupeLatestEntries(split.$1);
                        final inferred = dedupeLatestEntries(split.$2);
                        final visibleEntries = <LoadedSourceEntry>[
                          ...tracked,
                          ...inferred,
                        ];
                        return Padding(
                          padding: const EdgeInsets.only(top: 10),
                          child: Column(
                            crossAxisAlignment: CrossAxisAlignment.start,
                            children: <Widget>[
                              Row(
                                children: <Widget>[
                                  Expanded(
                                    child: Text(
                                      '${strings.databasePrefix}: ${dbEntry.key}',
                                      style: const TextStyle(
                                        fontSize: 13,
                                        fontWeight: FontWeight.w700,
                                      ),
                                    ),
                                  ),
                                  TextButton(
                                    onPressed: () =>
                                        onDeleteDatabase(hostPort, dbEntry.key),
                                    child: Text(strings.deleteAll),
                                  ),
                                ],
                              ),
                              const SizedBox(height: 6),
                              ...visibleEntries.map((LoadedSourceEntry entry) {
                                final refreshKey =
                                    'table:${entry.host}:${entry.port}:${entry.database}:${entry.sourceTable}';
                                return Container(
                                  margin: const EdgeInsets.only(bottom: 8),
                                  padding: const EdgeInsets.all(10),
                                  decoration: BoxDecoration(
                                    color: Colors.white,
                                    borderRadius: BorderRadius.circular(12),
                                    border: Border.all(
                                      color: const Color(0xFFDBE4EE),
                                    ),
                                  ),
                                  child: Row(
                                    crossAxisAlignment:
                                        CrossAxisAlignment.start,
                                    children: <Widget>[
                                      Expanded(
                                        child: Column(
                                          crossAxisAlignment:
                                              CrossAxisAlignment.start,
                                          children: <Widget>[
                                            Text(
                                              entry.sourceTable,
                                              style: const TextStyle(
                                                fontSize: 12,
                                                fontWeight: FontWeight.w700,
                                              ),
                                            ),
                                            const SizedBox(height: 4),
                                            Text(
                                              entry.targetTable.isEmpty
                                                  ? strings
                                                        .originalSourceUnknown
                                                  : '${strings.updatedAtPrefix}: ${formatTimestamp(entry.loadedAt) ?? strings.updatedTimeUnavailable}',
                                              style: const TextStyle(
                                                fontSize: 11,
                                                color: Color(0xFF64748B),
                                                height: 1.4,
                                              ),
                                            ),
                                          ],
                                        ),
                                      ),
                                      const SizedBox(width: 8),
                                      Column(
                                        children: <Widget>[
                                          IconButton(
                                            onPressed:
                                                refreshingKey == refreshKey
                                                ? null
                                                : () => onRefreshEntry(entry),
                                            icon: const Icon(
                                              Icons.refresh,
                                              size: 18,
                                            ),
                                          ),
                                          IconButton(
                                            onPressed: () =>
                                                onDeleteEntry(entry),
                                            icon: const Icon(
                                              Icons.delete_outline,
                                              size: 18,
                                            ),
                                          ),
                                        ],
                                      ),
                                    ],
                                  ),
                                );
                              }),
                            ],
                          ),
                        );
                      }),
                    ],
                  ),
                );
              }),
            if (loadedSourcesError != null &&
                loadedSources.isNotEmpty) ...<Widget>[
              const SizedBox(height: 8),
              StatusBox(
                text: loadedSourcesError!,
                background: const Color(0xFFFEF2F2),
                foreground: const Color(0xFFB91C1C),
              ),
            ],
          ],
        ],
      ),
    );
  }
}

class ChatPanel extends StatelessWidget {
  const ChatPanel({
    super.key,
    required this.strings,
    required this.chatHistory,
    required this.chatError,
    required this.showDebug,
    required this.chatLoading,
    required this.questionController,
    required this.chatScrollController,
    required this.onShowDebugChanged,
    required this.onAsk,
  });

  final AppStrings strings;
  final List<ChatMessage> chatHistory;
  final String? chatError;
  final bool showDebug;
  final bool chatLoading;
  final TextEditingController questionController;
  final ScrollController chatScrollController;
  final ValueChanged<bool> onShowDebugChanged;
  final Future<void> Function([String? question]) onAsk;

  @override
  Widget build(BuildContext context) {
    return GlassCard(
      child: SizedBox.expand(
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: <Widget>[
            Text(
              strings.analysisEyebrow.toUpperCase(),
              style: const TextStyle(
                fontSize: 12,
                letterSpacing: 1.4,
                color: Color(0xFF64748B),
                fontWeight: FontWeight.w700,
              ),
            ),
            const SizedBox(height: 8),
            Text(
              strings.analysisTitle,
              style: const TextStyle(fontSize: 24, fontWeight: FontWeight.w800),
            ),
            const SizedBox(height: 8),
            Text(
              strings.analysisCopy,
              style: const TextStyle(
                fontSize: 14,
                color: Color(0xFF64748B),
                height: 1.5,
              ),
            ),
            const SizedBox(height: 18),
            Expanded(
              child: Container(
                width: double.infinity,
                decoration: BoxDecoration(
                  color: const Color(0xFFF8FAFC).withValues(alpha: 0.7),
                  borderRadius: BorderRadius.circular(22),
                  border: Border.all(color: const Color(0xFFE2E8F0)),
                ),
                child: Column(
                  children: <Widget>[
                    Expanded(
                      child: Container(
                        width: double.infinity,
                        padding: const EdgeInsets.all(20),
                        child: chatHistory.isEmpty
                            ? _EmptyChatState(strings: strings)
                            : Scrollbar(
                                thumbVisibility: true,
                                controller: chatScrollController,
                                child: SingleChildScrollView(
                                  controller: chatScrollController,
                                  child: Column(
                                    children: chatHistory
                                        .map(
                                          (ChatMessage message) => Padding(
                                            padding: const EdgeInsets.only(
                                              bottom: 14,
                                            ),
                                            child: _ChatMessageCard(
                                              message: message,
                                              strings: strings,
                                              showDebug: showDebug,
                                            ),
                                          ),
                                        )
                                        .toList(),
                                  ),
                                ),
                              ),
                      ),
                    ),
                    if (chatError != null)
                      Padding(
                        padding: const EdgeInsets.fromLTRB(18, 0, 18, 12),
                        child: StatusBox(
                          text: chatError!,
                          background: const Color(0xFFFEF2F2),
                          foreground: const Color(0xFFB91C1C),
                        ),
                      ),
                    Container(
                      width: double.infinity,
                      padding: const EdgeInsets.all(14),
                      decoration: const BoxDecoration(
                        color: Color(0xF0FFFFFF),
                        borderRadius: BorderRadius.vertical(
                          bottom: Radius.circular(22),
                        ),
                        border: Border(
                          top: BorderSide(color: Color(0xFFE2E8F0)),
                        ),
                      ),
                      child: Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: <Widget>[
                          Row(
                            children: <Widget>[
                              Expanded(
                                child: Text(
                                  strings.composerHelper,
                                  style: const TextStyle(
                                    fontSize: 12,
                                    color: Color(0xFF64748B),
                                    fontWeight: FontWeight.w600,
                                  ),
                                ),
                              ),
                              Row(
                                children: <Widget>[
                                  Checkbox(
                                    visualDensity: VisualDensity.compact,
                                    materialTapTargetSize:
                                        MaterialTapTargetSize.shrinkWrap,
                                    value: showDebug,
                                    onChanged: (bool? value) =>
                                        onShowDebugChanged(value ?? false),
                                  ),
                                  Text(
                                    strings.showDebugData,
                                    style: const TextStyle(
                                      fontSize: 12,
                                      color: Color(0xFF475569),
                                      fontWeight: FontWeight.w600,
                                    ),
                                  ),
                                ],
                              ),
                            ],
                          ),
                          const SizedBox(height: 10),
                          LayoutBuilder(
                            builder:
                                (
                                  BuildContext context,
                                  BoxConstraints constraints,
                                ) {
                                  final stacked = constraints.maxWidth < 780;
                                  if (stacked) {
                                    return Column(
                                      crossAxisAlignment:
                                          CrossAxisAlignment.stretch,
                                      children: <Widget>[
                                        _ChatInputField(
                                          controller: questionController,
                                          chatLoading: chatLoading,
                                          placeholder: strings.chatPlaceholder,
                                          onAsk: onAsk,
                                        ),
                                        const SizedBox(height: 8),
                                        Align(
                                          alignment: Alignment.centerRight,
                                          child: FilledButton(
                                            onPressed: chatLoading
                                                ? null
                                                : onAsk,
                                            child: Text(
                                              chatLoading
                                                  ? strings.analyzing
                                                  : strings.send,
                                            ),
                                          ),
                                        ),
                                      ],
                                    );
                                  }

                                  return Row(
                                    crossAxisAlignment: CrossAxisAlignment.end,
                                    children: <Widget>[
                                      Expanded(
                                        child: _ChatInputField(
                                          controller: questionController,
                                          chatLoading: chatLoading,
                                          placeholder: strings.chatPlaceholder,
                                          onAsk: onAsk,
                                        ),
                                      ),
                                      const SizedBox(width: 8),
                                      FilledButton(
                                        onPressed: chatLoading ? null : onAsk,
                                        child: Text(
                                          chatLoading
                                              ? strings.analyzing
                                              : strings.send,
                                        ),
                                      ),
                                    ],
                                  );
                                },
                          ),
                          const SizedBox(height: 10),
                          Wrap(
                            spacing: 6,
                            runSpacing: 6,
                            children: strings.demoQuestions
                                .map(
                                  (String question) => ActionChip(
                                    visualDensity: VisualDensity.compact,
                                    materialTapTargetSize:
                                        MaterialTapTargetSize.shrinkWrap,
                                    label: Text(
                                      question,
                                      style: const TextStyle(fontSize: 12),
                                    ),
                                    onPressed: chatLoading
                                        ? null
                                        : () => onAsk(question),
                                  ),
                                )
                                .toList(),
                          ),
                        ],
                      ),
                    ),
                  ],
                ),
              ),
            ),
          ],
        ),
      ),
    );
  }
}

class _EmptyChatState extends StatelessWidget {
  const _EmptyChatState({required this.strings});

  final AppStrings strings;

  @override
  Widget build(BuildContext context) {
    return Container(
      width: double.infinity,
      padding: const EdgeInsets.all(28),
      decoration: BoxDecoration(
        color: Colors.white.withValues(alpha: 0.7),
        borderRadius: BorderRadius.circular(22),
        border: Border.all(
          color: const Color(0xFFCBD5E1),
          style: BorderStyle.solid,
        ),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: <Widget>[
          Text(
            strings.noAnalysisYet,
            style: const TextStyle(fontSize: 20, fontWeight: FontWeight.w700),
          ),
          const SizedBox(height: 8),
          Text(
            strings.noAnalysisCopy,
            style: const TextStyle(
              fontSize: 14,
              color: Color(0xFF475569),
              height: 1.5,
            ),
          ),
        ],
      ),
    );
  }
}

class _ChatInputField extends StatelessWidget {
  const _ChatInputField({
    required this.controller,
    required this.chatLoading,
    required this.placeholder,
    required this.onAsk,
  });

  final TextEditingController controller;
  final bool chatLoading;
  final String placeholder;
  final Future<void> Function([String? question]) onAsk;

  @override
  Widget build(BuildContext context) {
    return Focus(
      onKeyEvent: (FocusNode node, KeyEvent event) {
        if (event is! KeyDownEvent) {
          return KeyEventResult.ignored;
        }
        if (event.logicalKey != LogicalKeyboardKey.enter &&
            event.logicalKey != LogicalKeyboardKey.numpadEnter) {
          return KeyEventResult.ignored;
        }
        if (HardwareKeyboard.instance.isShiftPressed) {
          return KeyEventResult.ignored;
        }
        if (chatLoading) {
          return KeyEventResult.handled;
        }
        onAsk();
        return KeyEventResult.handled;
      },
      child: TextField(
        controller: controller,
        minLines: 2,
        maxLines: 5,
        keyboardType: TextInputType.multiline,
        textInputAction: TextInputAction.newline,
        decoration: workspaceInputDecoration(placeholder),
      ),
    );
  }
}

class _ChatMessageCard extends StatelessWidget {
  const _ChatMessageCard({
    required this.message,
    required this.strings,
    required this.showDebug,
  });

  final ChatMessage message;
  final AppStrings strings;
  final bool showDebug;

  @override
  Widget build(BuildContext context) {
    final isUser = message.role == 'user';
    return Align(
      alignment: isUser ? Alignment.centerRight : Alignment.centerLeft,
      child: Container(
        width: isUser ? null : double.infinity,
        constraints: BoxConstraints(maxWidth: isUser ? 760 : 920),
        padding: const EdgeInsets.all(16),
        decoration: BoxDecoration(
          gradient: isUser
              ? const LinearGradient(
                  colors: <Color>[Color(0xFF0F172A), Color(0xFF1D4ED8)],
                )
              : null,
          color: isUser ? null : Colors.white,
          borderRadius: BorderRadius.only(
            topLeft: const Radius.circular(20),
            topRight: const Radius.circular(20),
            bottomLeft: Radius.circular(isUser ? 20 : 6),
            bottomRight: Radius.circular(isUser ? 6 : 20),
          ),
          border: isUser ? null : Border.all(color: const Color(0xFFE2E8F0)),
          boxShadow: <BoxShadow>[
            BoxShadow(
              color: isUser ? const Color(0x331D4ED8) : const Color(0x140F172A),
              blurRadius: 24,
              offset: const Offset(0, 12),
            ),
          ],
        ),
        child: isUser
            ? Text(
                message.text ?? '',
                style: const TextStyle(color: Colors.white, height: 1.5),
              )
            : Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: <Widget>[
                  Text(
                    strings.aiAnalysis,
                    style: const TextStyle(
                      fontSize: 11,
                      letterSpacing: 1,
                      color: Color(0xFF64748B),
                      fontWeight: FontWeight.w700,
                    ),
                  ),
                  const SizedBox(height: 10),
                  Text(
                    message.answer ?? '',
                    style: const TextStyle(
                      fontSize: 14,
                      color: Color(0xFF0F172A),
                      height: 1.6,
                    ),
                  ),
                  if (message.insight != null &&
                      message.insight!.trim().isNotEmpty) ...<Widget>[
                    const SizedBox(height: 12),
                    StatusBox(
                      text: message.insight!,
                      background: const Color(0xFFEFF6FF),
                      foreground: const Color(0xFF1E40AF),
                    ),
                  ],
                  if (message.chart != null) ...<Widget>[
                    const SizedBox(height: 14),
                    SizedBox(
                      height: 320,
                      width: double.infinity,
                      child: ChartCard(
                        chart: ChartData.fromJson(message.chart!),
                        fallbackSeriesLabel: strings.resultSeries,
                      ),
                    ),
                  ],
                  if ((message.sources?.isNotEmpty ?? false) ||
                      (message.qualityFlags?.isNotEmpty ?? false) ||
                      showDebug) ...<Widget>[
                    const SizedBox(height: 12),
                    if (message.sources?.isNotEmpty ?? false)
                      Text(
                        '${strings.sourcesPrefix}: ${message.sources!.join(', ')}',
                        style: const TextStyle(
                          fontSize: 12,
                          color: Color(0xFF475569),
                        ),
                      ),
                    if (message.qualityFlags?.isNotEmpty ?? false)
                      Padding(
                        padding: const EdgeInsets.only(top: 6),
                        child: Text(
                          '${strings.qualityFlagsPrefix}: ${message.qualityFlags!.join(', ')}',
                          style: const TextStyle(
                            fontSize: 12,
                            color: Color(0xFF475569),
                          ),
                        ),
                      ),
                    if (showDebug && message.sqlUsed != null) ...<Widget>[
                      const SizedBox(height: 14),
                      DebugBlock(text: message.sqlUsed!, dark: true),
                    ],
                    if (showDebug && message.rows != null) ...<Widget>[
                      const SizedBox(height: 12),
                      DebugBlock(
                        text: const JsonEncoder.withIndent(
                          '  ',
                        ).convert(message.rows),
                      ),
                    ],
                  ],
                ],
              ),
      ),
    );
  }
}

class CardHeader extends StatelessWidget {
  const CardHeader({
    super.key,
    required this.title,
    required this.subtitle,
    required this.expanded,
    required this.onTap,
  });

  final String title;
  final String subtitle;
  final bool expanded;
  final VoidCallback onTap;

  @override
  Widget build(BuildContext context) {
    return InkWell(
      onTap: onTap,
      borderRadius: BorderRadius.circular(20),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: <Widget>[
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: <Widget>[
                Text(
                  title,
                  style: const TextStyle(
                    fontSize: 18,
                    fontWeight: FontWeight.w700,
                  ),
                ),
                const SizedBox(height: 6),
                Text(
                  subtitle,
                  style: const TextStyle(
                    fontSize: 13,
                    color: Color(0xFF64748B),
                    height: 1.45,
                  ),
                ),
              ],
            ),
          ),
          const SizedBox(width: 12),
          Text(
            expanded ? '−' : '+',
            style: const TextStyle(fontSize: 20, color: Color(0xFF64748B)),
          ),
        ],
      ),
    );
  }
}

class InputFieldBox extends StatelessWidget {
  const InputFieldBox({
    super.key,
    required this.controller,
    required this.hint,
    required this.width,
    this.obscure = false,
  });

  final TextEditingController controller;
  final String hint;
  final double width;
  final bool obscure;

  @override
  Widget build(BuildContext context) {
    return SizedBox(
      width: width,
      child: TextField(
        controller: controller,
        obscureText: obscure,
        decoration: workspaceInputDecoration(hint),
      ),
    );
  }
}

InputDecoration workspaceInputDecoration(String hint) {
  return InputDecoration(
    hintText: hint,
    filled: true,
    fillColor: Colors.white,
    contentPadding: const EdgeInsets.symmetric(horizontal: 13, vertical: 14),
    border: OutlineInputBorder(
      borderRadius: BorderRadius.circular(14),
      borderSide: const BorderSide(color: Color(0xFFDBE4EE)),
    ),
    enabledBorder: OutlineInputBorder(
      borderRadius: BorderRadius.circular(14),
      borderSide: const BorderSide(color: Color(0xFFDBE4EE)),
    ),
    focusedBorder: OutlineInputBorder(
      borderRadius: BorderRadius.circular(14),
      borderSide: const BorderSide(color: Color(0xFF3B82F6)),
    ),
  );
}
