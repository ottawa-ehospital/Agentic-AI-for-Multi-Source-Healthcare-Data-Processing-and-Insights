import 'package:elg6131/workspace_page.dart';
import 'package:flutter/material.dart';

const defaultBaseUrl = String.fromEnvironment(
  'BACKEND_URL',
  defaultValue: 'http://localhost:8000',
);

class WarehouseApp extends StatelessWidget {
  const WarehouseApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Warehouse Analysis Workspace',
      debugShowCheckedModeBanner: false,
      theme: ThemeData(
        useMaterial3: true,
        fontFamily: 'Avenir Next',
        colorScheme: ColorScheme.fromSeed(
          seedColor: const Color(0xFF0F172A),
          brightness: Brightness.light,
        ),
        scaffoldBackgroundColor: const Color(0xFFF6F8FC),
      ),
      home: const WorkspacePage(baseUrl: defaultBaseUrl),
    );
  }
}
