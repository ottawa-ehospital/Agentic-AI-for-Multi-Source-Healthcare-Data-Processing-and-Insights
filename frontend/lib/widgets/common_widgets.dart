import 'package:flutter/material.dart';

class GlassCard extends StatelessWidget {
  const GlassCard({
    super.key,
    required this.child,
    this.padding = const EdgeInsets.all(18),
  });

  final Widget child;
  final EdgeInsetsGeometry padding;

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: padding,
      decoration: BoxDecoration(
        color: Colors.white.withValues(alpha: 0.88),
        borderRadius: BorderRadius.circular(24),
        border: Border.all(color: const Color(0x3894A3B8)),
        boxShadow: const <BoxShadow>[
          BoxShadow(
            color: Color(0x140F172A),
            blurRadius: 45,
            offset: Offset(0, 18),
          ),
        ],
      ),
      child: child,
    );
  }
}

class StatusBox extends StatelessWidget {
  const StatusBox({
    super.key,
    required this.text,
    required this.background,
    required this.foreground,
  });

  final String text;
  final Color background;
  final Color foreground;

  @override
  Widget build(BuildContext context) {
    return Container(
      width: double.infinity,
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 10),
      decoration: BoxDecoration(
        color: background,
        borderRadius: BorderRadius.circular(14),
      ),
      child: Text(
        text,
        style: TextStyle(fontSize: 13, color: foreground, height: 1.45),
      ),
    );
  }
}

class DebugBlock extends StatelessWidget {
  const DebugBlock({super.key, required this.text, this.dark = false});

  final String text;
  final bool dark;

  @override
  Widget build(BuildContext context) {
    return Container(
      width: double.infinity,
      padding: const EdgeInsets.all(14),
      decoration: BoxDecoration(
        color: dark ? const Color(0xFF0F172A) : const Color(0xFFF8FAFC),
        borderRadius: BorderRadius.circular(16),
      ),
      child: SelectableText(
        text,
        style: TextStyle(
          fontSize: 12,
          height: 1.5,
          color: dark ? const Color(0xFFDBEAFE) : const Color(0xFF0F172A),
        ),
      ),
    );
  }
}
