import 'dart:math' as math;

import 'package:elg6131/models/workspace_models.dart';
import 'package:elg6131/utils/workspace_helpers.dart';
import 'package:flutter/material.dart';

class ChartCard extends StatelessWidget {
  const ChartCard({
    super.key,
    required this.chart,
    required this.fallbackSeriesLabel,
  });

  final ChartData chart;
  final String fallbackSeriesLabel;

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(18),
        border: Border.all(color: const Color(0xFFE2E8F0)),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: <Widget>[
          if (chart.title.isNotEmpty)
            Padding(
              padding: const EdgeInsets.only(bottom: 12),
              child: Text(
                chart.title,
                style: const TextStyle(
                  fontSize: 16,
                  fontWeight: FontWeight.w700,
                ),
              ),
            ),
          Expanded(
            child: CustomPaint(
              painter: ChartPainter(chart: chart),
              child: Container(),
            ),
          ),
          const SizedBox(height: 8),
          Text(
            chart.seriesLabel.isEmpty ? fallbackSeriesLabel : chart.seriesLabel,
            style: const TextStyle(fontSize: 12, color: Color(0xFF64748B)),
          ),
        ],
      ),
    );
  }
}

class ChartPainter extends CustomPainter {
  ChartPainter({required this.chart});

  final ChartData chart;

  static const _axisColor = Color(0xFFCBD5E1);
  static const _gridColor = Color(0xFFE2E8F0);
  static const _seriesColor = Color(0xFF0F766E);
  static const _seriesFill = Color(0x3315A39A);
  static const _textColor = Color(0xFF475569);

  @override
  void paint(Canvas canvas, Size size) {
    if (chart.x.isEmpty || chart.y.isEmpty) {
      return;
    }
    switch (chart.type) {
      case 'bar':
        _paintCartesian(canvas, size, bar: true);
        return;
      case 'pie':
        _paintPie(canvas, size);
        return;
      default:
        _paintCartesian(canvas, size, bar: false);
        return;
    }
  }

  void _paintCartesian(Canvas canvas, Size size, {required bool bar}) {
    const left = 48.0;
    const right = 20.0;
    const top = 16.0;
    const bottom = 42.0;
    final chartRect = Rect.fromLTWH(
      left,
      top,
      math.max(0, size.width - left - right),
      math.max(0, size.height - top - bottom),
    );

    final yValues = chart.y.map((num value) => value.toDouble()).toList();
    final minY = math.min(0, yValues.reduce(math.min));
    final maxY = yValues.reduce(math.max);
    final span = (maxY - minY).abs() < 0.0001 ? 1.0 : maxY - minY;

    final axisPaint = Paint()
      ..color = _axisColor
      ..strokeWidth = 1;
    final gridPaint = Paint()
      ..color = _gridColor
      ..strokeWidth = 1;

    for (var i = 0; i < 4; i++) {
      final ratio = i / 3;
      final y = chartRect.bottom - chartRect.height * ratio;
      canvas.drawLine(
        Offset(chartRect.left, y),
        Offset(chartRect.right, y),
        gridPaint,
      );
      _drawText(
        canvas,
        formatChartTick(minY + span * ratio, chart.valueFormat),
        Offset(0, y - 8),
      );
    }

    canvas.drawLine(
      Offset(chartRect.left, chartRect.bottom),
      Offset(chartRect.right, chartRect.bottom),
      axisPaint,
    );
    canvas.drawLine(
      Offset(chartRect.left, chartRect.top),
      Offset(chartRect.left, chartRect.bottom),
      axisPaint,
    );

    if (bar) {
      final barPaint = Paint()..color = _seriesColor;
      final width = chartRect.width / math.max(1, chart.x.length);
      for (var i = 0; i < chart.x.length; i++) {
        final value = yValues[i];
        final heightRatio = ((value - minY) / span).clamp(0.0, 1.0);
        final barHeight = chartRect.height * heightRatio;
        final rect = RRect.fromRectAndRadius(
          Rect.fromLTWH(
            chartRect.left + i * width + width * 0.16,
            chartRect.bottom - barHeight,
            width * 0.68,
            barHeight,
          ),
          const Radius.circular(10),
        );
        canvas.drawRRect(rect, barPaint);
        _drawText(
          canvas,
          chart.x[i],
          Offset(chartRect.left + i * width + 4, chartRect.bottom + 8),
          maxWidth: width - 8,
        );
      }
      return;
    }

    final linePaint = Paint()
      ..color = _seriesColor
      ..strokeWidth = 3
      ..style = PaintingStyle.stroke;
    final fillPaint = Paint()
      ..color = _seriesFill
      ..style = PaintingStyle.fill;
    final pointPaint = Paint()..color = _seriesColor;
    final path = Path();
    final fillPath = Path();

    for (var i = 0; i < chart.x.length; i++) {
      final x =
          chartRect.left +
          (chart.x.length == 1
              ? 0
              : chartRect.width * i / (chart.x.length - 1));
      final y =
          chartRect.bottom - chartRect.height * ((yValues[i] - minY) / span);
      if (i == 0) {
        path.moveTo(x, y);
        fillPath.moveTo(x, chartRect.bottom);
        fillPath.lineTo(x, y);
      } else {
        path.lineTo(x, y);
        fillPath.lineTo(x, y);
      }
      canvas.drawCircle(Offset(x, y), 4, pointPaint);
      _drawText(
        canvas,
        chart.x[i],
        Offset(x - 22, chartRect.bottom + 8),
        maxWidth: 48,
        align: TextAlign.center,
      );
    }

    fillPath.lineTo(chartRect.right, chartRect.bottom);
    fillPath.close();
    canvas.drawPath(fillPath, fillPaint);
    canvas.drawPath(path, linePaint);
  }

  void _paintPie(Canvas canvas, Size size) {
    final sum = chart.y.fold<double>(
      0,
      (double total, num value) => total + value.toDouble(),
    );
    if (sum <= 0) {
      return;
    }
    final radius = math.min(size.width * 0.28, size.height * 0.3);
    final center = Offset(size.width * 0.34, size.height * 0.48);
    final rect = Rect.fromCircle(center: center, radius: radius);
    final colors = <Color>[
      const Color(0xFF0F766E),
      const Color(0xFF1D4ED8),
      const Color(0xFFF59E0B),
      const Color(0xFFDC2626),
      const Color(0xFF7C3AED),
      const Color(0xFF14B8A6),
    ];

    var start = -math.pi / 2;
    for (var i = 0; i < chart.y.length; i++) {
      final sweep = (chart.y[i].toDouble() / sum) * math.pi * 2;
      final paint = Paint()
        ..color = colors[i % colors.length]
        ..style = PaintingStyle.fill;
      canvas.drawArc(rect, start, sweep, true, paint);
      start += sweep;
    }

    final legendStartX = size.width * 0.62;
    for (var i = 0; i < chart.x.length; i++) {
      final top = 28.0 + i * 24.0;
      final color = colors[i % colors.length];
      canvas.drawRRect(
        RRect.fromRectAndRadius(
          Rect.fromLTWH(legendStartX, top, 12, 12),
          const Radius.circular(4),
        ),
        Paint()..color = color,
      );
      final label =
          '${chart.x[i]}  ${formatChartTick(chart.y[i], chart.valueFormat)}';
      _drawText(
        canvas,
        label,
        Offset(legendStartX + 20, top - 2),
        maxWidth: size.width * 0.3,
      );
    }
  }

  void _drawText(
    Canvas canvas,
    String text,
    Offset offset, {
    double? maxWidth,
    TextAlign align = TextAlign.left,
  }) {
    final painter = TextPainter(
      text: TextSpan(
        text: text,
        style: const TextStyle(fontSize: 11, color: _textColor, height: 1.2),
      ),
      textDirection: TextDirection.ltr,
      textAlign: align,
      maxLines: 2,
      ellipsis: '…',
    )..layout(maxWidth: maxWidth ?? double.infinity);
    painter.paint(canvas, offset);
  }

  @override
  bool shouldRepaint(covariant ChartPainter oldDelegate) {
    return oldDelegate.chart != chart;
  }
}
