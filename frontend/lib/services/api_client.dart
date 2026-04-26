// ignore_for_file: avoid_web_libraries_in_flutter, deprecated_member_use

import 'dart:convert';
import 'dart:html' as html;

import 'package:elg6131/models/workspace_models.dart';

class ApiClient {
  Future<Map<String, dynamic>> getJson(String url) async {
    return _requestJson(
      url,
      method: 'GET',
      requestHeaders: <String, String>{'Accept': 'application/json'},
    );
  }

  Future<Map<String, dynamic>> postJson(
    String url,
    Map<String, dynamic> body,
  ) async {
    return _requestJson(
      url,
      method: 'POST',
      sendData: jsonEncode(body),
      requestHeaders: <String, String>{
        'Accept': 'application/json',
        'Content-Type': 'application/json',
      },
    );
  }

  Future<Map<String, dynamic>> _requestJson(
    String url, {
    required String method,
    Map<String, String>? requestHeaders,
    String? sendData,
  }) async {
    try {
      final response = await html.HttpRequest.request(
        url,
        method: method,
        sendData: sendData,
        requestHeaders: requestHeaders,
      );
      return _decodeResponse(response);
    } on html.ProgressEvent {
      throw ApiException(
        statusCode: 0,
        message:
            'The database connection could not be established. Check the credentials and connection details, then try again.',
        detailMap: <String, dynamic>{'url': url, 'type': 'network_or_cors'},
      );
    } catch (error) {
      throw ApiException(
        statusCode: 0,
        message: 'Request failed for $url: $error',
        detailMap: <String, dynamic>{'url': url, 'type': 'request_error'},
      );
    }
  }

  Map<String, dynamic> _decodeResponse(html.HttpRequest response) {
    final responseText = response.responseText ?? '{}';
    final decoded = responseText.trim().isEmpty
        ? <String, dynamic>{}
        : jsonDecode(responseText) as Object?;

    if (response.status != null && response.status! >= 400) {
      final errorMap = decoded is Map<String, dynamic>
          ? decoded
          : <String, dynamic>{'detail': responseText};
      final detail = errorMap['detail'];
      final message = detail is Map<String, dynamic>
          ? detail['message']?.toString() ??
                detail['error']?.toString() ??
                detail.toString()
          : detail?.toString() ??
                errorMap['message']?.toString() ??
                errorMap['error']?.toString() ??
                response.statusText ??
                'Request failed';
      throw ApiException(
        statusCode: response.status!,
        message: message,
        detailMap: detail is Map<String, dynamic> ? detail : errorMap,
      );
    }

    if (decoded is Map<String, dynamic>) {
      return decoded;
    }
    return <String, dynamic>{'data': decoded};
  }
}
