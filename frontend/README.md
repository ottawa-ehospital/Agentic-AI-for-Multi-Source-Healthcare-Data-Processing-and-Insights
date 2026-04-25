# ELG6131 Flutter Web App

This repository now contains a Flutter web client for the warehouse analytics workspace.

## Run locally

```bash
flutter pub get
flutter run -d chrome --dart-define=BACKEND_URL=http://localhost:8000
```

## Build for web

```bash
flutter build web --dart-define=BACKEND_URL=http://localhost:8000
```

If `BACKEND_URL` is omitted, the app defaults to `http://localhost:8000`.
