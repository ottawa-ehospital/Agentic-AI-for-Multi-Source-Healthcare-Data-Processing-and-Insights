# Analytics Workspace Backend

FastAPI backend for:
- ingesting source tables into a warehouse landing layer
- generating scoped `dw_*` views for analysis
- answering natural-language questions over the loaded views

## Run

Install dependencies:

```bash
python3 -m pip install -r requirements.txt
```

Start the API:

```bash
PYTHONPATH=. uvicorn app.main:app --reload --port 8000
```

## Environment

Warehouse connection:

- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASSWORD`
- `DB_NAME`

Optional:

- `OPENAI_API_KEY` for AI view proposals and analysis generation
- `SEMANTIC_CATALOG_PATH` to override `app/semantic_catalog.json`
- `REDACT_PII=true` to mask common sensitive fields in `/api/ask`
- `TABLE_API_VERIFY_SSL=true` to enforce SSL verification for `table_api` sources

## Main Endpoints

- `POST /api/ask`
- `POST /api/pipeline/ingest`
- `POST /api/pipeline/list_databases`
- `POST /api/pipeline/list_tables`
- `GET /api/pipeline/loaded_sources`
- `POST /api/pipeline/refresh`

## Ask Example

```bash
curl -X POST "http://127.0.0.1:8000/api/ask" \
  -H "Content-Type: application/json" \
  -d '{"question":"Most common blood tests.","locale":"en"}'
```

## Ingest Example: MySQL

```json
{
  "source": {
    "source_type": "mysql",
    "host": "source-db-host",
    "port": 3306,
    "user": "source_user",
    "password": "source_password",
    "database": "source_db"
  },
  "tables": ["appointments", "bloodtests"]
}
```

## Ingest Example: Table API

```json
{
  "source": {
    "source_type": "table_api",
    "base_url": "https://example.com",
    "api_token": "optional-token"
  },
  "tables": ["patient_feedback"]
}
```

The warehouse uses source-scoped landing and view names so tables with the same source name do not collide across different systems.
