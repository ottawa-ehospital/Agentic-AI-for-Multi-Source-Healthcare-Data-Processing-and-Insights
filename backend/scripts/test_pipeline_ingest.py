import os
import json
import httpx

api_url = os.getenv("PIPELINE_API_URL", "http://127.0.0.1:8000/api/pipeline/ingest")

tables_env = os.getenv("PIPELINE_TABLES", "")
if tables_env:
    tables = [t.strip() for t in tables_env.split(",") if t.strip()]
else:
    tables = ["appointments", "billing_records", "bloodtests", "patients_registration"]

source_type = os.getenv("PIPELINE_SOURCE_TYPE", "mysql").strip() or "mysql"

if source_type == "table_api":
    payload = {
        "source": {
            "source_type": "table_api",
            "base_url": os.getenv("TABLE_API_BASE_URL", "https://example.com"),
            "api_token": os.getenv("TABLE_API_TOKEN") or None,
        },
        "tables": tables,
        "staging_prefix": os.getenv("PIPELINE_STAGING_PREFIX", "stg_"),
        "batch_size": int(os.getenv("PIPELINE_BATCH_SIZE", "1000")),
    }
else:
    payload = {
        "source": {
            "source_type": "mysql",
            "host": os.getenv("SOURCE_DB_HOST", "localhost"),
            "port": int(os.getenv("SOURCE_DB_PORT", "3306")),
            "user": os.getenv("SOURCE_DB_USER", ""),
            "password": os.getenv("SOURCE_DB_PASSWORD", ""),
            "database": os.getenv("SOURCE_DB_NAME", ""),
        },
        "tables": tables,
        "staging_prefix": os.getenv("PIPELINE_STAGING_PREFIX", "stg_"),
        "batch_size": int(os.getenv("PIPELINE_BATCH_SIZE", "1000")),
    }

with httpx.Client(timeout=60.0) as client:
    resp = client.post(api_url, json=payload)

print(resp.status_code)
print(resp.headers.get("content-type"))
try:
    print(json.dumps(resp.json(), indent=2, default=str))
except Exception:
    print(resp.text)
