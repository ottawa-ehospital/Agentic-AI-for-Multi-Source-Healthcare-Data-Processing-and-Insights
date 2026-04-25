from typing import List, Dict, Any, Optional
import os
from openai import AsyncOpenAI

from app.services.pipeline_service import PipelineService

client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))


async def propose_views(
    tables: List[Dict[str, str]],
    target_database: str,
    staging_prefix: str = "stg_",
    view_prefix: str = "dw_",
    goals: Optional[str] = None,
) -> Dict[str, Any]:
    pipeline = PipelineService()
    schemas = []
    for table in tables:
        schema = pipeline.get_landing_table_schema(
            database=target_database,
            table=table.get("source_table", ""),
            staging_prefix=staging_prefix,
            target_table=table.get("target_table"),
        )
        schema["suggested_view_name"] = table.get("target_view") or pipeline._view_name_for_target_table(
            table.get("target_table", ""),
            staging_prefix=staging_prefix,
            view_prefix=view_prefix,
        )
        schemas.append(schema)

    system_prompt = """
You are a data warehouse modeling assistant.
Generate SQL for MySQL views only.
Rules:
- Only output JSON.
- Create VIEW statements that read from landing tables.
- Do not use DROP or ALTER.
- Prefer simple, readable column aliases.
- Include date breakdowns for date/time columns when helpful.
- Use the provided suggested_view_name for each schema.
Output format:
{
  "views": [
    {
      "name": "view_name",
      "sql": "CREATE OR REPLACE VIEW ..."
    }
  ]
}
"""

    user_prompt = {
        "target_database": target_database,
        "staging_prefix": staging_prefix,
        "view_prefix": view_prefix,
        "goals": goals or "Provide analysis-friendly views.",
        "schemas": schemas,
    }

    response = await client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": str(user_prompt)},
        ],
    )

    content = response.choices[0].message.content
    import json
    try:
        parsed = json.loads(content)
    except Exception:
        parsed = {"views": []}
    return {"schemas": schemas, "proposal": parsed}


def _normalize_identifier(value: str) -> str:
    return value.replace("`", "").replace('"', "").strip()


def _extract_sources(sql: str) -> List[str]:
    import re

    sources: List[str] = []
    if " from (" in sql.lower() or " join (" in sql.lower():
        return []
    pattern = re.compile(r"\bfrom\s+([`\"\w\.]+)|\bjoin\s+([`\"\w\.]+)", re.IGNORECASE)
    for match in pattern.finditer(sql):
        ident = match.group(1) or match.group(2)
        if ident:
            sources.append(_normalize_identifier(ident))
    return sources


def validate_view_sql(
    sql: str,
    target_database: str,
    staging_prefix: str,
    view_prefix: str,
) -> Optional[str]:
    text = sql.strip()
    if text.endswith(";"):
        text = text[:-1].strip()
    if ";" in text:
        return "multiple statements not allowed"
    lower = text.lower()
    if not lower.startswith("create or replace view"):
        return "must start with CREATE OR REPLACE VIEW"
    if " as select " not in lower:
        return "must define view as SELECT"
    forbidden = [" drop ", " alter ", " insert ", " update ", " delete ", " truncate ", " create table "]
    if any(token in lower for token in forbidden):
        return "contains forbidden statement"
    import re
    match = re.search(r"create\s+or\s+replace\s+view\s+([`\"\w\.]+)", text, re.IGNORECASE)
    if not match:
        return "could not parse view name"
    view_ident = _normalize_identifier(match.group(1))
    if "." in view_ident:
        db_name, view_name = view_ident.split(".", 1)
        if db_name != target_database:
            return f"view database not allowed: {db_name}"
    else:
        view_name = view_ident
    if not view_name.startswith(view_prefix):
        return f"view name must start with {view_prefix}"
    sources = _extract_sources(text)
    if not sources:
        return "could not safely parse FROM/JOIN sources"
    for source in sources:
        if "." in source:
            db_name, table_name = source.split(".", 1)
            if db_name != target_database:
                return f"source database not allowed: {db_name}"
        else:
            table_name = source
        if not (table_name.startswith(staging_prefix) or table_name.startswith(view_prefix)):
            return f"source table not allowed: {table_name}"
    return None
