from typing import Dict, Any, List, Optional
import time
import logging
import os
import asyncio
import json
import re
from pathlib import Path
from datetime import date, datetime

from openai import AsyncOpenAI
from sqlalchemy import create_engine

from app.services.pipeline_service import PipelineService

logger = logging.getLogger(__name__)

client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def _locale_system_instruction(locale: Optional[str], purpose: str = "response") -> str:
    requested_locale = locale or "en"
    if purpose == "sql":
        return (
            f"Requested locale code: {requested_locale}. "
            "Any human-readable aliases or labels should follow this locale. "
            "Do not use English unless the requested locale is `en`."
        )
    return (
        f"Requested locale code: {requested_locale}. "
        "All user-facing text must use this locale. "
        "Do not use English unless the requested locale is `en`."
    )


def _normalize_identifier(value: str) -> str:
    return value.replace("`", "").replace('"', "").strip()


def _extract_sources(sql: str) -> list:
    import re

    if " from (" in sql.lower() or " join (" in sql.lower():
        return []
    pattern = re.compile(r"\bfrom\s+([`\"\w\.]+)|\bjoin\s+([`\"\w\.]+)", re.IGNORECASE)
    sources = []
    for match in pattern.finditer(sql):
        ident = match.group(1) or match.group(2)
        if ident:
            sources.append(_normalize_identifier(ident))
    return sources


def _load_semantic_catalog() -> dict:
    path = Path(os.getenv("SEMANTIC_CATALOG_PATH", "app/semantic_catalog.json"))
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _safe_json_loads(value: str, fallback: Any) -> Any:
    text = (value or "").strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if len(lines) >= 3:
            text = "\n".join(lines[1:-1]).strip()
    try:
        return json.loads(text)
    except Exception:
        return fallback


def _redact_rows(rows: list) -> list:
    if not rows:
        return rows
    patterns = ("name", "email", "phone", "address", "dob", "ssn", "insurance", "patient")
    redacted = []
    for row in rows:
        new_row = dict(row)
        for key in list(new_row.keys()):
            if any(p in key.lower() for p in patterns):
                new_row[key] = "[REDACTED]"
        redacted.append(new_row)
    return redacted


def _human_label(value: str) -> str:
    return value.replace("_", " ").strip().title()


def _normalize_token(token: str) -> str:
    token = (token or "").lower().strip("_ ")
    if token.endswith("ies") and len(token) > 4:
        return token[:-3] + "y"
    if token.endswith("es") and len(token) > 4 and not token.endswith("ses"):
        return token[:-2]
    if token.endswith("s") and len(token) > 3 and not token.endswith("ss"):
        return token[:-1]
    return token


def _tokenize_text(value: str) -> set:
    raw_tokens = re.findall(r"[a-z0-9_]+", (value or "").lower())
    stopwords = {
        "the", "a", "an", "for", "of", "to", "in", "on", "by", "and", "or", "with", "show", "what",
        "which", "who", "is", "are", "was", "were", "be", "this", "that", "these", "those", "we",
        "do", "does", "did", "our", "from", "at", "into", "it", "all", "me", "tell", "give",
    }
    return {_normalize_token(token) for token in raw_tokens if token and token not in stopwords}


def _mentions_relative_time(question: str) -> bool:
    text = (question or "").lower()
    phrases = (
        "this year",
        "this month",
        "this week",
        "today",
        "current year",
        "current month",
        "current week",
        "ytd",
        "year to date",
        "month to date",
    )
    return any(phrase in text for phrase in phrases)


def _relative_time_label(question: str) -> str:
    text = (question or "").lower()
    if "this year" in text or "current year" in text or "ytd" in text or "year to date" in text:
        return "this year"
    if "this month" in text or "current month" in text or "month to date" in text:
        return "this month"
    if "this week" in text or "current week" in text:
        return "this week"
    if "today" in text:
        return "today"
    return "the requested period"


def _answer_no_data_for_question(
    question: str,
    sql_used: str,
    locale: Optional[str] = None,
) -> Dict[str, Any]:
    label = _relative_time_label(question)
    if _mentions_relative_time(question):
        return {
            "answer": f"No matching data was found for {label}.",
            "insight": "The loaded data may only cover an earlier period.",
            "quality_flags": ["no_data", "empty_requested_period"],
            "chart": None,
            "rows": [],
            "sql_used": sql_used,
            "sources": _extract_sources(sql_used) if sql_used else [],
        }
    return {
        "answer": "No matching data was returned for that question.",
        "insight": None,
        "quality_flags": ["no_data"],
        "chart": None,
        "rows": [],
        "sql_used": sql_used,
        "sources": _extract_sources(sql_used) if sql_used else [],
    }


def _heuristic_relevance(question: str, views: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    question_tokens = _tokenize_text(question)
    if not question_tokens or not views:
        return None

    intent_tokens = {
        "most", "common", "top", "highest", "lowest", "trend", "count", "total", "average", "avg",
        "sum", "year", "month", "week", "day", "distribution", "compare", "comparison",
    }
    schema_matches = []

    for view in views:
        view_tokens = _tokenize_text(view.get("view", "").replace("dw_", ""))
        column_tokens = set()
        for column in view.get("columns", []):
            column_tokens |= _tokenize_text(column.get("name", ""))
        overlap = question_tokens & (view_tokens | column_tokens)
        if overlap:
            schema_matches.append(
                {
                    "view": view.get("view"),
                    "overlap": overlap,
                    "view_tokens": view_tokens,
                    "column_tokens": column_tokens,
                }
            )

    if not schema_matches:
        return None

    best = max(schema_matches, key=lambda item: (len(item["overlap"]), len(item["column_tokens"])))
    overlap = best["overlap"]
    has_metric_intent = bool(question_tokens & intent_tokens)
    has_entity_match = any(token in overlap for token in ("bloodtest", "blood", "test", "billing", "appointment", "patient"))
    has_time_match = bool(question_tokens & {"year", "month", "week", "day"} & best["column_tokens"])

    if len(overlap) >= 2 or (has_entity_match and (has_metric_intent or has_time_match)):
        return {
            "relevant": True,
            "reason": f"Matched view {best['view']} on {', '.join(sorted(overlap))}.",
        }
    return None


def _format_number(value: Any, col_name: Optional[str] = None) -> str:
    try:
        num = float(value)
    except Exception:
        return str(value)
    key = (col_name or "").lower()
    is_percent = "percent" in key
    is_rate = "rate" in key
    is_amount = any(k in key for k in ("amount", "revenue", "total", "cost", "price", "charge", "payment"))
    if is_percent or is_rate:
        return f"{num:.1f}%"
    if is_amount:
        if abs(num) >= 1_000_000:
            return f"${num/1_000_000:.2f}M"
        if abs(num) >= 1_000:
            return f"${num/1_000:.2f}k"
        return f"${num:,.2f}"
    if abs(num) >= 1_000_000:
        return f"{num/1_000_000:.2f}M"
    if abs(num) >= 1_000:
        return f"{num/1_000:.2f}k"
    if num.is_integer():
        return f"{int(num)}"
    return f"{num:.2f}"


def _is_numeric(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _looks_temporal(column_name: str, values: List[Any]) -> bool:
    key = column_name.lower()
    if any(token in key for token in ("date", "time", "month", "year", "week", "day", "quarter")):
        return True
    sample = [value for value in values[:5] if value is not None]
    return any(isinstance(value, (date, datetime)) for value in sample)


def _normalize_label_value(value: Any, column_name: str) -> str:
    if value is None:
        return "Unknown"
    key = column_name.lower()
    month_map = {
        1: "Jan",
        2: "Feb",
        3: "Mar",
        4: "Apr",
        5: "May",
        6: "Jun",
        7: "Jul",
        8: "Aug",
        9: "Sep",
        10: "Oct",
        11: "Nov",
        12: "Dec",
    }
    if "month" in key:
        try:
            int_value = int(value)
            if int_value in month_map:
                return month_map[int_value]
        except Exception:
            pass
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _validate_select_sql(sql: str, target_database: str, allowed_views: set) -> str | None:
    text = sql.strip()
    if text.endswith(";"):
        text = text[:-1].strip()
    if ";" in text:
        return "multiple statements not allowed"
    lower = text.lower()
    if not lower.startswith("select") and not lower.startswith("with"):
        return "only SELECT statements are allowed"
    forbidden = [" drop ", " alter ", " insert ", " update ", " delete ", " truncate ", " create ", " replace "]
    if any(token in lower for token in forbidden):
        return "contains forbidden statement"
    if "date_trunc" in lower:
        return "unsupported function: DATE_TRUNC"
    sources = _extract_sources(text)
    if not sources:
        return "could not safely parse FROM/JOIN sources"
    for source in sources:
        if "." in source:
            db_name, view_name = source.split(".", 1)
            if db_name != target_database:
                return f"source database not allowed: {db_name}"
        else:
            view_name = source
        if view_name not in allowed_views:
            return f"source view not allowed: {view_name}"
    return None


def _format_history_for_prompt(history: Optional[List[Dict[str, str]]]) -> List[Dict[str, str]]:
    if not history:
        return []
    cleaned = []
    for item in history[-6:]:
        role = item.get("role", "")
        text = item.get("content", "")
        if role in {"user", "assistant"} and text:
            cleaned.append({"role": role, "content": text[:1200]})
    return cleaned


def _fallback_result_summary(question: str, rows: List[Dict[str, Any]], locale: Optional[str] = None) -> Dict[str, Any]:
    if not rows:
        return {
            "answer": "No matching data was returned for that question.",
            "insight": None,
            "quality_flags": ["no_data"],
        }

    first = rows[0]
    numeric_keys = [key for key, value in first.items() if _is_numeric(value)]
    text_keys = [key for key in first.keys() if key not in numeric_keys]
    if len(rows) == 1 and numeric_keys:
        parts = []
        for key in numeric_keys[:3]:
            parts.append(f"{_human_label(key)}: {_format_number(first.get(key), key)}")
        return {
            "answer": "; ".join(parts) if parts else "I found one matching result.",
            "insight": None,
            "quality_flags": [],
        }

    if numeric_keys and text_keys:
        metric_key = numeric_keys[0]
        label_key = text_keys[0]
        top_row = max(rows, key=lambda row: float(row.get(metric_key) or 0))
        return {
            "answer": (
                f"{_normalize_label_value(top_row.get(label_key), label_key)} leads on "
                f"{_human_label(metric_key).lower()} at {_format_number(top_row.get(metric_key), metric_key)}."
            ),
            "insight": None,
            "quality_flags": [],
        }

    return {
        "answer": f"I found {len(rows)} matching rows for that question.",
        "insight": None,
        "quality_flags": [],
    }


async def assess_question_relevance(
    question: str,
    views: list,
    history: Optional[List[Dict[str, str]]] = None,
    locale: Optional[str] = None,
) -> dict:
    heuristic = _heuristic_relevance(question, views)
    if heuristic:
        return heuristic

    requested_locale = locale or "en"
    system_prompt = f"""
You are a relevance classifier.
Given a user question, optional prior conversation, and available views with columns, decide if the data can answer the question.
Requested locale code: {requested_locale}
Return ONLY valid JSON:
{{
  "relevant": true|false,
  "reason": "short reason"
}}
"""
    user_prompt = {
        "question": question,
        "history": _format_history_for_prompt(history),
        "locale": locale or "en",
        "views": views,
    }
    response = await client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "system", "content": _locale_system_instruction(locale)},
            {"role": "user", "content": json.dumps(user_prompt)},
        ],
    )
    result = _safe_json_loads(response.choices[0].message.content or "", {"relevant": True, "reason": "fallback"})
    if not result.get("relevant", True):
        heuristic = _heuristic_relevance(question, views)
        if heuristic:
            return heuristic
    return result


async def generate_sql_with_ai(
    question: str,
    views: list,
    target_db: str,
    history: Optional[List[Dict[str, str]]] = None,
    locale: Optional[str] = None,
    extra_rules: Optional[str] = None,
) -> str | None:
    requested_locale = locale or "en"
    system_prompt = f"""
You are a SQL assistant for analytics.
Generate a single SELECT statement only.
Requested locale code: {requested_locale}

Rules:
- Query from the provided views only.
- Use fully qualified names: <database>.<view>.
- Use MySQL syntax only.
- Avoid DDL/DML and avoid nested subqueries unless absolutely required.
- Use clear aliases so the result columns read well in charts and summaries.
- Prefer aggregations that directly answer the question instead of returning raw detail rows.
- Do not add filters unless the question explicitly asks for them or the conversation context clearly requires them.
- Include ORDER BY when ranking or time trend order matters.
- Include LIMIT 500.

Return ONLY valid JSON:
{{
  "sql": "SELECT ..."
}}
"""

    user_prompt = {
        "question": question,
        "history": _format_history_for_prompt(history),
        "locale": locale or "en",
        "target_database": target_db,
        "views": views,
        "semantic_catalog": _load_semantic_catalog(),
    }

    if extra_rules:
        system_prompt = f"{system_prompt}\nAdditional rules:\n- {extra_rules}\n"

    response = await client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "system", "content": _locale_system_instruction(locale, purpose="sql")},
            {"role": "user", "content": json.dumps(user_prompt)},
        ],
    )
    parsed = _safe_json_loads(response.choices[0].message.content or "", {})
    return parsed.get("sql")


def _run_query_sync(sql_text: str, dsn: str) -> list:
    engine = create_engine(dsn, pool_pre_ping=True)
    conn = None
    cur = None
    try:
        conn = engine.raw_connection()
        cur = conn.cursor()
        cur.execute(sql_text)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description] if cur.description else []
        return [dict(zip(columns, row)) for row in rows]
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _build_chart_from_plan(rows: List[Dict[str, Any]], plan: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not rows or plan.get("chart_type") in {None, "none"}:
        return None
    x_column = plan.get("x_column")
    y_column = plan.get("y_column")
    if not x_column or not y_column:
        return None
    if x_column not in rows[0] or y_column not in rows[0]:
        return None

    x_values = [_normalize_label_value(row.get(x_column), x_column) for row in rows]
    y_values = [row.get(y_column) for row in rows]
    if not any(_is_numeric(value) for value in y_values):
        return None

    return {
        "type": plan.get("chart_type", "bar"),
        "title": plan.get("title") or f"{_human_label(y_column)} by {_human_label(x_column)}",
        "x": x_values,
        "y": y_values,
        "x_label": plan.get("x_label") or _human_label(x_column),
        "y_label": plan.get("y_label") or _human_label(y_column),
        "series_label": plan.get("series_label") or _human_label(y_column),
        "value_format": plan.get("value_format") or ("currency" if "$" in _format_number(y_values[0], y_column) else "number"),
    }


async def decide_presentation(
    question: str,
    rows: List[Dict[str, Any]],
    history: Optional[List[Dict[str, str]]] = None,
    locale: Optional[str] = None,
) -> Dict[str, Any]:
    if not rows:
        return {"show_chart": False, "chart_type": "none"}
    requested_locale = locale or "en"

    columns = list(rows[0].keys())
    numeric_columns = [column for column in columns if any(_is_numeric(row.get(column)) for row in rows[:20])]
    candidate_dimensions = [column for column in columns if column not in numeric_columns]

    heuristic = {
        "show_chart": False,
        "chart_type": "none",
        "x_column": None,
        "y_column": None,
        "title": None,
    }

    if len(rows) > 1 and numeric_columns and candidate_dimensions:
        x_column = candidate_dimensions[0]
        y_column = numeric_columns[0]
        heuristic = {
            "show_chart": True,
            "chart_type": "line" if _looks_temporal(x_column, [row.get(x_column) for row in rows]) else "bar",
            "x_column": x_column,
            "y_column": y_column,
            "title": f"{_human_label(y_column)} by {_human_label(x_column)}",
        }

    system_prompt = f"""
You are an analytics presentation planner.
Decide whether the answer should be text only, chart only, or both.
Requested locale code: {requested_locale}
Prefer:
- text only for single values, simple yes/no answers, or very small result sets
- chart + text for trends, rankings, distributions, or comparisons
- no chart when the result would be visually weak or misleading
- All user-facing text fields in your JSON response must be written in the requested locale.
- This includes title, x_label, y_label, and series_label.
- Never return English labels or titles unless the requested locale is `en`.

Return ONLY valid JSON:
{{
  "show_chart": true|false,
  "chart_type": "line|bar|pie|none",
  "x_column": "column or null",
  "y_column": "column or null",
  "title": "chart title or null",
  "x_label": "axis label or null",
  "y_label": "axis label or null",
  "series_label": "series label or null",
  "value_format": "currency|percent|number|auto"
}}
"""
    user_prompt = {
        "question": question,
        "history": _format_history_for_prompt(history),
        "locale": locale or "en",
        "columns": columns,
        "sample_rows": rows[:8],
        "row_count": len(rows),
        "heuristic": heuristic,
    }
    response = await client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "system", "content": _locale_system_instruction(locale)},
            {"role": "user", "content": json.dumps(user_prompt, default=str)},
        ],
    )
    parsed = _safe_json_loads(response.choices[0].message.content or "", heuristic)
    if parsed.get("show_chart") and parsed.get("chart_type") == "none":
        parsed["chart_type"] = heuristic.get("chart_type") or "bar"
    return parsed


async def summarize_result(
    question: str,
    rows: List[Dict[str, Any]],
    chart: Optional[Dict[str, Any]],
    history: Optional[List[Dict[str, str]]] = None,
    locale: Optional[str] = None,
) -> Dict[str, Any]:
    if not rows:
        return {
            "answer": "No matching data was returned for that question.",
            "insight": None,
            "quality_flags": ["no_data"],
        }
    requested_locale = locale or "en"

    system_prompt = f"""
You are an analytics writer.
Write a clear answer grounded only in the provided query result.
Requested locale code: {requested_locale}

Rules:
- Be concise but natural, not robotic.
- Answer the user's question directly in 1-3 short paragraphs.
- If there is a clear top performer, trend, drop, concentration, or imbalance, mention it plainly.
- Refer to months and time labels naturally.
- Use currency formatting when the metric is money.
- Write the answer in the requested language.
- Every user-facing string you return must be in the requested locale.
- This includes both `answer` and `insight`.
- Never answer in English unless the requested locale is `en`.
- Do not mention SQL, tables, columns, or implementation details.
- Do not invent facts not present in the result.

Return ONLY valid JSON:
{{
  "answer": "main answer text",
  "insight": "optional short supporting takeaway or null",
  "quality_flags": ["optional_flag"]
}}
"""
    user_prompt = {
        "question": question,
        "history": _format_history_for_prompt(history),
        "response_language": locale or "en",
        "row_count": len(rows),
        "columns": list(rows[0].keys()),
        "sample_rows": rows[:12],
        "chart": chart,
    }
    response = await client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.2,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "system", "content": _locale_system_instruction(locale)},
            {"role": "user", "content": json.dumps(user_prompt, default=str)},
        ],
    )
    parsed = _safe_json_loads(response.choices[0].message.content or "", {})
    if parsed.get("answer"):
        flags = parsed.get("quality_flags") or []
        return {
            "answer": parsed["answer"],
            "insight": parsed.get("insight"),
            "quality_flags": flags,
        }
    return _fallback_result_summary(question, rows, locale=locale)


async def ask_agent(
    question: str,
    history: Optional[List[Dict[str, str]]] = None,
    locale: Optional[str] = None,
) -> Dict[str, Any]:
    start = time.perf_counter()
    pipeline = PipelineService()
    target_db = pipeline.dw_database

    if not os.getenv("OPENAI_API_KEY"):
        return {
            "answer": "AI is not configured. Set OPENAI_API_KEY to enable automatic analysis.",
            "chart": None,
            "rows": [],
            "sql_used": "",
            "sources": [],
            "quality_flags": ["ai_not_configured"],
            "insight": None,
        }

    views = pipeline.list_views_with_columns(database=target_db, view_prefix="dw_")
    if not views:
        return {
            "answer": "No analysis views are available yet. Load data first.",
            "chart": None,
            "rows": [],
            "sql_used": "",
            "sources": [],
            "quality_flags": ["no_views"],
            "insight": None,
        }

    relevance = await assess_question_relevance(question, views, history=history, locale=locale)
    if not relevance.get("relevant", True):
        return {
            "answer": "That question does not line up with the data currently loaded.",
            "chart": None,
            "rows": [],
            "sql_used": "",
            "sources": [],
            "quality_flags": ["question_not_relevant"],
            "insight": relevance.get("reason"),
        }

    sql = await generate_sql_with_ai(question, views, target_db, history=history, locale=locale)
    if not sql:
        return {
            "answer": "I could not build a reliable query for that question.",
            "chart": None,
            "rows": [],
            "sql_used": "",
            "sources": [],
            "quality_flags": ["sql_generation_failed"],
            "insight": None,
        }

    allowed_views = {view["view"] for view in views}
    validation_error = _validate_select_sql(sql, target_db, allowed_views)
    if validation_error:
        return {
            "answer": f"I generated a query, but it was rejected for safety: {validation_error}.",
            "chart": None,
            "rows": [],
            "sql_used": sql,
            "sources": [],
            "quality_flags": ["sql_rejected"],
            "insight": None,
        }

    async def _run_query(sql_text: str):
        run_start = time.perf_counter()
        try:
            rows_result = await asyncio.to_thread(_run_query_sync, sql_text, pipeline.db_dsn)
        except Exception:
            rows_result = []
        return rows_result, time.perf_counter() - run_start

    rows, run_time = await _run_query(sql)
    sql_used = sql

    if not rows and " where " in sql.lower():
        relaxed_sql = await generate_sql_with_ai(
            question,
            views,
            target_db,
            history=history,
            locale=locale,
            extra_rules="Do not use WHERE clauses unless the user explicitly asked for a filter.",
        )
        if relaxed_sql and relaxed_sql != sql:
            relaxed_error = _validate_select_sql(relaxed_sql, target_db, allowed_views)
            if not relaxed_error:
                relaxed_rows, relaxed_run_time = await _run_query(relaxed_sql)
                if relaxed_rows:
                    rows = relaxed_rows
                    run_time = relaxed_run_time
                    sql_used = relaxed_sql

    if not rows:
        return _answer_no_data_for_question(question, sql_used, locale=locale)

    presentation = await decide_presentation(question, rows, history=history, locale=locale) if rows else {"show_chart": False}
    chart = _build_chart_from_plan(rows, presentation) if presentation.get("show_chart") else None

    visible_rows = _redact_rows(rows) if os.getenv("REDACT_PII", "false").lower() in {"1", "true", "yes"} else rows
    summary = await summarize_result(question, visible_rows, chart, history=history, locale=locale)

    total_time = time.perf_counter() - start
    logger.info(
        "ask_agent: question=%s views=%d rows=%d run_time=%.3fs total_time=%.3fs",
        question,
        len(views),
        len(rows),
        run_time,
        total_time,
    )

    used_sources = _extract_sources(sql_used) if sql_used else []
    return {
        "answer": summary.get("answer", ""),
        "chart": chart,
        "rows": visible_rows[:500] if len(visible_rows) > 500 else visible_rows,
        "sql_used": sql_used,
        "sources": used_sources,
        "quality_flags": summary.get("quality_flags", []),
        "insight": summary.get("insight"),
    }
