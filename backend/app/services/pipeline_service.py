import os
from typing import List, Optional, Dict, Any
from urllib.parse import quote_plus
from urllib.parse import urlparse, quote
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
import json
import ssl
import hashlib
import re

from sqlalchemy import create_engine


class RefreshCredentialsRequired(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

class PipelineService:
    """
    Ingest source tables into landing tables inside the DW database.
    """

    def __init__(self, db_dsn: Optional[str] = None, dw_database: str = "EHOSPITAL_DW"):
        self.db_dsn = db_dsn or self._resolve_dw_dsn()
        if not self.db_dsn:
            raise ValueError("Database DSN required (set DATABASE_URL/DW_DATABASE_URL or DB_* envs, or pass db_dsn).")
        self.dw_database = dw_database

    @staticmethod
    def _resolve_dw_dsn() -> Optional[str]:
        dsn = os.environ.get("DATABASE_URL") or os.environ.get("DW_DATABASE_URL")
        if dsn:
            return dsn
        host = os.environ.get("DB_HOST")
        if not host:
            return None
        port = os.environ.get("DB_PORT", "3306")
        user = quote_plus(os.environ.get("DB_USER", ""))
        password = quote_plus(os.environ.get("DB_PASSWORD", ""))
        db_name = os.environ.get("DB_NAME", "")
        return f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}"

    @staticmethod
    def _build_mysql_dsn(host: str, port: int, user: str, password: str, database: str) -> str:
        safe_user = quote_plus(user)
        safe_password = quote_plus(password)
        return f"mysql+pymysql://{safe_user}:{safe_password}@{host}:{port}/{database}"

    @staticmethod
    def _escape_identifier(value: str) -> str:
        return f"`{value.replace('`', '``')}`"

    @staticmethod
    def _format_default(value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="ignore")
        if isinstance(value, (int, float)):
            return str(value)
        text = str(value)
        upper = text.upper()
        if upper in {"CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP()", "NOW()", "CURRENT_DATE", "CURRENT_TIME"}:
            return text
        escaped = text.replace("'", "''")
        return f"'{escaped}'"

    @staticmethod
    def _slug_identifier(value: str) -> str:
        cleaned = re.sub(r"[^a-z0-9_]+", "_", (value or "").lower())
        cleaned = re.sub(r"_+", "_", cleaned).strip("_")
        return cleaned or "item"

    def _source_scope_suffix(
        self,
        source_type: str,
        source_host: str,
        source_port: int,
        source_database: str,
    ) -> str:
        raw = f"{source_type}|{source_host}|{source_port}|{source_database}"
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:10]

    def _build_target_table_name(
        self,
        staging_prefix: str,
        source_table: str,
        source_type: str,
        source_host: str,
        source_port: int,
        source_database: str,
    ) -> str:
        return (
            f"{staging_prefix}"
            f"{self._slug_identifier(source_table)}"
            f"__{self._source_scope_suffix(source_type, source_host, source_port, source_database)}"
        )

    def _view_name_for_target_table(
        self,
        target_table: str,
        staging_prefix: str = "stg_",
        view_prefix: str = "dw_",
    ) -> str:
        if target_table.startswith(staging_prefix):
            return f"{view_prefix}{target_table[len(staging_prefix):]}"
        return f"{view_prefix}{target_table}"

    def _get_source_columns(self, conn, database: str, table: str) -> List[Dict[str, Any]]:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ORDINAL_POSITION
                """,
                (database, table),
            )
            rows = cur.fetchall()
        finally:
            try:
                cur.close()
            except Exception:
                pass
        return [
            {
                "name": row[0],
                "type": row[1],
                "nullable": row[2] == "YES",
                "default": row[3],
                "extra": row[4],
            }
            for row in rows
        ]

    def _get_columns_with_types(self, conn, database: str, table: str) -> List[Dict[str, Any]]:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT COLUMN_NAME, DATA_TYPE
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ORDINAL_POSITION
                """,
                (database, table),
            )
            rows = cur.fetchall()
        finally:
            try:
                cur.close()
            except Exception:
                pass
        return [{"name": row[0], "data_type": row[1]} for row in rows]

    def _get_source_primary_keys(self, conn, database: str, table: str) -> List[str]:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT kcu.COLUMN_NAME
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                 AND tc.table_name = kcu.table_name
                WHERE tc.constraint_type = 'PRIMARY KEY'
                  AND tc.table_schema = %s
                  AND tc.table_name = %s
                ORDER BY kcu.ORDINAL_POSITION
                """,
                (database, table),
            )
            rows = cur.fetchall()
        finally:
            try:
                cur.close()
            except Exception:
                pass
        return [row[0] for row in rows]

    def _ensure_metadata_tables(self, conn, target_db: str) -> None:
        ddl = [
            f"""
            CREATE TABLE IF NOT EXISTS {self._escape_identifier(target_db)}.ingestion_loads (
                `id` BIGINT NOT NULL AUTO_INCREMENT,
                `source_type` VARCHAR(32),
                `source_host` VARCHAR(255),
                `source_port` VARCHAR(32),
                `source_database` VARCHAR(255),
                `source_table` VARCHAR(255),
                `target_table` VARCHAR(255),
                `target_view` VARCHAR(255),
                `load_mode` VARCHAR(32),
                `row_count` BIGINT,
                `loaded_at` VARCHAR(32),
                PRIMARY KEY (`id`)
            ) ENGINE=InnoDB
            """,
        ]
        cur = conn.cursor()
        try:
            cur.execute(f"DROP TABLE IF EXISTS {self._escape_identifier(target_db)}.ingestion_watermarks")
            cur.execute(f"DROP TABLE IF EXISTS {self._escape_identifier(target_db)}.ingestion_schema")
            cur.execute(f"DROP TABLE IF EXISTS {self._escape_identifier(target_db)}.ingestion_sources")
            for stmt in ddl:
                cur.execute(stmt)
            cur.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = 'ingestion_loads' AND column_name = 'source_type'
                """,
                (target_db,),
            )
            has_source_type = cur.fetchone()
            if has_source_type and int(has_source_type[0] or 0) == 0:
                cur.execute(
                    f"ALTER TABLE {self._escape_identifier(target_db)}.ingestion_loads "
                    "ADD COLUMN `source_type` VARCHAR(32) AFTER `id`"
                )
            cur.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = 'ingestion_loads' AND column_name = 'target_view'
                """,
                (target_db,),
            )
            has_target_view = cur.fetchone()
            if has_target_view and int(has_target_view[0] or 0) == 0:
                cur.execute(
                    f"ALTER TABLE {self._escape_identifier(target_db)}.ingestion_loads "
                    "ADD COLUMN `target_view` VARCHAR(255) AFTER `target_table`"
                )
            conn.commit()
        finally:
            try:
                cur.close()
            except Exception:
                pass

    def _count_rows(self, conn, database: str, table: str) -> int:
        cur = conn.cursor()
        try:
            cur.execute(
                f"SELECT COUNT(*) FROM {self._escape_identifier(database)}.{self._escape_identifier(table)}"
            )
            row = cur.fetchone()
        finally:
            try:
                cur.close()
            except Exception:
                pass
        return int(row[0]) if row else 0

    def _count_nulls(self, conn, database: str, table: str, columns: List[str]) -> Dict[str, int]:
        if not columns:
            return {}
        exprs = ", ".join(
            f"SUM({self._escape_identifier(col)} IS NULL) AS {self._escape_identifier(col)}" for col in columns
        )
        cur = conn.cursor()
        try:
            cur.execute(
                f"SELECT {exprs} FROM {self._escape_identifier(database)}.{self._escape_identifier(table)}"
            )
            row = cur.fetchone()
            result = {}
            if row:
                for idx, col in enumerate(columns):
                    result[col] = int(row[idx] or 0)
        finally:
            try:
                cur.close()
            except Exception:
                pass
        return result

    def get_landing_table_schema(
        self,
        database: str,
        table: str,
        staging_prefix: str = "stg_",
        target_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        resolved_target_table = target_table or f"{staging_prefix}{table}"
        engine = create_engine(self.db_dsn, pool_pre_ping=True)
        conn = None
        try:
            conn = engine.raw_connection()
            columns = self._get_source_columns(conn, database, resolved_target_table)
            primary_keys = self._get_source_primary_keys(conn, database, resolved_target_table)
        finally:
            if conn is not None:
                conn.close()
        return {
            "table": resolved_target_table,
            "source_table": table,
            "columns": columns,
            "primary_keys": primary_keys,
        }

    def list_source_tables(self, source_dsn: str, source_database: str) -> List[str]:
        engine = create_engine(source_dsn, pool_pre_ping=True)
        conn = None
        try:
            conn = engine.raw_connection()
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                    """,
                    (source_database,),
                )
                rows = cur.fetchall()
            finally:
                try:
                    cur.close()
                except Exception:
                    pass
        finally:
            if conn is not None:
                conn.close()
        return [row[0] for row in rows]

    def _record_load(
        self,
        conn,
        target_db: str,
        source_type: str,
        source_host: str,
        source_port: int,
        source_database: str,
        source_table: str,
        target_table: str,
        target_view: str,
        load_mode: str,
        row_count: int,
    ) -> None:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                INSERT INTO {self._escape_identifier(target_db)}.ingestion_loads
                    (source_type, source_host, source_port, source_database, source_table, target_table, target_view, load_mode, row_count, loaded_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, DATE_FORMAT(NOW(), '%%Y-%%m-%%d %%H:%%i:%%s'))
                """,
                (
                    source_type,
                    source_host,
                    str(source_port),
                    source_database,
                    source_table,
                    target_table,
                    target_view,
                    load_mode,
                    row_count,
                ),
            )
            conn.commit()
        finally:
            try:
                cur.close()
            except Exception:
                pass

    def list_loaded_sources(
        self,
        target_database: Optional[str] = None,
        staging_prefix: str = "stg_",
    ) -> List[Dict[str, Any]]:
        target_db = target_database or self.dw_database
        engine = create_engine(self.db_dsn, pool_pre_ping=True)
        conn = None
        results: List[Dict[str, Any]] = []
        try:
            conn = engine.raw_connection()
            self._ensure_metadata_tables(conn, target_db)

            cur = conn.cursor()
            try:
                cur.execute(
                    f"""
                    SELECT source_type, source_host, source_port, source_database, source_table, target_table, target_view, load_mode, row_count, loaded_at
                    FROM {self._escape_identifier(target_db)}.ingestion_loads
                    ORDER BY id DESC
                    """
                )
                rows = cur.fetchall()
            finally:
                try:
                    cur.close()
                except Exception:
                    pass

            seen = set()
            for row in rows:
                key = (row[1], row[2], row[3], row[5])
                if key in seen:
                    continue
                seen.add(key)
                results.append(
                    {
                        "source_type": row[0] or "mysql",
                        "host": row[1] or "Unknown source",
                        "port": row[2] or "",
                        "database": row[3] or target_db,
                        "source_table": row[4],
                        "target_table": row[5],
                        "target_view": row[6],
                        "mode": row[7],
                        "row_count": int(row[8] or 0),
                        "loaded_at": row[9],
                        "inferred": False,
                    }
                )

            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s AND table_type = 'BASE TABLE' AND table_name LIKE %s
                    ORDER BY table_name
                    """,
                    (target_db, f"{staging_prefix}%"),
                )
                staged_rows = cur.fetchall()
            finally:
                try:
                    cur.close()
                except Exception:
                    pass

            existing_targets = {item["target_table"] for item in results}
            for row in staged_rows:
                table_name = row[0]
                if table_name in existing_targets:
                    continue
                results.append(
                    {
                        "source_type": "mysql",
                        "host": "Warehouse inventory",
                        "port": "",
                        "database": target_db,
                        "source_table": table_name[len(staging_prefix):] if table_name.startswith(staging_prefix) else table_name,
                        "target_table": table_name,
                        "target_view": self._view_name_for_target_table(table_name, staging_prefix=staging_prefix, view_prefix="dw_"),
                        "mode": "unknown",
                        "row_count": self._count_rows(conn, target_db, table_name),
                        "loaded_at": None,
                        "inferred": True,
                    }
                )
        finally:
            if conn is not None:
                conn.close()
        return results

    def refresh_loaded_sources(
        self,
        source_host: str,
        source_port: int,
        source_database: Optional[str] = None,
        source_table: Optional[str] = None,
        source_user: Optional[str] = None,
        source_password: Optional[str] = None,
        target_database: Optional[str] = None,
        staging_prefix: str = "stg_",
        batch_size: int = 1000,
    ) -> Dict[str, Any]:
        target_db = target_database or self.dw_database
        engine = create_engine(self.db_dsn, pool_pre_ping=True)
        conn = None
        try:
            conn = engine.raw_connection()
            self._ensure_metadata_tables(conn, target_db)

            cur = conn.cursor()
            try:
                params: List[Any] = [source_host, str(source_port)]
                sql = f"""
                    SELECT DISTINCT source_type, source_database
                    FROM {self._escape_identifier(target_db)}.ingestion_loads
                    WHERE source_host = %s AND source_port = %s
                """
                if source_database:
                    sql += " AND source_database = %s"
                    params.append(source_database)
                sql += " ORDER BY source_database"
                cur.execute(sql, tuple(params))
                source_records = [
                    (str(row[0] or "mysql"), str(row[1]))
                    for row in cur.fetchall()
                    if row[1]
                ]
            finally:
                try:
                    cur.close()
                except Exception:
                    pass

            if not source_records:
                raise RefreshCredentialsRequired("No tracked source loads found for this host.")

            all_results = []
            for source_type, database_name in source_records:
                cur = conn.cursor()
                try:
                    params = [source_host, str(source_port), database_name]
                    sql = f"""
                        SELECT source_table, target_table
                        FROM {self._escape_identifier(target_db)}.ingestion_loads
                        WHERE source_host = %s AND source_port = %s AND source_database = %s
                    """
                    if source_table:
                        sql += " AND source_table = %s"
                        params.append(source_table)
                    sql += " ORDER BY id DESC"
                    cur.execute(sql, tuple(params))
                    table_rows = cur.fetchall()
                finally:
                    try:
                        cur.close()
                    except Exception:
                        pass

                tables = []
                seen_tables = set()
                for row in table_rows:
                    table_name = row[0]
                    target_name = row[1]
                    dedupe_key = (table_name, target_name)
                    if table_name and dedupe_key not in seen_tables:
                        seen_tables.add(dedupe_key)
                        tables.append(table_name)

                if not tables:
                    continue

                try:
                    if source_type == "table_api":
                        result = self.ingest_from_table_api(
                            base_url=database_name,
                            tables=tables,
                            api_token=source_password or None,
                            target_database=target_db,
                            staging_prefix=staging_prefix,
                            batch_size=batch_size,
                        )
                    else:
                        creds = {"user": source_user or "", "password": source_password or ""}
                        if not creds["user"] or not creds["password"]:
                            raise RefreshCredentialsRequired(
                                f"Re-enter credentials for {source_host}:{source_port} / {database_name}."
                            )
                        source_dsn = self._build_mysql_dsn(
                            host=source_host,
                            port=source_port,
                            user=creds["user"],
                            password=creds["password"],
                            database=database_name,
                        )
                        result = self.ingest_from_source(
                            source_dsn=source_dsn,
                            source_database=database_name,
                            source_host=source_host,
                            source_port=source_port,
                            tables=tables,
                            target_database=target_db,
                            staging_prefix=staging_prefix,
                            batch_size=batch_size,
                        )
                    all_results.append(result)
                except Exception as exc:
                    raise RefreshCredentialsRequired(str(exc)) from exc

            if not all_results:
                raise RefreshCredentialsRequired("No tracked tables found for refresh.")

            merged_tables = []
            for result in all_results:
                merged_tables.extend(result.get("tables", []))
            return {"target_database": target_db, "tables": merged_tables}
        finally:
            if conn is not None:
                conn.close()

    def delete_loaded_table(
        self,
        source_host: str,
        source_port: int,
        source_database: str,
        source_table: str,
        target_database: Optional[str] = None,
        staging_prefix: str = "stg_",
    ) -> Dict[str, Any]:
        target_db = target_database or self.dw_database
        engine = create_engine(self.db_dsn, pool_pre_ping=True)
        conn = None
        try:
            conn = engine.raw_connection()
            self._ensure_metadata_tables(conn, target_db)
            cur = conn.cursor()
            try:
                cur.execute(
                    f"""
                    SELECT target_table, target_view
                    FROM {self._escape_identifier(target_db)}.ingestion_loads
                    WHERE source_host = %s AND source_port = %s AND source_database = %s AND source_table = %s
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (source_host, str(source_port), source_database, source_table),
                )
                row = cur.fetchone()
                target_table = row[0] if row and row[0] else self._build_target_table_name(
                    staging_prefix=staging_prefix,
                    source_table=source_table,
                    source_type="mysql",
                    source_host=source_host,
                    source_port=source_port,
                    source_database=source_database,
                )
                target_view = row[1] if row and row[1] else self._view_name_for_target_table(
                    target_table, staging_prefix=staging_prefix, view_prefix="dw_"
                )
                cur.execute(f"DROP VIEW IF EXISTS {self._escape_identifier(target_db)}.{self._escape_identifier(target_view)}")
                cur.execute(f"DROP TABLE IF EXISTS {self._escape_identifier(target_db)}.{self._escape_identifier(target_table)}")
                cur.execute(
                    f"""
                    DELETE FROM {self._escape_identifier(target_db)}.ingestion_loads
                    WHERE source_host = %s AND source_port = %s AND source_database = %s AND source_table = %s
                    """,
                    (source_host, str(source_port), source_database, source_table),
                )
                conn.commit()
            finally:
                try:
                    cur.close()
                except Exception:
                    pass
        finally:
            if conn is not None:
                conn.close()
        return {
            "target_database": target_db,
            "source_table": source_table,
            "target_table": target_table,
        }

    def delete_loaded_group(
        self,
        source_host: str,
        source_port: int,
        source_database: Optional[str] = None,
        target_database: Optional[str] = None,
        staging_prefix: str = "stg_",
    ) -> Dict[str, Any]:
        target_db = target_database or self.dw_database
        engine = create_engine(self.db_dsn, pool_pre_ping=True)
        conn = None
        deleted_tables: List[str] = []
        try:
            conn = engine.raw_connection()
            self._ensure_metadata_tables(conn, target_db)

            cur = conn.cursor()
            try:
                params: List[Any] = [source_host, str(source_port)]
                sql = f"""
                    SELECT DISTINCT source_table, target_table, target_view
                    FROM {self._escape_identifier(target_db)}.ingestion_loads
                    WHERE source_host = %s AND source_port = %s
                """
                if source_database:
                    sql += " AND source_database = %s"
                    params.append(source_database)
                sql += " ORDER BY source_table"
                cur.execute(sql, tuple(params))
                load_rows = [row for row in cur.fetchall() if row[0]]
                deleted_tables = [row[0] for row in load_rows]
            finally:
                try:
                    cur.close()
                except Exception:
                    pass

            cur = conn.cursor()
            try:
                for source_table, target_table, target_view in load_rows:
                    cur.execute(
                        f"DROP VIEW IF EXISTS {self._escape_identifier(target_db)}.{self._escape_identifier(target_view or self._view_name_for_target_table(target_table, staging_prefix=staging_prefix, view_prefix='dw_'))}"
                    )
                    cur.execute(
                        f"DROP TABLE IF EXISTS {self._escape_identifier(target_db)}.{self._escape_identifier(target_table or self._build_target_table_name(staging_prefix, source_table, 'mysql', source_host, source_port, source_database or ''))}"
                    )

                delete_params: List[Any] = [source_host, str(source_port)]
                delete_sql = f"""
                    DELETE FROM {self._escape_identifier(target_db)}.ingestion_loads
                    WHERE source_host = %s AND source_port = %s
                """
                if source_database:
                    delete_sql += " AND source_database = %s"
                    delete_params.append(source_database)
                cur.execute(delete_sql, tuple(delete_params))
                conn.commit()
            finally:
                try:
                    cur.close()
                except Exception:
                    pass
        finally:
            if conn is not None:
                conn.close()

        return {
            "target_database": target_db,
            "deleted_tables": deleted_tables,
            "source_host": source_host,
            "source_port": str(source_port),
            "source_database": source_database,
        }

    def list_source_databases(self, source_dsn: str) -> List[str]:
        engine = create_engine(source_dsn, pool_pre_ping=True)
        conn = None
        try:
            conn = engine.raw_connection()
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT schema_name
                    FROM information_schema.schemata
                    ORDER BY schema_name
                    """
                )
                rows = cur.fetchall()
            finally:
                try:
                    cur.close()
                except Exception:
                    pass
        finally:
            if conn is not None:
                conn.close()
        return [row[0] for row in rows]

    def _coerce_api_value(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float, str)):
            return value
        return json.dumps(value)

    @staticmethod
    def _table_api_ssl_context():
        if os.getenv("TABLE_API_VERIFY_SSL", "false").lower() in {"1", "true", "yes"}:
            return None
        return ssl._create_unverified_context()

    def _infer_api_columns(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not rows:
            return []
        ordered_names: List[str] = []
        for row in rows:
            for key in row.keys():
                if key not in ordered_names:
                    ordered_names.append(key)

        def column_type(values: List[Any]) -> str:
            non_null = [value for value in values if value is not None]
            if not non_null:
                return "LONGTEXT"
            if all(isinstance(value, bool) for value in non_null):
                return "TINYINT(1)"
            if all(isinstance(value, int) and not isinstance(value, bool) for value in non_null):
                return "BIGINT"
            if all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in non_null):
                return "DOUBLE"
            return "LONGTEXT"

        columns: List[Dict[str, Any]] = []
        for name in ordered_names:
            values = [self._coerce_api_value(row.get(name)) for row in rows]
            columns.append(
                {
                    "name": name,
                    "type": column_type(values),
                    "nullable": True,
                    "default": None,
                    "extra": "",
                }
            )
        return columns

    def _fetch_table_api_rows(self, base_url: str, table: str, api_token: Optional[str] = None) -> List[Dict[str, Any]]:
        url = f"{base_url.rstrip('/')}/table/{quote(table)}"
        headers = {"Accept": "application/json"}
        if api_token:
            headers["Authorization"] = f"Bearer {api_token}"
        request = Request(url, headers=headers, method="GET")
        try:
            with urlopen(request, context=self._table_api_ssl_context()) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            raise ValueError(f"Table API request failed for {table}: HTTP {exc.code} {body}".strip())
        except URLError as exc:
            raise ValueError(f"Could not reach the table API source: {exc.reason}") from exc

        if isinstance(payload, list):
            items = payload
        elif isinstance(payload, dict):
            items = None
            for key in ("rows", "data", "items", "records"):
                candidate = payload.get(key)
                if isinstance(candidate, list):
                    items = candidate
                    break
            if items is None:
                raise ValueError(f"Table API response for {table} did not contain a row list.")
        else:
            raise ValueError(f"Table API response for {table} was not valid JSON rows.")

        rows: List[Dict[str, Any]] = []
        for item in items:
            if isinstance(item, dict):
                rows.append({str(key): self._coerce_api_value(value) for key, value in item.items()})
        return rows

    def list_table_api_tables(self, base_url: str, api_token: Optional[str] = None) -> List[str]:
        url = f"{base_url.rstrip('/')}/tables"
        headers = {"Accept": "application/json"}
        if api_token:
            headers["Authorization"] = f"Bearer {api_token}"
        request = Request(url, headers=headers, method="GET")
        try:
            with urlopen(request, context=self._table_api_ssl_context()) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            raise ValueError(f"Table API table discovery failed: HTTP {exc.code} {body}".strip())
        except URLError as exc:
            raise ValueError(f"Could not reach the table API source: {exc.reason}") from exc

        if isinstance(payload, list):
            if all(isinstance(item, str) for item in payload):
                return [str(item) for item in payload]
            names: List[str] = []
            for item in payload:
                if isinstance(item, dict):
                    value = item.get("name") or item.get("table") or item.get("id")
                    if value:
                        names.append(str(value))
            if names:
                return names
        elif isinstance(payload, dict):
            for key in ("tables", "items", "data"):
                candidate = payload.get(key)
                if isinstance(candidate, list):
                    output: List[str] = []
                    for item in candidate:
                        if isinstance(item, dict):
                            value = item.get("name") or item.get("table") or item.get("id")
                            if value:
                                output.append(str(value))
                        else:
                            output.append(str(item))
                    if output:
                        return output
        raise ValueError("Table API /tables response did not contain a valid table list.")

    def _replace_table_rows(
        self,
        conn,
        target_db: str,
        target_table: str,
        columns: List[Dict[str, Any]],
        rows: List[Dict[str, Any]],
        batch_size: int,
    ) -> int:
        if not columns:
            return 0
        column_names = [column["name"] for column in columns]
        placeholders = ", ".join(["%s"] * len(column_names))
        insert_sql = (
            f"INSERT INTO {self._escape_identifier(target_db)}.{self._escape_identifier(target_table)} "
            f"({', '.join(self._escape_identifier(name) for name in column_names)}) "
            f"VALUES ({placeholders})"
        )
        cur = conn.cursor()
        try:
            inserted = 0
            for start in range(0, len(rows), batch_size):
                chunk = rows[start:start + batch_size]
                values = [tuple(row.get(name) for name in column_names) for row in chunk]
                cur.executemany(insert_sql, values)
                inserted += cur.rowcount
            conn.commit()
            return inserted
        finally:
            try:
                cur.close()
            except Exception:
                pass

    def ingest_from_table_api(
        self,
        base_url: str,
        tables: List[str],
        api_token: Optional[str] = None,
        target_database: Optional[str] = None,
        staging_prefix: str = "stg_",
        batch_size: int = 1000,
    ) -> Dict[str, Any]:
        if not tables:
            raise ValueError("At least one table is required to ingest.")
        target_db = target_database or self.dw_database
        tgt_engine = create_engine(self.db_dsn, pool_pre_ping=True)
        tgt_conn = None
        parsed = urlparse(base_url)
        source_host = parsed.hostname or base_url.rstrip("/")
        default_port = 443 if parsed.scheme == "https" else 80
        source_port = parsed.port or default_port
        source_database = base_url.rstrip("/")
        results: Dict[str, Any] = {"target_database": target_db, "tables": []}
        try:
            tgt_conn = tgt_engine.raw_connection()
            tgt_cur = tgt_conn.cursor()
            try:
                tgt_cur.execute(f"CREATE DATABASE IF NOT EXISTS {self._escape_identifier(target_db)}")
                tgt_conn.commit()
            finally:
                try:
                    tgt_cur.close()
                except Exception:
                    pass
            self._ensure_metadata_tables(tgt_conn, target_db)

            for table in tables:
                rows = self._fetch_table_api_rows(base_url=base_url, table=table, api_token=api_token)
                columns = self._infer_api_columns(rows)
                if not columns:
                    raise ValueError(f"Table API source returned no usable columns for {table}.")
                target_table = self._build_target_table_name(
                    staging_prefix=staging_prefix,
                    source_table=table,
                    source_type="table_api",
                    source_host=source_host,
                    source_port=source_port,
                    source_database=source_database,
                )
                target_view = self._view_name_for_target_table(target_table, staging_prefix=staging_prefix, view_prefix="dw_")
                cur = tgt_conn.cursor()
                try:
                    cur.execute(f"DROP TABLE IF EXISTS {self._escape_identifier(target_db)}.{self._escape_identifier(target_table)}")
                    cur.execute(f"DROP VIEW IF EXISTS {self._escape_identifier(target_db)}.{self._escape_identifier(target_view)}")
                    tgt_conn.commit()
                finally:
                    try:
                        cur.close()
                    except Exception:
                        pass
                self._ensure_staging_table(tgt_conn, target_db, target_table, columns, [])
                inserted = self._replace_table_rows(
                    tgt_conn,
                    target_db,
                    target_table,
                    columns,
                    rows,
                    batch_size,
                )
                row_count = self._count_rows(tgt_conn, target_db, target_table)
                self._record_load(
                    tgt_conn,
                    target_db,
                    source_type="table_api",
                    source_host=source_host,
                    source_port=source_port,
                    source_database=source_database,
                    source_table=table,
                    target_table=target_table,
                    target_view=target_view,
                    load_mode="replace",
                    row_count=row_count,
                )
                results["tables"].append(
                    {
                        "source_table": table,
                        "target_table": target_table,
                        "target_view": target_view,
                        "rows_copied": inserted,
                        "row_count": row_count,
                    }
                )
        finally:
            if tgt_conn is not None:
                tgt_conn.close()
        return results

    def list_views_with_columns(
        self,
        database: Optional[str] = None,
        view_prefix: str = "dw_",
    ) -> List[Dict[str, Any]]:
        target_db = database or self.dw_database
        engine = create_engine(self.db_dsn, pool_pre_ping=True)
        conn = None
        results: List[Dict[str, Any]] = []
        try:
            conn = engine.raw_connection()
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s AND table_type = 'VIEW' AND table_name LIKE %s
                    ORDER BY table_name
                    """,
                    (target_db, f"{view_prefix}%"),
                )
                view_rows = cur.fetchall()
            finally:
                try:
                    cur.close()
                except Exception:
                    pass

            for row in view_rows:
                view_name = row[0]
                columns = self._get_columns_with_types(conn, target_db, view_name)
                results.append({"view": view_name, "columns": columns})
        finally:
            if conn is not None:
                conn.close()
        return results

    def _ensure_staging_table(
        self,
        conn,
        target_db: str,
        target_table: str,
        columns: List[Dict[str, Any]],
        primary_keys: List[str],
    ) -> None:
        if not columns:
            raise ValueError(f"Source table {target_table} has no columns.")
        column_defs = []
        for col in columns:
            name = self._escape_identifier(col["name"])
            col_type = col["type"]
            nullable = col["nullable"]
            default = self._format_default(col["default"])
            extra = col["extra"] or ""
            parts = [name, col_type]
            if not nullable:
                parts.append("NOT NULL")
            if default is not None:
                parts.append(f"DEFAULT {default}")
            if extra:
                parts.append(extra)
            column_defs.append(" ".join(parts))
        if primary_keys:
            pk_cols = ", ".join(self._escape_identifier(c) for c in primary_keys)
            column_defs.append(f"PRIMARY KEY ({pk_cols})")
        ddl = f"CREATE TABLE IF NOT EXISTS {self._escape_identifier(target_db)}.{self._escape_identifier(target_table)} (\n  "
        ddl += ",\n  ".join(column_defs)
        ddl += "\n)"
        cur = conn.cursor()
        try:
            cur.execute(ddl)
        finally:
            try:
                cur.close()
            except Exception:
                pass

    def _copy_table_data(
        self,
        src_conn,
        tgt_conn,
        source_db: str,
        source_table: str,
        target_db: str,
        target_table: str,
        columns: List[Dict[str, Any]],
        primary_keys: List[str],
        mode: str,
        where_clause: Optional[str],
        where_params: Optional[tuple],
        batch_size: int,
    ) -> int:
        src_cur = src_conn.cursor()
        tgt_cur = tgt_conn.cursor()
        inserted = 0
        column_names = [col["name"] for col in columns]
        select_cols = ", ".join(self._escape_identifier(c) for c in column_names)
        placeholders = ", ".join(["%s"] * len(column_names))
        update_clause = ""
        if mode == "append" and primary_keys:
            non_pk_cols = [c for c in column_names if c not in set(primary_keys)]
            if non_pk_cols:
                assignments = ", ".join(
                    f"{self._escape_identifier(c)} = VALUES({self._escape_identifier(c)})" for c in non_pk_cols
                )
                update_clause = f" ON DUPLICATE KEY UPDATE {assignments}"
        insert_sql = (
            f"INSERT INTO {self._escape_identifier(target_db)}.{self._escape_identifier(target_table)} "
            f"({', '.join(self._escape_identifier(c) for c in column_names)}) "
            f"VALUES ({placeholders}){update_clause}"
        )
        try:
            select_sql = (
                f"SELECT {select_cols} FROM {self._escape_identifier(source_db)}.{self._escape_identifier(source_table)}"
            )
            if where_clause:
                select_sql = f"{select_sql} {where_clause}"
            src_cur.execute(select_sql, where_params or ())
            while True:
                rows = src_cur.fetchmany(batch_size)
                if not rows:
                    break
                tgt_cur.executemany(insert_sql, rows)
                inserted += tgt_cur.rowcount
                tgt_conn.commit()
        finally:
            try:
                src_cur.close()
            except Exception:
                pass
            try:
                tgt_cur.close()
            except Exception:
                pass
        return inserted

    def ingest_from_source(
        self,
        source_dsn: str,
        source_database: str,
        source_host: str,
        source_port: int,
        tables: List[str],
        target_database: Optional[str] = None,
        staging_prefix: str = "stg_",
        batch_size: int = 1000,
    ) -> Dict[str, Any]:
        """Copy source tables into DW staging tables."""
        if not tables:
            raise ValueError("At least one table is required to ingest.")
        target_db = target_database or self.dw_database
        src_engine = create_engine(source_dsn, pool_pre_ping=True)
        tgt_engine = create_engine(self.db_dsn, pool_pre_ping=True)

        results: Dict[str, Any] = {"target_database": target_db, "tables": []}
        src_conn = None
        tgt_conn = None
        try:
            src_conn = src_engine.raw_connection()
            tgt_conn = tgt_engine.raw_connection()

            tgt_cur = tgt_conn.cursor()
            try:
                tgt_cur.execute(f"CREATE DATABASE IF NOT EXISTS {self._escape_identifier(target_db)}")
                tgt_conn.commit()
            finally:
                try:
                    tgt_cur.close()
                except Exception:
                    pass
            self._ensure_metadata_tables(tgt_conn, target_db)

            for table in tables:
                columns = self._get_source_columns(src_conn, source_database, table)
                primary_keys = self._get_source_primary_keys(src_conn, source_database, table)
                target_table = self._build_target_table_name(
                    staging_prefix=staging_prefix,
                    source_table=table,
                    source_type="mysql",
                    source_host=source_host,
                    source_port=source_port,
                    source_database=source_database,
                )
                target_view = self._view_name_for_target_table(target_table, staging_prefix=staging_prefix, view_prefix="dw_")
                self._ensure_staging_table(tgt_conn, target_db, target_table, columns, primary_keys)
                where_clause = None
                where_params = None

                cur = tgt_conn.cursor()
                try:
                    cur.execute(f"DROP VIEW IF EXISTS {self._escape_identifier(target_db)}.{self._escape_identifier(target_view)}")
                    cur.execute(
                        f"TRUNCATE TABLE {self._escape_identifier(target_db)}.{self._escape_identifier(target_table)}"
                    )
                    tgt_conn.commit()
                finally:
                    try:
                        cur.close()
                    except Exception:
                        pass
                inserted = self._copy_table_data(
                    src_conn,
                    tgt_conn,
                    source_database,
                    table,
                    target_db,
                    target_table,
                    columns,
                    primary_keys,
                    "replace",
                    where_clause,
                    where_params,
                    batch_size,
                )

                row_count = self._count_rows(tgt_conn, target_db, target_table)
                pk_nulls = self._count_nulls(tgt_conn, target_db, target_table, primary_keys)
                table_result = {"source_table": table, "target_table": target_table, "target_view": target_view, "rows_copied": inserted}
                table_result["row_count"] = row_count
                if pk_nulls:
                    table_result["pk_nulls"] = pk_nulls
                self._record_load(
                    tgt_conn,
                    target_db,
                    source_type="mysql",
                    source_host=source_host,
                    source_port=source_port,
                    source_database=source_database,
                    source_table=table,
                    target_table=target_table,
                    target_view=target_view,
                    load_mode="replace",
                    row_count=row_count,
                )
                results["tables"].append(table_result)
        finally:
            if src_conn is not None:
                src_conn.close()
            if tgt_conn is not None:
                tgt_conn.close()
        return results


    def apply_view_statements(
        self,
        statements: List[str],
        target_database: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not statements:
            raise ValueError("No view statements provided.")
        target_db = target_database or self.dw_database
        tgt_engine = create_engine(self.db_dsn, pool_pre_ping=True)
        conn = None
        results: Dict[str, Any] = {"target_database": target_db, "applied": []}
        try:
            conn = tgt_engine.raw_connection()
            cur = conn.cursor()
            try:
                cur.execute(f"USE {self._escape_identifier(target_db)}")
            finally:
                try:
                    cur.close()
                except Exception:
                    pass
            for sql in statements:
                cur = conn.cursor()
                try:
                    cur.execute(sql)
                    conn.commit()
                finally:
                    try:
                        cur.close()
                    except Exception:
                        pass
                results["applied"].append(sql)
        finally:
            if conn is not None:
                conn.close()
        return results

    def create_baseline_views(
        self,
        tables: List[Dict[str, str]],
        target_database: Optional[str] = None,
        staging_prefix: str = "stg_",
        view_prefix: str = "dw_",
    ) -> List[str]:
        if not tables:
            raise ValueError("At least one table is required to transform.")
        target_db = target_database or self.dw_database
        tgt_engine = create_engine(self.db_dsn, pool_pre_ping=True)
        conn = None
        created = []
        try:
            conn = tgt_engine.raw_connection()
            for table in tables:
                source_table = table["target_table"]
                columns = self._get_columns_with_types(conn, target_db, source_table)
                if not columns:
                    raise ValueError(f"Landing table not found or empty schema: {source_table}")

                select_parts = [self._escape_identifier(c["name"]) for c in columns]

                view_name = table.get("target_view") or self._view_name_for_target_table(
                    source_table,
                    staging_prefix=staging_prefix,
                    view_prefix=view_prefix,
                )
                sql = (
                    f"CREATE OR REPLACE VIEW {self._escape_identifier(target_db)}.{self._escape_identifier(view_name)} AS \n"
                    f"SELECT\n  " + ",\n  ".join(select_parts) + "\n"
                    f"FROM {self._escape_identifier(target_db)}.{self._escape_identifier(source_table)}"
                )
                cur = conn.cursor()
                try:
                    cur.execute(sql)
                    conn.commit()
                finally:
                    try:
                        cur.close()
                    except Exception:
                        pass
                created.append(view_name)
        finally:
            if conn is not None:
                conn.close()
        return created
