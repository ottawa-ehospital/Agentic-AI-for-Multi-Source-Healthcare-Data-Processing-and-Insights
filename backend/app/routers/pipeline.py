import os
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional

from app.services.pipeline_service import PipelineService, RefreshCredentialsRequired
from app.services.ai_transformer import propose_views, validate_view_sql

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])


def _pipeline_error_response(exc: Exception) -> HTTPException:
    message = str(exc)
    lowered = message.lower()

    if "cryptography' package is required" in lowered or '"cryptography" package is required' in lowered:
        return HTTPException(
            status_code=400,
            detail=(
                "Could not authenticate with the source database. "
                "This MySQL server uses an authentication method that requires additional backend support."
            ),
        )
    if "access denied for user" in lowered:
        return HTTPException(
            status_code=400,
            detail="Could not authenticate with the source database. Check the username and password and try again.",
        )
    if "unknown database" in lowered:
        return HTTPException(
            status_code=400,
            detail="The selected source database could not be found.",
        )
    if (
        "can't connect to mysql server" in lowered
        or "can't connect to mysql" in lowered
        or "could not connect" in lowered
        or "connection refused" in lowered
    ):
        return HTTPException(
            status_code=502,
            detail="Could not reach the source database host. Check the host, port, and network access and try again.",
        )
    return HTTPException(status_code=500, detail=message)


class SourceCredentials(BaseModel):
    source_type: str = "mysql"
    host: Optional[str] = None
    port: int = 3306
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    base_url: Optional[str] = None
    api_token: Optional[str] = None


class IngestRequest(BaseModel):
    source: SourceCredentials
    tables: List[str] = Field(..., min_length=1)
    target_database: Optional[str] = None
    staging_prefix: str = "stg_"
    batch_size: int = Field(1000, ge=1, le=10000)


class IngestResponse(BaseModel):
    target_database: str
    tables: List[dict]
    proposal: Optional[dict] = None
    applied: Optional[List[dict]] = None
    rejected: Optional[List[dict]] = None
    baseline_views: Optional[List[str]] = None
    baseline_error: Optional[str] = None


class ListTablesRequest(BaseModel):
    source: SourceCredentials


class ListTablesResponse(BaseModel):
    tables: List[str]


class ListDatabasesRequest(BaseModel):
    source: SourceCredentials


class ListDatabasesResponse(BaseModel):
    databases: List[str]


class LoadedSourceItem(BaseModel):
    source_type: str = "mysql"
    host: str
    port: str
    database: str
    source_table: str
    target_table: str
    mode: str
    row_count: int
    loaded_at: Optional[str] = None
    inferred: bool


class LoadedSourcesResponse(BaseModel):
    items: List[LoadedSourceItem]


class RefreshRequest(BaseModel):
    source: SourceCredentials
    table: Optional[str] = None
    target_database: Optional[str] = None
    staging_prefix: str = "stg_"
    batch_size: int = Field(1000, ge=1, le=10000)


class DeleteLoadedTableRequest(BaseModel):
    source: SourceCredentials
    table: str
    target_database: Optional[str] = None
    staging_prefix: str = "stg_"


class DeleteLoadedTableResponse(BaseModel):
    target_database: str
    source_table: str
    target_table: str


class DeleteLoadedGroupRequest(BaseModel):
    source: SourceCredentials
    target_database: Optional[str] = None
    staging_prefix: str = "stg_"


class DeleteLoadedGroupResponse(BaseModel):
    target_database: str
    deleted_tables: List[str]
    source_host: str
    source_port: str
    source_database: Optional[str] = None


async def _refresh_analysis_layer(pipeline: PipelineService, target_db: str, tables: List[dict], staging_prefix: str):
    expected_view_names = {table.get("target_view") for table in tables if table.get("target_view")}
    if not os.getenv("OPENAI_API_KEY"):
        try:
            baseline_views = pipeline.create_baseline_views(
                tables=tables,
                target_database=target_db,
                staging_prefix=staging_prefix,
                view_prefix="dw_",
            )
            return {
                "proposal": None,
                "applied": None,
                "rejected": None,
                "baseline_views": baseline_views,
                "baseline_error": None,
            }
        except Exception as exc:
            return {
                "proposal": None,
                "applied": None,
                "rejected": None,
                "baseline_views": None,
                "baseline_error": str(exc),
            }

    ai_results = await propose_views(
        tables=tables,
        target_database=target_db,
        staging_prefix=staging_prefix,
        view_prefix="dw_",
        goals="Create analysis-friendly views with clear naming. Avoid adding derived columns unless clearly useful.",
    )
    proposal = ai_results.get("proposal", {})
    views = proposal.get("views", []) if isinstance(proposal, dict) else []
    applied = []
    rejected = []
    statements = []
    for view in views:
        sql = view.get("sql") if isinstance(view, dict) else None
        view_name = view.get("name") if isinstance(view, dict) else None
        if not sql:
            rejected.append({"view": view, "reason": "missing sql"})
            continue
        if not view_name:
            rejected.append({"view": view, "reason": "missing view name"})
            continue
        if expected_view_names and view_name not in expected_view_names:
            rejected.append({"view": view, "reason": "view name did not match the source-scoped target view"})
            continue
        reason = validate_view_sql(
            sql=sql,
            target_database=target_db,
            staging_prefix=staging_prefix,
            view_prefix="dw_",
        )
        if reason:
            rejected.append({"view": view, "reason": reason})
            continue
        statements.append(sql)
        applied.append({"view": view.get("name"), "status": "applied"})

    baseline_views = None
    baseline_error = None
    if statements:
        pipeline.apply_view_statements(statements=statements, target_database=target_db)
    else:
        try:
            baseline_views = pipeline.create_baseline_views(
                tables=tables,
                target_database=target_db,
                staging_prefix=staging_prefix,
                view_prefix="dw_",
            )
        except Exception as exc:
            baseline_error = str(exc)

    return {
        "proposal": proposal,
        "applied": applied,
        "rejected": rejected,
        "baseline_views": baseline_views,
        "baseline_error": baseline_error,
    }


@router.post("/ingest", response_model=IngestResponse)
async def ingest(req: IngestRequest):
    """Ingest source tables and auto-apply AI-generated views."""
    try:
        pipeline = PipelineService()
        if req.source.source_type == "table_api":
            if not req.source.base_url:
                raise HTTPException(status_code=400, detail="A table API base URL is required.")
            results = pipeline.ingest_from_table_api(
                base_url=req.source.base_url,
                tables=req.tables,
                api_token=req.source.api_token,
                target_database=req.target_database,
                staging_prefix=req.staging_prefix,
                batch_size=req.batch_size,
            )
        else:
            source_dsn = pipeline._build_mysql_dsn(
                host=req.source.host or "",
                port=req.source.port,
                user=req.source.user or "",
                password=req.source.password or "",
                database=req.source.database or "",
            )
            results = pipeline.ingest_from_source(
                source_dsn=source_dsn,
                source_database=req.source.database or "",
                source_host=req.source.host or "",
                source_port=req.source.port,
                tables=req.tables,
                target_database=req.target_database,
                staging_prefix=req.staging_prefix,
                batch_size=req.batch_size,
            )
        target_db = req.target_database or pipeline.dw_database
        analysis_layer = await _refresh_analysis_layer(
            pipeline,
            target_db=target_db,
            tables=results["tables"],
            staging_prefix=req.staging_prefix,
        )
        return {
            "target_database": results["target_database"],
            "tables": results["tables"],
            "proposal": analysis_layer["proposal"],
            "applied": analysis_layer["applied"],
            "rejected": analysis_layer["rejected"],
            "baseline_views": analysis_layer["baseline_views"],
            "baseline_error": analysis_layer["baseline_error"],
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise _pipeline_error_response(exc)


@router.post("/refresh", response_model=IngestResponse)
async def refresh(req: RefreshRequest):
    """Refresh one tracked table or all tracked tables for a source."""
    try:
        pipeline = PipelineService()
        results = pipeline.refresh_loaded_sources(
            source_host=req.source.host or "",
            source_port=req.source.port,
            source_database=req.source.database or None,
            source_table=req.table,
            source_user=req.source.user or None,
            source_password=req.source.password or None,
            target_database=req.target_database,
            staging_prefix=req.staging_prefix,
            batch_size=req.batch_size,
        )
        analysis_layer = await _refresh_analysis_layer(
            pipeline,
            target_db=results["target_database"],
            tables=results["tables"],
            staging_prefix=req.staging_prefix,
        )
        return {
            "target_database": results["target_database"],
            "tables": results["tables"],
            "proposal": analysis_layer["proposal"],
            "applied": analysis_layer["applied"],
            "rejected": analysis_layer["rejected"],
            "baseline_views": analysis_layer["baseline_views"],
            "baseline_error": analysis_layer["baseline_error"],
        }
    except RefreshCredentialsRequired as exc:
        raise HTTPException(
            status_code=409,
            detail={"message": exc.message, "reconnect_required": True},
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise _pipeline_error_response(exc)


@router.get("/loaded_sources", response_model=LoadedSourcesResponse)
async def loaded_sources():
    """List warehouse-backed source loads and inferred staging tables."""
    try:
        pipeline = PipelineService()
        items = pipeline.list_loaded_sources()
        return {"items": items}
    except HTTPException:
        raise
    except Exception as exc:
        raise _pipeline_error_response(exc)


@router.post("/delete_loaded_table", response_model=DeleteLoadedTableResponse)
async def delete_loaded_table(req: DeleteLoadedTableRequest):
    """Delete one tracked loaded table from the warehouse and inventory."""
    try:
        pipeline = PipelineService()
        result = pipeline.delete_loaded_table(
            source_host=req.source.host,
            source_port=req.source.port,
            source_database=req.source.database,
            source_table=req.table,
            target_database=req.target_database,
            staging_prefix=req.staging_prefix,
        )
        return result
    except HTTPException:
        raise
    except Exception as exc:
        raise _pipeline_error_response(exc)


@router.post("/delete_loaded_group", response_model=DeleteLoadedGroupResponse)
async def delete_loaded_group(req: DeleteLoadedGroupRequest):
    try:
        pipeline = PipelineService()
        result = pipeline.delete_loaded_group(
            source_host=req.source.host,
            source_port=req.source.port,
            source_database=req.source.database or None,
            target_database=req.target_database,
            staging_prefix=req.staging_prefix,
        )
        return result
    except HTTPException:
        raise
    except Exception as exc:
        raise _pipeline_error_response(exc)


@router.post("/list_tables", response_model=ListTablesResponse)
async def list_tables(req: ListTablesRequest):
    """List available tables from the source database."""
    try:
        pipeline = PipelineService()
        if req.source.source_type == "table_api":
            if not req.source.base_url:
                raise HTTPException(status_code=400, detail="A table API base URL is required.")
            tables = pipeline.list_table_api_tables(
                base_url=req.source.base_url,
                api_token=req.source.api_token,
            )
            return {"tables": tables}
        source_dsn = pipeline._build_mysql_dsn(
            host=req.source.host or "",
            port=req.source.port,
            user=req.source.user or "",
            password=req.source.password or "",
            database=req.source.database or "",
        )
        tables = pipeline.list_source_tables(source_dsn=source_dsn, source_database=req.source.database or "")
        return {"tables": tables}
    except HTTPException:
        raise
    except Exception as exc:
        raise _pipeline_error_response(exc)


@router.post("/list_databases", response_model=ListDatabasesResponse)
async def list_databases(req: ListDatabasesRequest):
    """List available databases from the source server."""
    try:
        pipeline = PipelineService()
        if req.source.source_type == "table_api":
            raise HTTPException(
                status_code=400,
                detail="Table API sources do not support database discovery.",
            )
        source_dsn = pipeline._build_mysql_dsn(
            host=req.source.host or "",
            port=req.source.port,
            user=req.source.user or "",
            password=req.source.password or "",
            database=req.source.database or "",
        )
        databases = pipeline.list_source_databases(source_dsn=source_dsn)
        return {"databases": databases}
    except HTTPException:
        raise
    except Exception as exc:
        raise _pipeline_error_response(exc)

