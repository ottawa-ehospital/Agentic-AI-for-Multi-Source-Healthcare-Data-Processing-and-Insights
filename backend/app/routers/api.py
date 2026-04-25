from fastapi import APIRouter, HTTPException
from typing import List, Any, Optional, Dict
from pydantic import BaseModel

from app.services.agent import ask_agent

router = APIRouter(
    prefix="/api",
    tags=["API"],
)

class AskRequest(BaseModel):
    question: str
    history: Optional[List[Dict[str, str]]] = None
    locale: Optional[str] = None

class ChartData(BaseModel):
    type: str
    x: List[Any]
    y: List[Any]
    title: Optional[str] = None
    x_label: Optional[str] = None
    y_label: Optional[str] = None
    series_label: Optional[str] = None
    value_format: Optional[str] = None

class AskResponse(BaseModel):
    answer: str
    chart: Optional[ChartData]
    rows: Optional[List[Dict[str, Any]]]
    sql_used: str
    sources: List[str]
    quality_flags: Optional[List[str]]
    insight: Optional[str] = None

@router.post("/ask", response_model=AskResponse)
async def ask(request: AskRequest):
    """Route incoming question to the agent and return a structured response."""
    result = await ask_agent(request.question, history=request.history, locale=request.locale)
    if not isinstance(result, dict):
        raise HTTPException(status_code=500, detail="agent failed to return a dict")
    return result
