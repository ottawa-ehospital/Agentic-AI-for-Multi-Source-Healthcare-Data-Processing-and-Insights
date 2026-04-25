from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_ask_endpoint_returns_router_shape(monkeypatch):
    async def fake_ask_agent(question, history=None, locale=None):
        return {
            "answer": f"stubbed: {question}",
            "chart": None,
            "rows": [],
            "sql_used": "",
            "sources": [],
            "quality_flags": [],
            "insight": None,
        }

    monkeypatch.setattr("app.routers.api.ask_agent", fake_ask_agent)

    response = client.post(
        "/api/ask",
        json={"question": "show me revenue", "history": [], "locale": "en"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["answer"] == "stubbed: show me revenue"
    assert data["chart"] is None
    assert data["rows"] == []
    assert data["sql_used"] == ""
    assert data["sources"] == []
    assert data["quality_flags"] == []
    assert data["insight"] is None


def test_pipeline_ingest_endpoint_exists():
    response = client.post(
        "/api/pipeline/ingest",
        json={
            "source": {
                "source_type": "mysql",
                "host": "localhost",
                "port": 3306,
                "user": "",
                "password": "",
                "database": "",
            },
            "tables": ["example"],
        },
    )
    assert response.status_code in {400, 500, 502}
