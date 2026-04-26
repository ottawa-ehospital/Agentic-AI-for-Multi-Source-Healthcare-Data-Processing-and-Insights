from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()

from app.routers.api import router as api_router
from app.routers.pipeline import router as pipeline_router

# initialize logging
import app.logging_config  # noqa: F401

app = FastAPI(
    title="Analytics Workspace Backend",
    version="1.0.0",
)

# Open CORS for development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(api_router)
app.include_router(pipeline_router)
