import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    OPENAI_API_KEY: str | None = os.getenv("OPENAI_API_KEY")
    PROJECT_NAME: str = os.getenv("PROJECT_NAME", "FastAPI Starter")

settings = Settings()
