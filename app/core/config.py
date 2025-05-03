from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    API_V1_STR: str = ""  # Removed /v2 prefix
    PROJECT_NAME: str = "Dataset API"
    BACKEND_URL: str = "http://localhost:3000"

    class Config:
        case_sensitive = True
        env_file = ".env"

@lru_cache()
def get_settings() -> Settings:
    return Settings()