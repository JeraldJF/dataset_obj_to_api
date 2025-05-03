from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    API_V1_STR: str = ""  # Removed /v2 prefix
    PROJECT_NAME: str = "Dataset API"
    BACKEND_URL: str = "http://localhost:3005"

    class Config:
        case_sensitive = True
        env_file = ".env"
        env_file_encoding = 'utf-8'

def get_settings() -> Settings:
    return Settings(_env_file='.env', _env_file_encoding='utf-8')