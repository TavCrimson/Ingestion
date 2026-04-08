from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "sqlite:///./data/repository.db"
    chroma_path: str = "./data/chroma"
    raw_store_path: str = "./data/raw"
    models_path: str = "./data/models"
    embedding_model: str = "all-MiniLM-L6-v2"

    escalation_timeout_hours: int = 48
    escalation_check_interval_minutes: int = 15

    default_rate_limit_per_minute: int = 60

    log_level: str = "INFO"

    # Anthropic (for chat agent)
    anthropic_api_key: str = ""
    chat_model: str = "claude-haiku-4-5-20251001"
    chat_context_chunks: int = 8

    @property
    def raw_store_dir(self) -> Path:
        return Path(self.raw_store_path)

    @property
    def chroma_dir(self) -> Path:
        return Path(self.chroma_path)

    @property
    def models_dir(self) -> Path:
        return Path(self.models_path)


settings = Settings()
