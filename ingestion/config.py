from pathlib import Path
from pydantic import field_validator, model_validator
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

    # Deduplication thresholds
    dedup_near_duplicate_threshold: float = 0.95
    dedup_similar_lower_bound: float = 0.80

    # Reciprocal Rank Fusion offset
    rrf_rank_offset: int = 60

    @field_validator("rrf_rank_offset")
    @classmethod
    def rrf_offset_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("rrf_rank_offset must be a positive integer")
        return v

    @field_validator("chat_context_chunks")
    @classmethod
    def chat_context_chunks_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("chat_context_chunks must be a positive integer")
        return v

    @model_validator(mode="after")
    def dedup_thresholds_ordered(self) -> "Settings":
        if self.dedup_similar_lower_bound >= self.dedup_near_duplicate_threshold:
            raise ValueError(
                "dedup_similar_lower_bound must be less than dedup_near_duplicate_threshold"
            )
        return self

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
