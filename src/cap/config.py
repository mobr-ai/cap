from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    # Virtuoso settings
    VIRTUOSO_HOST: str
    VIRTUOSO_PORT: int
    VIRTUOSO_USER: str
    VIRTUOSO_PASSWORD: str
    CARDANO_GRAPH: str

    # PostgreSQL settings for cardano-db-sync
    POSTGRES_HOST: str
    POSTGRES_PORT: int
    POSTGRES_DB: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str

    # ETL settings
    ETL_BATCH_SIZE: int
    ETL_SYNC_INTERVAL: int
    ETL_AUTO_START: bool
    ETL_CONTINUOUS: bool
    ETL_PROGRESS_GRAPH: str
    ETL_PARALLEL_WORKERS: int

    # Monitoring settings
    ENABLE_TRACING: bool
    LOG_LEVEL: str
    ETL_METRICS_ENABLED: bool

    # CAP settings
    CAP_HOST: str
    CAP_PORT: int

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=True
    )

settings = Settings()