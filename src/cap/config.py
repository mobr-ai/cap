from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    VIRTUOSO_HOST: str
    VIRTUOSO_PORT: int
    VIRTUOSO_USER: str
    VIRTUOSO_PASSWORD: str
    CARDANO_GRAPH: str

    # PostgreSQL settings for cardano-db-sync
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "cap"
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "mysecretpassword"

    # ETL settings
    ETL_BATCH_SIZE: int = 1000
    ETL_SYNC_INTERVAL: int = 300  # seconds
    ETL_AUTO_START: bool = False
    ETL_CONTINUOUS: bool = True

    # Monitoring settings
    ETL_METRICS_ENABLED: bool = True
    ETL_LOG_LEVEL: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=True
    )

settings = Settings()