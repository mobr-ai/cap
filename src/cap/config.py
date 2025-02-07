from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    VIRTUOSO_HOST: str
    VIRTUOSO_PORT: int
    VIRTUOSO_USER: str
    VIRTUOSO_PASSWORD: str
    CARDANO_GRAPH: str

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=True
    )

settings = Settings()