from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = (
        "postgresql+asyncpg://shop_user:shop_pass@localhost:5432/shop_db"
    )
    jwt_secret_key: str = "change-me-in-production-please"
    jwt_algorithm: str = "HS256"
    access_token_ttl_minutes: int = 15
    refresh_token_ttl_days: int = 30
    # Rate-limit: max CREATE_ORDER operations per window
    order_rate_limit: int = 3
    order_rate_window_seconds: int = 60

    class Config:
        env_file = ".env"


settings = Settings()
