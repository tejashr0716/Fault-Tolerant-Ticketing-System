from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration settings."""

    APP_NAME: str = "CineBox Ticketing System"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False

    # Redis Configurations
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: Optional[str] = None
    REDIS_URL: Optional[str] = None

    # Locking TTL in seconds (default: 300 seconds = 5 minutes)
    SEAT_LOCK_TTL_SECONDS: int = 300

    # RabbitMQ Configurations
    RABBITMQ_HOST: str = "localhost"
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USER: str = "guest"
    RABBITMQ_PASSWORD: str = "guest"
    RABBITMQ_VHOST: str = "/"
    RABBITMQ_URL: Optional[str] = None
    RABBITMQ_QUEUE: str = "ticket_payments"
    RABBITMQ_EXCHANGE: str = "ticket_events_exchange"
    RABBITMQ_ROUTING_KEY: str = "ticket_payments"
    RABBITMQ_DLX: str = "ticket_payments_dlx"
    RABBITMQ_DLQ: str = "ticket_payments_dlq"

    # PostgreSQL Configurations
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "ticketing_db"
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    DATABASE_URL: Optional[str] = None

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def redis_dsn(self) -> str:
        """Return the Redis connection URI string."""
        if self.REDIS_URL:
            return self.REDIS_URL
        if self.REDIS_PASSWORD:
            return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"

    @property
    def rabbitmq_dsn(self) -> str:
        """Return the RabbitMQ connection AMQP URI string."""
        if self.RABBITMQ_URL:
            return self.RABBITMQ_URL
        vhost = self.RABBITMQ_VHOST.lstrip("/")
        return f"amqp://{self.RABBITMQ_USER}:{self.RABBITMQ_PASSWORD}@{self.RABBITMQ_HOST}:{self.RABBITMQ_PORT}/{vhost}"

    @property
    def postgres_dsn(self) -> str:
        """Return the PostgreSQL connection URI string."""
        if self.DATABASE_URL:
            return self.DATABASE_URL
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"


settings = Settings()
