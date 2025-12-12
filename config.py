# config.py
from pathlib import Path

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Ищем .env в той же директории, где лежит этот файл
ENV_FILE_PATH = Path(__file__).resolve().parent / ".env"


class ProxyConfig(BaseSettings):
    """
    Proxy configuration.

    Reads proxy parameters from environment variables with the PROXY_ prefix.
    """
    model_config = SettingsConfigDict(
        env_prefix="PROXY_",
        env_file=ENV_FILE_PATH,
        env_file_encoding="utf-8",
        extra="ignore",  # Игнорировать лишние переменные окружения
    )

    # --- Core connection parameters ---
    validity_check_url: str = Field(...)
    proxies_file: str = Field(...)
    valid_proxies_file: str = Field(...)

    # --- Вычисляемые пути ---
    @property
    def proxies_file_path(self) -> Path:
        """Полный путь к файлу с новыми прокси."""
        return Path(__file__).resolve().parent / self.proxies_file

    @property
    def valid_proxies_file_path(self) -> Path:
        """Полный путь к файлу с валидными прокси."""
        return Path(__file__).resolve().parent / self.valid_proxies_file


class PostgresConfig(BaseSettings):
    """
    Database configuration for psycopg (without SQLAlchemy).
    """
    model_config = SettingsConfigDict(
        env_prefix="PG_",
        env_file=ENV_FILE_PATH,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Core connection parameters ---
    host: str = Field(...)
    port: int = Field(...)
    db: str = Field(...)
    db_schema: str = Field(default="public")
    user: str = Field(...)
    password: str = Field(...)

    # --- Pool parameters (для psycopg_pool) ---
    pool_min_size: int = Field(default=1, ge=1)
    pool_max_size: int = Field(default=20, ge=1, le=100)
    
    # --- Timeouts ---
    connect_timeout: int = Field(default=10, description="Connection timeout in seconds")

    @computed_field
    @property
    def connection_url(self) -> str:
        """
        Connection URL для psycopg.
        Формат: postgresql://user:password@host:port/dbname
        """
        return (
            f"postgresql://{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.db}"
        )
    
    @property
    def connection_kwargs(self) -> dict:
        """
        Параметры подключения как словарь.
        Удобно для psycopg.connect(**kwargs).
        """
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.db,
            "user": self.user,
            "password": self.password,
            "connect_timeout": self.connect_timeout,
            "options": f"-c search_path={self.db_schema}",  # 👈 используем схему
        }

class Settings(BaseSettings):
    """
    Top-level application settings aggregating sub-configs.
    """
    model_config = SettingsConfigDict(
        env_file=ENV_FILE_PATH,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    proxy: ProxyConfig = Field(default_factory=ProxyConfig)
    pg: PostgresConfig = Field(default_factory=PostgresConfig)


# Глобальный экземпляр настроек, используемый во всем приложении
settings = Settings()