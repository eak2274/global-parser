# config.py
from pathlib import Path

from pydantic import Field
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


# Глобальный экземпляр настроек, используемый во всем приложении
settings = Settings()