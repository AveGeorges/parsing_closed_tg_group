from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(".env"))
print("Environment variables loaded")


class Settings(BaseSettings):
    api_id: int
    api_hash: str
    session_name: str

    db_url: str
    MAX_LEVEL_OF_DUPLICATE_SIMILARITY: float = 0.7
    project_root: Path = Path(__file__).parent.parent

    model_config = SettingsConfigDict(
        env_file='../.env',
        env_file_encoding='utf-8',
        extra='ignore'
    )


settings = Settings()
print("Loaded settings:", settings.dict())
