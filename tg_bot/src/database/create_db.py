import logging

import sys
from pathlib import Path

# Добавляем корневую директорию проекта в sys.path
project_root = Path(__file__).resolve().parents[2]  # Переход на два уровня выше (src -> tg-bot)
sys.path.append(str(project_root))

# Теперь можно использовать абсолютный импорт
from src.config import settings


# Настройка логирования SQLAlchemy
sqlalchemy_logger = logging.getLogger('sqlalchemy.engine')
sqlalchemy_logger.setLevel(logging.INFO)

# Логи будут записываться в файл sqlalchemy.log
handler = logging.FileHandler('sqlalchemy.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)

sqlalchemy_logger.addHandler(handler)


from src.database.models import ( 
    Category,
    Message,
    TelegramLink,
    Keyword,
    NegativeKeyword,
    CategoryMessageLink,
    CategoryTelegramLinkLink,
)

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel

# Укажите путь к SQLite базе данных
# DATABASE_URL = "sqlite+aiosqlite:///test.db?charset=utf8"

# Укажите путь к PostgreSQL базе данных
DATABASE_URL = settings.db_url

# Создайте асинхронный движок для подключения к базе данных
engine = create_async_engine(DATABASE_URL, echo=True)

# Создайте асинхронную сессию с expire_on_commit=False
AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# Создайте все таблицы в базе данных
async def create_db_and_tables():
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)

if __name__ == "__main__":
    import asyncio
    asyncio.run(create_db_and_tables())