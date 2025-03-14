from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from src.config import settings

# Создаём движок
engine = create_engine(settings.db_url)

# Создаём сессию
Session = sessionmaker(bind=engine)

# Проверяем подключение
with Session() as session:
    result = session.execute(text("SELECT 1"))
    print("Database connection test:", result.scalar())  # Должно вывести 1

# Проверяем данные в таблице
with Session() as session:
    result = session.execute(text("SELECT * FROM telegram_links WHERE id = 4"))
    print("Data for id=4:", result.fetchone())