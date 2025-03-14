import logging
from logging.handlers import RotatingFileHandler
from src.config import settings


def get_logger(
    name: str,
    filename='my_log.log',
    fmt='%(asctime)s %(levelname)s %(message)s',
    level=logging.INFO,
):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Указываем кодировку UTF-8 для корректного отображения текста
    handler = RotatingFileHandler(
        filename,
        maxBytes=1000000,
        backupCount=5,
        encoding='utf-8'  # Добавляем кодировку UTF-8
    )

    formatter = logging.Formatter(fmt)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


# admin_logger = get_logger(
#     "admin logger",
#     filename=settings.project_root / "../../public/storage/parsing_log.html"
# )

bot_logger = get_logger(
    "bot logger",
    filename=settings.project_root / "storage/logs/parsing.log"
)