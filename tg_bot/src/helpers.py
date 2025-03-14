from pyrogram.types import Message

from src.database.models import TelegramLink
from src.database.operations import (get_keyword_rows, )

from typing import Union


async def find_message_categories(text: str, telegram_link: TelegramLink) -> set:
    keyword_rows = await get_keyword_rows(telegram_link.id)
    categories = set()

    if not text:
        return categories

    text_lowercase = text.lower()

    for keyword_row in keyword_rows:
        if hasattr(keyword_row, 'keyword') and keyword_row.keyword in text_lowercase:
            categories.add(keyword_row.category.id)

    return categories


def extract_chat(telegram_link: TelegramLink) -> Union[str, int]:
    if not telegram_link.link:
        return None
    if telegram_link.link.startswith("t.me/+"):
        return telegram_link.link
    elif telegram_link.link.startswith("t.me/"):
        return telegram_link.link.removeprefix("t.me/")
    elif isinstance(telegram_link.chat_id, int):
        return telegram_link.chat_id
    return None


def get_text_with_link(message: Message):
    text = message.text
    title = message.chat.title
    link = message.link

    text += f'\n\n<a href="{link}">Переслано из {title}</a>'
    if message.from_user and message.from_user.username:
        text += f'\n\n👉 @{message.from_user.username}'
    return text
