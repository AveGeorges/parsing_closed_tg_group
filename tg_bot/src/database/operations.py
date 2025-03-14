from datetime import datetime, timedelta
from typing import Sequence, Optional
from uuid import uuid4

from async_lru import alru_cache
from bs4 import BeautifulSoup as bs
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sqlalchemy import case, func
from sqlalchemy.orm import selectinload
from sqlmodel import select, update, asc, or_, desc

from src.config import settings
from src.database.create_db import AsyncSessionLocal
from src.database.models import Message, Keyword, NegativeKeyword, TelegramLink, CategoryMessageLink, Category, CategoryTelegramLinkLink
from src.logger import bot_logger
import asyncio


@alru_cache(ttl=300)
async def get_keyword_rows(telegram_link_id: str) -> Sequence[Keyword]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Keyword)
            .options(selectinload(Keyword.category))
            .join(Category)
            .join(CategoryTelegramLinkLink)
            .where(or_(
                CategoryTelegramLinkLink.telegram_link_id == telegram_link_id,
                Category.global_search == 1
            ))
            .group_by(Keyword.id)
        )
        keyword_rows = result.scalars().all()
        return keyword_rows


@alru_cache(ttl=300)
async def get_negative_keyword_rows() -> Sequence[NegativeKeyword]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(NegativeKeyword).options(selectinload(NegativeKeyword.category)))
        return result.scalars().all()


async def save_message(
    text: str,
    date: datetime,
    categories: Sequence[int],
    from_user_id: Optional[int] = None,
    from_username: Optional[str] = None,
    chat_id: Optional[int] = None,
    link: Optional[str] = None
) -> None:
    async with AsyncSessionLocal() as session:
        async with session.begin():
            message_id = str(uuid4())
            message_row = Message(
                id=message_id,
                text=text,
                date=date,
                from_user_id=from_user_id,
                from_username=from_username,
                chat_id=chat_id,
                link=link
            )
            session.add(message_row)

            await session.flush()

            for category_id in categories:
                session.add(CategoryMessageLink(category_id=category_id, message_id=message_id))

            bot_logger.info(f"Message {message_id} saved to DB")


async def get_tg_links(exclude_unknown_chats=True, session_name: Optional[str] = None) -> Sequence[TelegramLink]:
    try:
        async with AsyncSessionLocal() as session:
            if exclude_unknown_chats:
                query = (
                    select(TelegramLink)
                    .where(
                        (TelegramLink.chat_id != None) == exclude_unknown_chats,
                        or_(TelegramLink.parser_account == session_name, TelegramLink.parser_account == None),
                    )
                    .order_by(
                        case(
                            (TelegramLink.last_check_at == None, 1), else_=0
                        ),
                        asc(TelegramLink.last_check_at)
                    )
                    .limit(30)
                )
            else:
                query = (
                    select(TelegramLink)
                    .where(
                        TelegramLink.chat_id == None,
                        or_(TelegramLink.parser_account == session_name, TelegramLink.parser_account == None),
                        or_(TelegramLink.invalid == False, TelegramLink.invalid == None),
                    )
                    .order_by(func.rand())
                    .limit(1)
                )

            # Используем await для выполнения асинхронного запроса
            result = await session.execute(query)
            return result.scalars().all()  # Теперь scalars() вызывается на результате
    except Exception as e:
        bot_logger.error(f"Error during database operation: {e}", exc_info=True)
        return []


async def update_tg_link(link: TelegramLink) -> TelegramLink:
    async with AsyncSessionLocal() as session:
        await session.add(link)
        await session.commit()
        await session.refresh(link)
        return link


@alru_cache(ttl=300)
async def check_duplicate(text: str) -> bool:
    if not text:
        return True

    try:
        text = bs(text.split('<a href="')[0], "html.parser").text
        yesterday = datetime.utcnow() - timedelta(days=1)

        async with AsyncSessionLocal() as session:
            async with session.begin():
                statement = select(Message.text).where(Message.created_at > yesterday)
                result = await session.execute(statement)
                messages_texts = result.scalars().all()

                for sent_text in messages_texts:
                    sent_text = bs(sent_text.split('<a href="')[0], 'html.parser').text

                    similarity = await asyncio.to_thread(
                        lambda: cosine_similarity(
                            TfidfVectorizer().fit_transform([text, sent_text])
                        )[0][1]
                    )

                    if similarity >= settings.MAX_LEVEL_OF_DUPLICATE_SIMILARITY:
                        return False
        return True
    except Exception as e:
        bot_logger.error(f"Error in check_duplicate: {e}")
        return True


async def save_chat_id(telegram_link: TelegramLink, chat_id: int, session_name: Optional[str] = None):
    if telegram_link.chat_id:
        return
    async with AsyncSessionLocal() as session:
        telegram_link.chat_id = chat_id
        telegram_link.parser_account = session_name
        await session.merge(telegram_link)
        await session.commit()


async def set_telegram_link_invalid(telegram_link: TelegramLink):
    async with AsyncSessionLocal() as session:
        telegram_link.invalid = True
        await session.merge(telegram_link)
        await session.commit()


async def update_telegram_link_last_check(telegram_link: TelegramLink, last_check_at: datetime, last_message_id: int):
    async with AsyncSessionLocal() as session:
        telegram_link.last_check_at = last_check_at
        telegram_link.last_message_id = last_message_id
        await session.merge(telegram_link)
        await session.commit()


async def set_telegram_link_chat_id_null(telegram_link: TelegramLink, session_name: Optional[str]):
    async with AsyncSessionLocal() as session:
        if telegram_link.parser_account != session_name:
            return

        if telegram_link.parser_account == 'velinapp':
            return

        telegram_link.chat_id = None
        await session.merge(telegram_link)
        await session.commit()
