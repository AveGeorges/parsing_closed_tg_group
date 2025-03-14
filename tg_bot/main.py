from dataclasses import dataclass, field
from typing import Union
from datetime import datetime, timedelta
from typing import Optional, AsyncGenerator, List
import asyncio
import winloop
import random
from pyrogram import Client
from pyrogram.errors import UsernameInvalid, FloodWait, UsernameNotOccupied, BadRequest, ChannelInvalid
from pyrogram.types import Message
import traceback

from src.config import settings
from src.database.models import TelegramLink
from src.database.operations import (
   get_tg_links,
   save_message,
   check_duplicate,
   save_chat_id,
   set_telegram_link_invalid,
   update_telegram_link_last_check,
   set_telegram_link_chat_id_null,
)

from src.helpers import extract_chat, find_message_categories, get_text_with_link
from src.logger import  bot_logger


@dataclass
class ParserConfig:
   """Configuration settings for the parser"""
   MAX_MESSAGES_PER_CHAT: int = 300  # Уменьшено для снижения нагрузки
   MESSAGE_LOG_INTERVAL: int = 50
   MIN_DELAY: int = 1  # Минимальная задержка между запросами
   MAX_DELAY: int = 5  # Максимальная задержка между запросами
   PAUSE_AFTER_FULL_PARSING: int = 6 * 60 * 60  # Пауза в 6 часов после полного парсинга группы


@dataclass
class ClientConfig:
   """Configuration for a single Telegram client"""
   session_name: str
   api_id: int
   api_hash: str
   

@dataclass
class ParserStats:
   """Statistics tracking for parser operation"""
   session_name: str
   exclude_unknown_chats: bool
   message_count: int = 0
   chats_count: int = 0
   relevant_messages: int = 0
   start_time: datetime = field(default_factory=datetime.now)

   def log_starting(self, chats_count: int):
      """Log current starting process"""
      bot_logger.info(
         f"Get chats "
         f"Session {self.session_name}: "
         f"{self.exclude_unknown_chats=}, "
         f"Всего чатов: {chats_count} "
      )

   def log_progress(self, messages_in_chat: int, current_chat: str):
      """Log current parsing progress"""
      if self.message_count % ParserConfig.MESSAGE_LOG_INTERVAL == 0:
         bot_logger.info(
               f"Session {self.session_name}: "
               f"{self.exclude_unknown_chats=}, "
               f"{self.message_count=}, {messages_in_chat=}, "
               f"{self.chats_count=}, {self.relevant_messages=}, "
               f"current_chat={current_chat}"
         )

   def log_final_stats(self):
      """Log final parsing statistics"""
      duration_minutes = (datetime.now() - self.start_time).total_seconds() / 60
      bot_logger.info(
         f"Session {self.session_name} - "
         f"{self.exclude_unknown_chats=}, "
         f"Processed: {self.message_count=}, {self.chats_count=}, "
         f"{self.relevant_messages=}, {duration_minutes:.2f} min"
      )


class TelegramParser:
   def __init__(self, client: Client, session_name: str):
      self.client = client
      self.config = ParserConfig()
      self.session_name = session_name

   async def process_message(
        self,
        message: Message,
        chat_id: int,
        stats: ParserStats,
        telegram_link: TelegramLink,
) -> Optional[int]:
    if not message.text:
        bot_logger.info(f"Message {message.id} has no text, skipping")
        return None
     
      # Проверка на дубликаты
    if not await check_duplicate(message.text):
        bot_logger.info(f"Duplicate message {message.id} found, skipping")
        return None

    categories = await find_message_categories(message.text, telegram_link)
    if not categories:
        bot_logger.info(f"No categories found for message {message.id}, skipping")
        return message.id

    try:
        text_with_link = get_text_with_link(message)
        bot_logger.info(f"Prepared text with link for message {message.id}")
    except Exception as e:
        bot_logger.error(f"Failed to get text with link for message {message.id}: {e}")
        return message.id

    await self._save_relevant_message(message, text_with_link, categories)
    stats.relevant_messages += 1
    bot_logger.info(f"Message {message.id} processed and saved")

    return message.id


   async def _save_relevant_message(
      self,
      message: Message,
      text_with_link: str,
      categories: list
   ):
      """Save a relevant message to the database"""
      if not message or not text_with_link or not categories:
         bot_logger.error("Invalid data in _save_relevant_message")
         return

      await save_message(
         text=text_with_link,
         date=message.date,
         categories=categories,
         from_user_id=message.from_user.id if message.from_user else None,
         from_username=message.from_user.username if message.from_user else None,
         chat_id=message.chat.id,
         link=message.link,
      )
      bot_logger.info(f"{message.chat.id}: {text_with_link}")


   async def process_chat(
         self,
         telegram_link: TelegramLink,
         stats: ParserStats
   ) -> None:
      """Process a single chat's messages"""
      bot_logger.info(f"Starting to process chat: {telegram_link.link}")
      
      # Извлекаем chat (ID, username или инвайт-ссылку)
      chat = extract_chat(telegram_link)
      if not chat:
         bot_logger.warning(f"Failed to extract chat from link: {telegram_link.link}")
         return

      bot_logger.info(f"Successfully extracted chat: {chat}")
      max_id = telegram_link.last_message_id or 0
      last_check_at = datetime.utcnow()
      messages_in_chat = 0

      bot_logger.info(f"Initial parameters: max_id={max_id}, last_check_at={last_check_at}")

      try:
         # Получаем информацию о чате
         try:
            chat_info = await self.client.get_chat(chat)
            bot_logger.info(f"Chat info: {chat_info}")
         except (UsernameInvalid, UsernameNotOccupied, ChannelInvalid) as e:
            bot_logger.warning(f"Failed to get chat info for {chat}. Error: {e}")
            return
         except BadRequest as e:
            bot_logger.warning(f"BadRequest for chat: {chat}. Error: {e}")
            return

         # Если чат закрытый и бот не является участником, пытаемся вступить
         if chat_info.type in ["group", "supergroup", "channel"] and not chat_info.is_accessible:
            if telegram_link.link.startswith("t.me/+"):
                  await self.client.join_chat(telegram_link.link)
                  bot_logger.info(f"Successfully joined chat: {chat}")
            else:
                  bot_logger.warning(f"No invite link provided for private chat: {chat}")
                  return

         # Получаем сообщения из чата
         bot_logger.info(f"Fetching messages for chat: {chat} (type: {type(chat)})")
         async for message in self._get_chat_messages(chat, max_id):
            bot_logger.info(f"Content: {message.text}")
            messages_in_chat += 1
            if messages_in_chat > self.config.MAX_MESSAGES_PER_CHAT:
                  bot_logger.info(f"Reached message limit ({self.config.MAX_MESSAGES_PER_CHAT}) for chat: {chat}")
                  break

            bot_logger.info(f"Processing message {messages_in_chat} from chat {chat}")
            await save_chat_id(telegram_link, message.chat.id, self.session_name)
            stats.message_count += 1
            stats.log_progress(messages_in_chat, telegram_link.link)

            new_max_id = await self.process_message(message, message.chat.id, stats, telegram_link)
            if new_max_id:
                  max_id = max(new_max_id, max_id)
                  telegram_link.last_check_at = datetime.utcnow()
                  bot_logger.info(f"Updated max_id to {max_id}")

            # Задержка между обработкой сообщений
            delay = random.uniform(self.config.MIN_DELAY, self.config.MAX_DELAY)
            bot_logger.info(f"Sleeping for {delay:.2f} seconds")
            await asyncio.sleep(delay)

      except FloodWait as e:
         bot_logger.warning(f'FloodWait {e.value} seconds')
         await asyncio.sleep(e.value)
      except Exception as e:
         bot_logger.error(f"Unexpected error: {e}\n{traceback.format_exc()}", exc_info=True)
      finally:
         bot_logger.info(f"Finishing chat processing. Updating last check time to {last_check_at}")
         update_telegram_link_last_check(telegram_link, last_check_at, max_id)


   async def _get_chat_messages(
      self,
      chat: Union[str, int],
      min_id: int
   ) -> AsyncGenerator[Message, None]:
      if not chat:
         bot_logger.warning("Chat is None, skipping")
         return

      try:
         if isinstance(chat, str) and chat.startswith("t.me/+"):
               try:
                  chat_info = await self.client.get_chat(chat)
                  chat = chat_info.id
                  bot_logger.info(f"Resolved invite link {chat} to chat ID: {chat_info.id}")
               except (UsernameInvalid, UsernameNotOccupied, ChannelInvalid) as e:
                  bot_logger.warning(f"Failed to get chat info for {chat}. Error: {e}")
                  return
               except BadRequest as e:
                  bot_logger.warning(f"BadRequest for chat: {chat}. Error: {e}")
                  return

         bot_logger.info(f"Fetching messages for chat: {chat} (type: {type(chat)})")
         async for message in self.client.get_chat_history(chat, offset_id=min_id, limit=100):
               yield message
      except BadRequest as e:
         bot_logger.warning(f"BadRequest for chat: {chat}. Error: {e}")
      except Exception as e:
         bot_logger.error(f"Unexpected error for chat {chat}: {e}", exc_info=True)
      

   async def run(self, exclude_unknown_chats: bool = True) -> None:
      """Main parser loop"""
      bot_logger.info(f"Starting parser run with exclude_unknown_chats={exclude_unknown_chats}")
      inaccessible_chats = set()  # Множество для хранения недоступных чатов

      while True:
         bot_logger.info("Starting new parsing iteration")
         stats = ParserStats(session_name=self.session_name, exclude_unknown_chats=exclude_unknown_chats)
         bot_logger.info(f"Created new ParserStats instance for session {self.session_name}")

         bot_logger.info("Fetching Telegram links from database")
         links = await get_tg_links(exclude_unknown_chats=exclude_unknown_chats, session_name=self.session_name)
         bot_logger.info(f"Retrieved {len(links)} links from database")

         if not links:
               bot_logger.warning("No links found, sleeping for 5 minutes")
               await asyncio.sleep(300)
               continue

         stats.log_starting(len(links))
         
         bot_logger.info("Starting to process individual links")
         for telegram_link in links:
               if telegram_link.link in inaccessible_chats:
                  bot_logger.info(f"Skipping inaccessible chat: {telegram_link.link}")
                  continue  # Пропускаем недоступные чаты

               bot_logger.info(f"Processing link: {telegram_link.link}")
               stats.chats_count += 1
               bot_logger.info(f"Current chat count: {stats.chats_count}")

               try:
                  await self.process_chat(telegram_link, stats)
                  # Если чат успешно обработан, удаляем его из множества недоступных
                  if telegram_link.link in inaccessible_chats:
                     inaccessible_chats.remove(telegram_link.link)
               except Exception as e:
                  bot_logger.error(f"Error processing chat {telegram_link.link}: {e}")
                  inaccessible_chats.add(telegram_link.link)  # Добавляем чат в недоступные
               finally:
                  bot_logger.info(f"Finished processing link: {telegram_link.link}")

         bot_logger.info("Finished processing all links, logging final stats")
         stats.log_final_stats()
         bot_logger.info("Completed parsing iteration")
         
         # Добавляем паузу после завершения парсинга всех групп
         bot_logger.info(f"Pausing for {self.config.PAUSE_AFTER_FULL_PARSING // 3600} hours before next iteration")
         await asyncio.sleep(self.config.PAUSE_AFTER_FULL_PARSING)


async def main() -> None:
   # Конфигурация для одного клиента
   client_config = ClientConfig(
      session_name=settings.session_name,
      api_id=settings.api_id,
      api_hash=settings.api_hash
   )

   async with Client(
      name=client_config.session_name,
      api_id=client_config.api_id,
      api_hash=client_config.api_hash
   ) as app:
      # Запускаем основной парсер
      parser = TelegramParser(app, client_config.session_name)
      await parser.run()


if __name__ == "__main__":
   winloop.install()
   asyncio.run(main())