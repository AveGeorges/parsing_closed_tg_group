from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4

from sqlmodel import Field, SQLModel, Relationship
from sqlalchemy import BigInteger


class CategoryMessageLink(SQLModel, table=True):
    __tablename__ = "category_message"

    category_id: int = Field(foreign_key="categories.id", primary_key=True)
    message_id: str = Field(foreign_key="messages.id", primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class CategoryTelegramLinkLink(SQLModel, table=True):
    __tablename__ = "category_telegram_link"
    category_id: int = Field(foreign_key="categories.id", primary_key=True)
    telegram_link_id: str = Field(foreign_key="telegram_links.id", primary_key=True)


class Category(SQLModel, table=True):
    __tablename__ = "categories"

    id: int = Field(primary_key=True)
    name: str
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    global_search: int

    messages: list["Message"] = Relationship(back_populates="categories", link_model=CategoryMessageLink)
    telegram_links: list["TelegramLink"] = Relationship(back_populates="categories",
                                                        link_model=CategoryTelegramLinkLink)
    keywords: list["Keyword"] = Relationship(back_populates="category")
    negative_keywords: list["NegativeKeyword"] = Relationship(back_populates="category")


class Message(SQLModel, table=True):
    __tablename__ = "messages"

    id: str = Field(primary_key=True, default_factory=lambda: str(uuid4()))
    text: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    from_user_id: Optional[int]
    from_username: Optional[str]
    chat_id: Optional[int] = Field(sa_type=BigInteger)
    link: Optional[str]
    date: Optional[datetime]

    categories: list["Category"] = Relationship(back_populates="messages", link_model=CategoryMessageLink)


class TelegramLink(SQLModel, table=True):
    __tablename__ = "telegram_links"

    id: str = Field(primary_key=True)
    link: Optional[str]
    link_raw: Optional[str]
    chat_id: Optional[int] = Field(sa_type=BigInteger)
    closed_group_id: Optional[str]
    last_check_at: Optional[datetime]
    last_message_id: Optional[int]
    created_at: datetime
    updated_at: datetime
    invalid: Optional[bool]
    parser_account: Optional[str]

    categories: list["Category"] = Relationship(back_populates="telegram_links", link_model=CategoryTelegramLinkLink)


class Keyword(SQLModel, table=True):
    __tablename__ = "keywords"

    id: str = Field(primary_key=True)
    keyword: str  # Убедитесь, что этот столбец существует в таблице
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    category_id: int = Field(foreign_key="categories.id")

    # Связь с категорией
    category: "Category" = Relationship(back_populates="keywords")
    

class NegativeKeyword(SQLModel, table=True):
    __tablename__ = "negative_keywords"

    id: str = Field(primary_key=True)
    keyword: str
    created_at: datetime
    updated_at: datetime
    category_id: int = Field(foreign_key="categories.id")

    category: Category = Relationship(back_populates="negative_keywords")
