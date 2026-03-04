from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from src.config import settings

async_engine = create_async_engine(settings.database_url, echo=False)

session_factory = async_sessionmaker(
    async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    pass


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    async with session_factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
