# app/db/session.py
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    create_async_engine,
    async_sessionmaker,
)
from typing import AsyncGenerator
from app.core.config import DATABASE_URL, DATABASE_PPOB_URL
from app.db.base import Base

# Engine & session untuk DB FDS (rules + fraud_events)
engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    future=True,
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    expire_on_commit=False,
    class_=AsyncSession,
)

# Engine & session untuk DB LOG (logpaydata + logsuspectdata)
ppob_engine = create_async_engine(
    DATABASE_PPOB_URL,
    echo=False,
    future=True,
    pool_size=10,
    max_overflow=20,
    pool_recycle=1800,
)

PPOBAsyncSessionLocal = async_sessionmaker(
    ppob_engine,
    expire_on_commit=False,
    class_=AsyncSession,
)


async def init_db() -> None:
    """
    Create all tables di DB FDS (bukan di DB log).
    Di prod biasanya pakai migration.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def get_ppob_session() -> AsyncGenerator[AsyncSession, None]:
    """Dependency untuk inject session DB PPOB."""
    async with PPOBAsyncSessionLocal() as session:
        yield session
