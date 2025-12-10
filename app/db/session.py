# app/db/session.py
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    create_async_engine,
    async_sessionmaker,
)

from app.core.config import DATABASE_URL
from app.db.base import Base

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

async def init_db() -> None:
    """Create all tables (untuk dev; di prod biasanya pakai migration)."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
