# app/db/models.py
from datetime import datetime

from sqlalchemy import Integer, String, DateTime, JSON
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.db.base import Base


class FraudEvent(Base):
    __tablename__ = "fraud_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    customer_id: Mapped[str] = mapped_column(String(100), index=True)
    partner_id: Mapped[str] = mapped_column(String(100), index=True)
    transaction_id: Mapped[str] = mapped_column(String(100), index=True)

    product_type: Mapped[str] = mapped_column(String(50))
    denom: Mapped[int] = mapped_column(Integer)
    amount: Mapped[int] = mapped_column(Integer)

    action: Mapped[str] = mapped_column(String(20))  # block / alert

    triggered_rules: Mapped[dict] = mapped_column(JSON)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        index=True,
    )
