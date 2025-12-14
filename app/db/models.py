# app/db/models.py

from datetime import datetime
from sqlalchemy import Integer, String, DateTime, JSON, Boolean, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func
from app.db.base import Base

class FDSRuleDB(Base):
    __tablename__ = "fds_rules"

    # ID rule (mis: "R001") - primary key
    rule_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    name: Mapped[str] = mapped_column(String(255))
    type: Mapped[str] = mapped_column(String(50))     # 'frequency', 'denom_frequency', 'amount_threshold'
    action: Mapped[str] = mapped_column(String(20))   # 'block', 'alert', 'allow'
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    priority: Mapped[int] = mapped_column(Integer, default=1)
    conditions: Mapped[dict] = mapped_column(JSON) # contoh: {"product_type": "pulsa", "time_window": "1d", "max_count": 3, ...}
    created_on: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_on: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

class FraudEvent(Base):
    __tablename__ = "fraud_events"
    # __table_args__ = {
    #     UniqueConstraint("transaction_id", name="fraud_trx_id")
    # }

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    customer_id: Mapped[str] = mapped_column(String(100), index=True)
    partner_id: Mapped[str] = mapped_column(String(100), index=True)
    transaction_id: Mapped[str] = mapped_column(String(100), index=True)
    product_type: Mapped[str] = mapped_column(String(50))
    denom: Mapped[int] = mapped_column(Integer)
    amount: Mapped[int] = mapped_column(Integer)
    action: Mapped[str] = mapped_column(String(20))  # block / alert
    triggered_rules: Mapped[dict] = mapped_column(JSON)
    created_on: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        index=True,
    )
