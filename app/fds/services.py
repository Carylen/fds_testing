# app/fds/services.py
from __future__ import annotations

from app.db.session import AsyncSessionLocal
from app.db.models import FraudEvent
from app.fds.schemas import TransactionRequest, FDSResult


async def log_fraud_event_to_db(
    transaction: TransactionRequest,
    result: FDSResult,
) -> None:
    """Insert fraud event ke DB ketika BLOCK / ALERT."""
    async with AsyncSessionLocal() as session:
        event = FraudEvent(
            customer_id=transaction.customer_id,
            partner_id=transaction.partner_id,
            transaction_id=transaction.transaction_id,
            product_type=transaction.product_type.value,
            denom=transaction.denom,
            amount=transaction.amount,
            action=result.action.value,
            triggered_rules=result.triggered_rules,
        )
        session.add(event)
        try:
            await session.commit()
        except Exception as e:
            await session.rollback()
            print(f"Failed to insert FraudEvent: {e}")
