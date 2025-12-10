# app/fds/notifications.py
from __future__ import annotations

import httpx

from app.core.config import GCHAT_SPACE
from app.fds.schemas import FDSResult, TransactionRequest, ActionType


class NotificationService:
    def __init__(self) -> None:
        self.google_chat_webhook = GCHAT_SPACE
        self.email_recipients = ["ops@company.com"]  # placeholder
    
    async def send_notification(self, result: FDSResult, transaction: TransactionRequest) -> None:
        if result.action == ActionType.ALLOW:
            return
        message = self.format_message(result, transaction)
        await self.send_google_chat(message)
    
    def format_message(self, result: FDSResult, transaction: TransactionRequest) -> str:
        status = "🔴 BLOCKED" if result.action == ActionType.BLOCK else "⚠️ ALERT"
        
        msg = f"{status} - Fraud Detection Alert\n\n"
        msg += f"Transaction ID: {transaction.transaction_id}\n"
        msg += f"Customer ID: {transaction.customer_id}\n"
        msg += f"Partner ID: {transaction.partner_id}\n"
        msg += f"Product: {transaction.product_type.value}\n"
        msg += f"Denom: {transaction.denom}\n"
        msg += f"Amount: {transaction.amount}\n"
        msg += f"Timestamp: {transaction.timestamp}\n\n"
        
        msg += "Triggered Rules:\n"
        for rule in result.triggered_rules:
            msg += f"- {rule['rule_name']}: {rule['reason']}\n"
        
        msg += f"\nProcessing Time: {result.processing_time_ms}ms"
        return msg
    
    async def send_google_chat(self, message: str) -> None:
        if not self.google_chat_webhook:
            print("GCHAT_SPACE not configured, skipping notification")
            return
        
        try:
            async with httpx.AsyncClient() as client:
                await client.post(
                    self.google_chat_webhook,
                    json={"text": message},
                    timeout=5.0,
                )
        except Exception as e:
            print(f"Failed to send Google Chat notification: {e}")


notification_service = NotificationService()
