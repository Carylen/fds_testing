"""
Fraud Detection System (FDS) - Rule-Based Layer
Biller Aggregator - FastAPI Implementation
WITHOUT Redis - Using In-Memory Sliding Window
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Literal
from datetime import datetime, timedelta, timezone
from enum import Enum
from collections import deque
from contextlib import asynccontextmanager
import asyncio
import httpx
import time
import os
from dotenv import load_dotenv
from threading import Lock

load_dotenv()
# ================== MODELS ==================

class ProductType(str, Enum):
    PULSA = "pulsa"
    PAKET_DATA = "paket_data"

class ActionType(str, Enum):
    BLOCK = "block"
    ALERT = "alert"
    ALLOW = "allow"

class RuleType(str, Enum):
    FREQUENCY = "frequency"
    AMOUNT_THRESHOLD = "amount_threshold"
    DENOM_FREQUENCY = "denom_frequency"

class TimeWindow(str, Enum):
    MINUTE = "1m"
    DAY = "1d"
    MONTH = "1M"

class TransactionRequest(BaseModel):
    transaction_id: str
    customer_id: str
    partner_id: str
    product_type: ProductType
    denom: int
    amount: int
    timestamp: Optional[datetime] = Field(default_factory=datetime.now)

class TransactionRecord(BaseModel):
    transaction_id: str
    customer_id: str
    partner_id: str
    product_type: ProductType
    denom: int
    amount: int
    timestamp: datetime

class RuleConditions(BaseModel):
    product_type: Optional[ProductType] = None
    time_window: TimeWindow = TimeWindow.DAY
    max_count: Optional[int] = None
    deviation_multiplier: Optional[float] = None
    lookback_days: Optional[int] = 30
    check_per_denom: Optional[bool] = False

class FraudRule(BaseModel):
    id: str
    name: str
    type: RuleType
    conditions: RuleConditions
    action: ActionType
    enabled: bool = True
    priority: int = 1

class FDSResult(BaseModel):
    allowed: bool
    action: ActionType
    triggered_rules: List[Dict[str, Any]] = []
    processing_time_ms: float
    metadata: Dict[str, Any] = {}

# ================== IN-MEMORY STORAGE ==================

class SlidingWindowStore:
    """
    In-memory sliding window storage
    Efficiently stores transactions and auto-cleans old data
    """
    
    def __init__(self):
        self.transactions: Dict[str, deque] = {}  # customer_id -> deque of transactions
        self.lock = Lock()
        self.max_history_days = 7  # Keep max 7 days (1 week) of history
        self.last_cleanup = datetime.now(timezone.utc)
    
    def _get_window_duration(self, window: TimeWindow) -> timedelta:
        """Get timedelta for window"""
        if window == TimeWindow.MINUTE:
            return timedelta(minutes=1)
        elif window == TimeWindow.DAY:
            return timedelta(days=1)
        elif window == TimeWindow.MONTH:
            return timedelta(days=30)
        return timedelta(days=1)
    
    def _clean_old_transactions(self, customer_id: str):
        """Remove transactions older than max_history_days"""
        if customer_id not in self.transactions:
            return
        
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.max_history_days)
        
        # Remove old transactions from the front
        removed_count = 0
        while self.transactions[customer_id]:
            txn = self.transactions[customer_id][0]
            # Normalize both timestamps to UTC
            txn_time = txn.timestamp if txn.timestamp.tzinfo else txn.timestamp.replace(tzinfo=timezone.utc)
            
            if txn_time < cutoff:
                self.transactions[customer_id].popleft()
                removed_count += 1
            else:
                break
        
        # Remove empty deques
        if not self.transactions[customer_id]:
            del self.transactions[customer_id]
        
        return removed_count
    
    def add_transaction(self, transaction: TransactionRecord):
        """Add transaction to store"""
        with self.lock:
            # Normalize timestamp to UTC
            if transaction.timestamp.tzinfo is None:
                transaction.timestamp = transaction.timestamp.replace(tzinfo=timezone.utc)
            
            if transaction.customer_id not in self.transactions:
                self.transactions[transaction.customer_id] = deque()
            
            self.transactions[transaction.customer_id].append(transaction)
            
            # Clean old data only every 1 hour (performance optimization)
            now = datetime.now(timezone.utc)
            if (now - self.last_cleanup).total_seconds() > 3600:  # 1 hour
                self._clean_all_old_transactions()
                self.last_cleanup = now
    
    def _clean_all_old_transactions(self):
        """Clean old transactions for all customers"""
        total_removed = 0
        for customer_id in list(self.transactions.keys()):
            removed = self._clean_old_transactions(customer_id)
            total_removed += removed
        print(f"🧹 Cleaned {total_removed} old transactions")
        return total_removed
    
    def get_transactions_in_window(
        self,
        customer_id: str,
        window: TimeWindow,
        product_type: Optional[ProductType] = None,
        denom: Optional[int] = None
    ) -> List[TransactionRecord]:
        """Get transactions within time window with filters"""
        with self.lock:
            if customer_id not in self.transactions:
                return []
            
            cutoff_time = datetime.now(timezone.utc) - self._get_window_duration(window)
            
            # Filter transactions
            filtered = []
            for txn in reversed(self.transactions[customer_id]):
                # Normalize timestamp
                txn_time = txn.timestamp if txn.timestamp.tzinfo else txn.timestamp.replace(tzinfo=timezone.utc)
                
                # Stop if too old (deque is sorted by time)
                if txn_time < cutoff_time:
                    break
                
                # Apply filters
                if product_type and txn.product_type != product_type:
                    continue
                if denom is not None and txn.denom != denom:
                    continue
                
                filtered.append(txn)
            
            return filtered
    
    def get_transaction_count(
        self,
        customer_id: str,
        window: TimeWindow,
        product_type: Optional[ProductType] = None,
        denom: Optional[int] = None
    ) -> int:
        """Count transactions in window"""
        return len(self.get_transactions_in_window(
            customer_id, window, product_type, denom
        ))
    
    def get_average_amount(
        self,
        customer_id: str,
        product_type: ProductType,
        denom: int,
        lookback_days: int = 30
    ) -> float:
        """Calculate average amount for customer/product/denom"""
        with self.lock:
            if customer_id not in self.transactions:
                return 0.0
            
            cutoff_time = datetime.now(timezone.utc) - timedelta(days=lookback_days)
            
            amounts = []
            for txn in self.transactions[customer_id]:
                # Normalize timestamp
                txn_time = txn.timestamp if txn.timestamp.tzinfo else txn.timestamp.replace(tzinfo=timezone.utc)
                
                if (txn_time >= cutoff_time and 
                    txn.product_type == product_type and 
                    txn.denom == denom):
                    amounts.append(txn.amount)
            
            return sum(amounts) / len(amounts) if amounts else 0.0
    
    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics"""
        with self.lock:
            total_transactions = sum(len(txns) for txns in self.transactions.values())
            return {
                "total_customers": len(self.transactions),
                "total_transactions": total_transactions,
                "avg_transactions_per_customer": (
                    total_transactions / len(self.transactions) 
                    if self.transactions else 0
                )
            }

storage = SlidingWindowStore()

# ================== RULE ENGINE ==================

class RuleEngine:
    def __init__(self):
        self.rules: List[FraudRule] = []
        self.rules_lock = Lock()
        self.load_default_rules()
    
    def load_default_rules(self):
        """Load default rules with sliding windows"""
        self.rules = [
            # 1 Minute Window Rules
            FraudRule(
                id="R001",
                name="1 Minute - Pulsa per Denom Limit",
                type=RuleType.DENOM_FREQUENCY,
                conditions=RuleConditions(
                    product_type=ProductType.PULSA,
                    time_window=TimeWindow.MINUTE,
                    max_count=2,
                    check_per_denom=True
                ),
                action=ActionType.BLOCK,
                priority=1
            ),
            FraudRule(
                id="R002",
                name="1 Minute - Paket Data per Denom Limit",
                type=RuleType.DENOM_FREQUENCY,
                conditions=RuleConditions(
                    product_type=ProductType.PAKET_DATA,
                    time_window=TimeWindow.MINUTE,
                    max_count=2,
                    check_per_denom=True
                ),
                action=ActionType.BLOCK,
                priority=1
            ),
            
            # 1 Day Window Rules
            FraudRule(
                id="R003",
                name="Daily - Pulsa per Denom Limit",
                type=RuleType.DENOM_FREQUENCY,
                conditions=RuleConditions(
                    product_type=ProductType.PULSA,
                    time_window=TimeWindow.DAY,
                    max_count=3,
                    check_per_denom=True
                ),
                action=ActionType.BLOCK,
                priority=2
            ),
            FraudRule(
                id="R004",
                name="Daily - Paket Data per Denom Limit",
                type=RuleType.DENOM_FREQUENCY,
                conditions=RuleConditions(
                    product_type=ProductType.PAKET_DATA,
                    time_window=TimeWindow.DAY,
                    max_count=3,
                    check_per_denom=True
                ),
                action=ActionType.BLOCK,
                priority=2
            ),
            FraudRule(
                id="R005",
                name="Daily - Pulsa Total Limit",
                type=RuleType.FREQUENCY,
                conditions=RuleConditions(
                    product_type=ProductType.PULSA,
                    time_window=TimeWindow.DAY,
                    max_count=2
                ),
                action=ActionType.BLOCK,
                priority=3
            ),
            FraudRule(
                id="R006",
                name="Daily - Paket Data Total Limit",
                type=RuleType.FREQUENCY,
                conditions=RuleConditions(
                    product_type=ProductType.PAKET_DATA,
                    time_window=TimeWindow.DAY,
                    max_count=4
                ),
                action=ActionType.BLOCK,
                priority=3
            ),
            
            # 1 Month Window Rules
            FraudRule(
                id="R007",
                name="Monthly - Pulsa Total Limit",
                type=RuleType.FREQUENCY,
                conditions=RuleConditions(
                    product_type=ProductType.PULSA,
                    time_window=TimeWindow.MONTH,
                    max_count=50
                ),
                action=ActionType.ALERT,
                priority=4
            ),
            FraudRule(
                id="R008",
                name="Monthly - Paket Data Total Limit",
                type=RuleType.FREQUENCY,
                conditions=RuleConditions(
                    product_type=ProductType.PAKET_DATA,
                    time_window=TimeWindow.MONTH,
                    max_count=100
                ),
                action=ActionType.ALERT,
                priority=4
            ),
            
            # Amount Threshold
            FraudRule(
                id="R009",
                name="Unusual Transaction Amount",
                type=RuleType.AMOUNT_THRESHOLD,
                conditions=RuleConditions(
                    deviation_multiplier=2.5,
                    lookback_days=30
                ),
                action=ActionType.ALERT,
                priority=5
            )
        ]
    
    def add_rule(self, rule: FraudRule):
        """Add new rule"""
        with self.rules_lock:
            self.rules.append(rule)
            self.rules.sort(key=lambda x: x.priority)
    
    def get_rule(self, rule_id: str) -> Optional[FraudRule]:
        """Get rule by ID"""
        with self.rules_lock:
            return next((r for r in self.rules if r.id == rule_id), None)
    
    def update_rule(self, rule_id: str, updated_rule: FraudRule) -> bool:
        """Update existing rule"""
        with self.rules_lock:
            for i, rule in enumerate(self.rules):
                if rule.id == rule_id:
                    self.rules[i] = updated_rule
                    self.rules.sort(key=lambda x: x.priority)
                    return True
            return False
    
    def delete_rule(self, rule_id: str) -> bool:
        """Delete rule"""
        with self.rules_lock:
            for i, rule in enumerate(self.rules):
                if rule.id == rule_id:
                    self.rules.pop(i)
                    return True
            return False
    
    async def check_frequency_rule(
        self, 
        rule: FraudRule, 
        transaction: TransactionRequest
    ) -> Optional[Dict[str, Any]]:
        """Check frequency-based rule"""
        if rule.conditions.product_type and rule.conditions.product_type != transaction.product_type:
            return None
        
        count = storage.get_transaction_count(
            customer_id=transaction.customer_id,
            window=rule.conditions.time_window,
            product_type=transaction.product_type
        )
        
        if count >= rule.conditions.max_count:
            return {
                "rule_id": rule.id,
                "rule_name": rule.name,
                "action": rule.action,
                "reason": f"Exceeded {rule.conditions.max_count} transactions in {rule.conditions.time_window}",
                "current_count": count,
                "window": rule.conditions.time_window
            }
        
        return None
    
    async def check_denom_frequency_rule(
        self,
        rule: FraudRule,
        transaction: TransactionRequest
    ) -> Optional[Dict[str, Any]]:
        """Check frequency per denom rule"""
        if rule.conditions.product_type and rule.conditions.product_type != transaction.product_type:
            return None
        
        count = storage.get_transaction_count(
            customer_id=transaction.customer_id,
            window=rule.conditions.time_window,
            product_type=transaction.product_type,
            denom=transaction.denom
        )
        
        if count >= rule.conditions.max_count:
            return {
                "rule_id": rule.id,
                "rule_name": rule.name,
                "action": rule.action,
                "reason": f"Exceeded {rule.conditions.max_count} transactions for denom {transaction.denom} in {rule.conditions.time_window}",
                "current_count": count,
                "denom": transaction.denom,
                "window": rule.conditions.time_window
            }
        
        return None
    
    async def check_amount_threshold_rule(
        self,
        rule: FraudRule,
        transaction: TransactionRequest
    ) -> Optional[Dict[str, Any]]:
        """Check unusual amount rule"""
        avg_amount = storage.get_average_amount(
            customer_id=transaction.customer_id,
            product_type=transaction.product_type,
            denom=transaction.denom,
            lookback_days=rule.conditions.lookback_days
        )
        
        if avg_amount > 0:
            threshold = avg_amount * rule.conditions.deviation_multiplier
            if transaction.amount > threshold:
                return {
                    "rule_id": rule.id,
                    "rule_name": rule.name,
                    "action": rule.action,
                    "reason": f"Unusual amount: {transaction.amount} vs avg {avg_amount:.2f}",
                    "amount": transaction.amount,
                    "average": avg_amount,
                    "threshold": threshold
                }
        
        return None
    
    async def check_transaction(self, transaction: TransactionRequest) -> FDSResult:
        """Main checking logic with sliding window"""
        start_time = time.time()
        triggered_rules = []
        final_action = ActionType.ALLOW
        
        # Get enabled rules sorted by priority
        with self.rules_lock:
            enabled_rules = [r for r in self.rules if r.enabled]
            enabled_rules.sort(key=lambda x: x.priority)
        
        # Check all rules
        for rule in enabled_rules:
            violation = None
            
            if rule.type == RuleType.FREQUENCY:
                violation = await self.check_frequency_rule(rule, transaction)
            elif rule.type == RuleType.DENOM_FREQUENCY:
                violation = await self.check_denom_frequency_rule(rule, transaction)
            elif rule.type == RuleType.AMOUNT_THRESHOLD:
                violation = await self.check_amount_threshold_rule(rule, transaction)
            
            if violation:
                triggered_rules.append(violation)
                
                # Update final action (BLOCK takes precedence)
                if violation["action"] == ActionType.BLOCK:
                    final_action = ActionType.BLOCK
                    break  # Stop checking if blocked
                elif violation["action"] == ActionType.ALERT and final_action == ActionType.ALLOW:
                    final_action = ActionType.ALERT
        
        # Store transaction if allowed
        if final_action != ActionType.BLOCK:
            storage.add_transaction(TransactionRecord(**transaction.dict()))
        
        processing_time = (time.time() - start_time) * 1000
        
        return FDSResult(
            allowed=(final_action != ActionType.BLOCK),
            action=final_action,
            triggered_rules=triggered_rules,
            processing_time_ms=round(processing_time, 2),
            metadata={
                "customer_id": transaction.customer_id,
                "transaction_id": transaction.transaction_id,
                "product_type": transaction.product_type.value
            }
        )

rule_engine = RuleEngine()

# ================== NOTIFICATION SERVICE ==================

class NotificationService:
    def __init__(self):
        self.google_chat_webhook = os.getenv("GCHAT_SPACE")
        self.email_recipients = ["ops@company.com"]
    
    async def send_notification(self, result: FDSResult, transaction: TransactionRequest):
        """Send notification via Google Chat"""
        if result.action == ActionType.ALLOW:
            return
        
        message = self.format_message(result, transaction)
        await self.send_google_chat(message)
    
    def format_message(self, result: FDSResult, transaction: TransactionRequest) -> str:
        """Format notification message"""
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
    
    async def send_google_chat(self, message: str):
        """Send to Google Chat"""
        try:
            async with httpx.AsyncClient() as client:
                await client.post(
                    self.google_chat_webhook,
                    json={"text": message},
                    timeout=5.0
                )
        except Exception as e:
            print(f"Failed to send Google Chat notification: {e}")

notification_service = NotificationService()

# ================== FASTAPI APP ==================

app = FastAPI(
    title="FDS Rule-Based System - Sliding Window",
    description="Fraud Detection System with In-Memory Sliding Window (1m, 1d, 1M)",
    version="2.0.0"
)

@app.on_event("startup")
async def startup_event():
    """Startup event - start background cleanup job"""
    asyncio.create_task(periodic_cleanup())
    print("✅ FDS System Started - Cleanup job running every 1 week")

async def periodic_cleanup():
    """Periodic cleanup every 1 week"""
    while True:
        await asyncio.sleep(604800)  # 7 days = 604800 seconds
        print("🧹 Starting weekly cleanup...")
        removed = storage._clean_all_old_transactions()
        print(f"✅ Weekly cleanup completed: {removed} transactions removed")

# ================== ENDPOINTS ==================

@app.post("/fds/check", response_model=FDSResult)
async def check_transaction(
    transaction: TransactionRequest,
    background_tasks: BackgroundTasks
):
    """
    Check transaction against fraud rules using sliding window
    Target: < 200ms processing time
    """
    result = await rule_engine.check_transaction(transaction)
    
    # Send notification in background
    if result.action != ActionType.ALLOW:
        background_tasks.add_task(
            notification_service.send_notification,
            result,
            transaction
        )
    
    return result

@app.get("/fds/rules", response_model=List[FraudRule])
async def get_rules():
    """Get all fraud rules"""
    return rule_engine.rules

@app.post("/fds/rules", response_model=FraudRule)
async def create_rule(rule: FraudRule):
    """Create new fraud rule (instantly applied)"""
    rule_engine.add_rule(rule)
    return rule

@app.put("/fds/rules/{rule_id}", response_model=FraudRule)
async def update_rule(rule_id: str, rule: FraudRule):
    """Update existing fraud rule (instantly applied)"""
    if not rule_engine.update_rule(rule_id, rule):
        raise HTTPException(status_code=404, detail="Rule not found")
    return rule

@app.delete("/fds/rules/{rule_id}")
async def delete_rule(rule_id: str):
    """Delete fraud rule (instantly applied)"""
    if not rule_engine.delete_rule(rule_id):
        raise HTTPException(status_code=404, detail="Rule not found")
    return {"message": "Rule deleted successfully"}

@app.get("/fds/health")
async def health_check():
    """Health check endpoint"""
    stats = storage.get_stats()
    return {
        "status": "healthy",
        "storage": "in-memory",
        "rules_count": len(rule_engine.rules),
        "storage_stats": stats
    }

@app.get("/fds/stats/{customer_id}")
async def get_customer_stats(customer_id: str):
    """Get customer transaction statistics with sliding windows"""
    stats = {}
    
    for window in TimeWindow:
        stats[window.value] = {}
        for product_type in ProductType:
            count = storage.get_transaction_count(
                customer_id=customer_id,
                window=window,
                product_type=product_type
            )
            stats[window.value][product_type.value] = count
    
    return {
        "customer_id": customer_id,
        "windows": stats
    }

@app.get("/fds/customer/{customer_id}/history")
async def get_customer_history(
    customer_id: str,
    window: TimeWindow = TimeWindow.DAY,
    product_type: Optional[ProductType] = None
):
    """Get customer transaction history"""
    transactions = storage.get_transactions_in_window(
        customer_id=customer_id,
        window=window,
        product_type=product_type
    )
    
    return {
        "customer_id": customer_id,
        "window": window,
        "product_type": product_type,
        "count": len(transactions),
        "transactions": [t.dict() for t in transactions]
    }

@app.get("/fds/storage/stats")
async def get_storage_stats():
    """Get storage statistics"""
    return storage.get_stats()

@app.post("/fds/storage/cleanup")
async def manual_cleanup():
    """Manually trigger cleanup of old transactions"""
    removed = storage._clean_all_old_transactions()
    return {
        "message": "Cleanup completed",
        "transactions_removed": removed,
        "retention_days": storage.max_history_days
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)