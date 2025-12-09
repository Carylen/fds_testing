"""
Fraud Detection System (FDS) - Rule-Based Layer
Biller Aggregator - FastAPI Implementation
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Literal
from datetime import datetime, date
from enum import Enum
import asyncio
import json
import os
import httpx
from redis.asyncio import Redis
from contextlib import asynccontextmanager
import time
from dotenv import load_dotenv

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

class TransactionRequest(BaseModel):
    transaction_id: str
    customer_id: str
    partner_id: str
    product_type: ProductType
    denom: int
    amount: int
    timestamp: Optional[datetime] = Field(default_factory=datetime.now)

class RuleConditions(BaseModel):
    product_type: Optional[ProductType] = None
    timeframe: Optional[str] = "daily"  # daily, hourly
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

# ================== REDIS CLIENT ==================

class RedisClient:
    def __init__(self):
        self.redis: Optional[Redis] = None
    
    async def connect(self):
        self.redis = Redis(
            host='localhost',
            port=6379,
            db=0,
            decode_responses=True,
            socket_connect_timeout=5
        )
        await self.redis.ping()
    
    async def disconnect(self):
        if self.redis:
            await self.redis.close()
    
    async def get_counter(self, key: str) -> int:
        val = await self.redis.get(key)
        return int(val) if val else 0
    
    async def increment_counter(self, key: str, ttl: int = 86400):
        pipe = self.redis.pipeline()
        pipe.incr(key)
        pipe.expire(key, ttl)
        result = await pipe.execute()
        return result[0]
    
    async def get_avg_amount(self, key: str) -> float:
        val = await self.redis.get(key)
        return float(val) if val else 0.0
    
    async def set_avg_amount(self, key: str, value: float, ttl: int = 2592000):
        await self.redis.setex(key, ttl, str(value))

redis_client = RedisClient()

# ================== RULE ENGINE ==================

class RuleEngine:
    def __init__(self):
        self.rules: List[FraudRule] = []
        self.load_default_rules()
    
    def load_default_rules(self):
        """Load default rules"""
        self.rules = [
            FraudRule(
                id="R001",
                name="Daily Transaction Limit - Pulsa per Denom",
                type=RuleType.DENOM_FREQUENCY,
                conditions=RuleConditions(
                    product_type=ProductType.PULSA,
                    timeframe="daily",
                    max_count=3,
                    check_per_denom=True
                ),
                action=ActionType.BLOCK,
                priority=1
            ),
            FraudRule(
                id="R002",
                name="Daily Transaction Limit - Paket Data per Denom",
                type=RuleType.DENOM_FREQUENCY,
                conditions=RuleConditions(
                    product_type=ProductType.PAKET_DATA,
                    timeframe="daily",
                    max_count=3,
                    check_per_denom=True
                ),
                action=ActionType.BLOCK,
                priority=1
            ),
            FraudRule(
                id="R003",
                name="Daily Transaction Limit - Pulsa Total",
                type=RuleType.FREQUENCY,
                conditions=RuleConditions(
                    product_type=ProductType.PULSA,
                    timeframe="daily",
                    max_count=2
                ),
                action=ActionType.BLOCK,
                priority=2
            ),
            FraudRule(
                id="R004",
                name="Daily Transaction Limit - Paket Data Total",
                type=RuleType.FREQUENCY,
                conditions=RuleConditions(
                    product_type=ProductType.PAKET_DATA,
                    timeframe="daily",
                    max_count=4
                ),
                action=ActionType.BLOCK,
                priority=2
            ),
            FraudRule(
                id="R005",
                name="Unusual Transaction Amount",
                type=RuleType.AMOUNT_THRESHOLD,
                conditions=RuleConditions(
                    deviation_multiplier=2.5,
                    lookback_days=30
                ),
                action=ActionType.ALERT,
                priority=3
            )
        ]
    
    async def reload_rules(self):
        """Reload rules from Redis"""
        rules_json = await redis_client.redis.get("fds:rules:config")
        if rules_json:
            rules_data = json.loads(rules_json)
            self.rules = [FraudRule(**rule) for rule in rules_data]
    
    async def save_rules(self):
        """Save rules to Redis"""
        rules_data = [rule.dict() for rule in self.rules]
        await redis_client.redis.set("fds:rules:config", json.dumps(rules_data))
    
    def add_rule(self, rule: FraudRule):
        """Add new rule"""
        self.rules.append(rule)
        self.rules.sort(key=lambda x: x.priority)
    
    def get_rule(self, rule_id: str) -> Optional[FraudRule]:
        """Get rule by ID"""
        return next((r for r in self.rules if r.id == rule_id), None)
    
    def update_rule(self, rule_id: str, updated_rule: FraudRule) -> bool:
        """Update existing rule"""
        for i, rule in enumerate(self.rules):
            if rule.id == rule_id:
                self.rules[i] = updated_rule
                self.rules.sort(key=lambda x: x.priority)
                return True
        return False
    
    def delete_rule(self, rule_id: str) -> bool:
        """Delete rule"""
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
        
        today = date.today().isoformat()
        key = f"fraud:counter:{transaction.customer_id}:{today}:{transaction.product_type.value}"
        
        count = await redis_client.get_counter(key)
        
        if count >= rule.conditions.max_count:
            return {
                "rule_id": rule.id,
                "rule_name": rule.name,
                "action": rule.action,
                "reason": f"Exceeded {rule.conditions.max_count} transactions per day",
                "current_count": count
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
        
        today = date.today().isoformat()
        key = f"fraud:counter:{transaction.customer_id}:{today}:{transaction.product_type.value}:{transaction.denom}"
        
        count = await redis_client.get_counter(key)
        
        if count >= rule.conditions.max_count:
            return {
                "rule_id": rule.id,
                "rule_name": rule.name,
                "action": rule.action,
                "reason": f"Exceeded {rule.conditions.max_count} transactions for denom {transaction.denom}",
                "current_count": count,
                "denom": transaction.denom
            }
        
        return None
    
    async def check_amount_threshold_rule(
        self,
        rule: FraudRule,
        transaction: TransactionRequest
    ) -> Optional[Dict[str, Any]]:
        """Check unusual amount rule"""
        # Get average amount for this customer + product + denom
        avg_key = f"fraud:avg:{transaction.customer_id}:{transaction.product_type.value}:{transaction.denom}"
        avg_amount = await redis_client.get_avg_amount(avg_key)
        
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
        """Main checking logic"""
        start_time = time.time()
        triggered_rules = []
        final_action = ActionType.ALLOW
        
        # Check all enabled rules sorted by priority
        for rule in sorted([r for r in self.rules if r.enabled], key=lambda x: x.priority):
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
        
        # Update counters if allowed (async, not blocking)
        if final_action != ActionType.BLOCK:
            await self.update_counters(transaction)
        
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
    
    async def update_counters(self, transaction: TransactionRequest):
        """Update transaction counters"""
        today = date.today().isoformat()
        
        # Counter per product type
        key1 = f"fraud:counter:{transaction.customer_id}:{today}:{transaction.product_type.value}"
        await redis_client.increment_counter(key1)
        
        # Counter per denom
        key2 = f"fraud:counter:{transaction.customer_id}:{today}:{transaction.product_type.value}:{transaction.denom}"
        await redis_client.increment_counter(key2)
        
        # Update average amount (simple moving average)
        avg_key = f"fraud:avg:{transaction.customer_id}:{transaction.product_type.value}:{transaction.denom}"
        current_avg = await redis_client.get_avg_amount(avg_key)
        
        # Simple update: if no avg, use current amount, else take simple average
        if current_avg == 0:
            new_avg = float(transaction.amount)
        else:
            # Weighted average (90% old, 10% new)
            new_avg = current_avg * 0.9 + transaction.amount * 0.1
        
        await redis_client.set_avg_amount(avg_key, new_avg, ttl=2592000)  # 30 days

rule_engine = RuleEngine()

# ================== NOTIFICATION SERVICE ==================

class NotificationService:
    def __init__(self):
        self.google_chat_webhook = os.getenv("GCHAT_SPACE")
        self.email_recipients = ["ops@company.com"]
    
    async def send_notification(self, result: FDSResult, transaction: TransactionRequest):
        """Send notification via Google Chat and Email"""
        if result.action == ActionType.ALLOW:
            return
        
        message = self.format_message(result, transaction)
        
        # Send to Google Chat (non-blocking)
        await self.send_google_chat(message)
        
        # Send email can be added here
        # await self.send_email(message)
    
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

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await redis_client.connect()
    await rule_engine.save_rules()
    print("✅ FDS System Started")
    yield
    # Shutdown
    await redis_client.disconnect()
    print("✅ FDS System Stopped")

app = FastAPI(
    title="FDS Rule-Based System",
    description="Fraud Detection System for Biller Aggregator",
    version="1.0.0",
    lifespan=lifespan
)

# ================== ENDPOINTS ==================

@app.post("/fds/check", response_model=FDSResult)
async def check_transaction(
    transaction: TransactionRequest,
    background_tasks: BackgroundTasks
):
    """
    Check transaction against fraud rules
    Target: < 200ms processing time
    """
    result = await rule_engine.check_transaction(transaction)
    
    # Send notification in background (non-blocking)
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
    """Create new fraud rule"""
    rule_engine.add_rule(rule)
    await rule_engine.save_rules()
    
    # Broadcast reload signal (for multiple instances)
    await redis_client.redis.publish("fds:rule:reload", rule.id)
    
    return rule

@app.put("/fds/rules/{rule_id}", response_model=FraudRule)
async def update_rule(rule_id: str, rule: FraudRule):
    """Update existing fraud rule"""
    if not rule_engine.update_rule(rule_id, rule):
        raise HTTPException(status_code=404, detail="Rule not found")
    
    await rule_engine.save_rules()
    await redis_client.redis.publish("fds:rule:reload", rule_id)
    
    return rule

@app.delete("/fds/rules/{rule_id}")
async def delete_rule(rule_id: str):
    """Delete fraud rule"""
    if not rule_engine.delete_rule(rule_id):
        raise HTTPException(status_code=404, detail="Rule not found")
    
    await rule_engine.save_rules()
    await redis_client.redis.publish("fds:rule:reload", rule_id)
    
    return {"message": "Rule deleted successfully"}

@app.post("/fds/rules/reload")
async def reload_rules():
    """Reload rules from Redis (for hot reload)"""
    await rule_engine.reload_rules()
    return {"message": "Rules reloaded successfully"}

@app.get("/fds/health")
async def health_check():
    """Health check endpoint"""
    try:
        await redis_client.redis.ping()
        return {
            "status": "healthy",
            "redis": "connected",
            "rules_count": len(rule_engine.rules)
        }
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "error": str(e)}
        )

@app.get("/fds/stats/{customer_id}")
async def get_customer_stats(customer_id: str):
    """Get customer transaction statistics"""
    today = date.today().isoformat()
    
    stats = {}
    for product_type in ProductType:
        key = f"fraud:counter:{customer_id}:{today}:{product_type.value}"
        count = await redis_client.get_counter(key)
        stats[product_type.value] = count
    
    return {
        "customer_id": customer_id,
        "date": today,
        "transactions": stats
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)