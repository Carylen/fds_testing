# app/fds/rules.py
from __future__ import annotations

import asyncio
import time
from typing import Optional, Dict, Any, List

from app.core.time_utils import utc_now
from app.fds.storage import storage
from app.fds.schemas import (
    ProductType,
    ActionType,
    RuleType,
    TimeWindow,
    TransactionRequest,
    TransactionRecord,
    RuleConditions,
    FraudRule,
    FDSResult,
)


class RuleEngine:
    def __init__(self) -> None:
        self.rules: List[FraudRule] = []
        self.rules_lock = asyncio.Lock()
        self.load_default_rules()
    
    def load_default_rules(self) -> None:
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
                    check_per_denom=True,
                ),
                action=ActionType.BLOCK,
                priority=1,
            ),
            FraudRule(
                id="R002",
                name="1 Minute - Paket Data per Denom Limit",
                type=RuleType.DENOM_FREQUENCY,
                conditions=RuleConditions(
                    product_type=ProductType.PAKET_DATA,
                    time_window=TimeWindow.MINUTE,
                    max_count=2,
                    check_per_denom=True,
                ),
                action=ActionType.BLOCK,
                priority=1,
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
                    check_per_denom=True,
                ),
                action=ActionType.BLOCK,
                priority=2,
            ),
            FraudRule(
                id="R004",
                name="Daily - Paket Data per Denom Limit",
                type=RuleType.DENOM_FREQUENCY,
                conditions=RuleConditions(
                    product_type=ProductType.PAKET_DATA,
                    time_window=TimeWindow.DAY,
                    max_count=3,
                    check_per_denom=True,
                ),
                action=ActionType.BLOCK,
                priority=2,
            ),
            FraudRule(
                id="R005",
                name="Daily - Pulsa Total Limit",
                type=RuleType.FREQUENCY,
                conditions=RuleConditions(
                    product_type=ProductType.PULSA,
                    time_window=TimeWindow.DAY,
                    max_count=2,
                ),
                action=ActionType.BLOCK,
                priority=3,
            ),
            FraudRule(
                id="R006",
                name="Daily - Paket Data Total Limit",
                type=RuleType.FREQUENCY,
                conditions=RuleConditions(
                    product_type=ProductType.PAKET_DATA,
                    time_window=TimeWindow.DAY,
                    max_count=4,
                ),
                action=ActionType.BLOCK,
                priority=3,
            ),
            # 1 Month Window Rules
            FraudRule(
                id="R007",
                name="Monthly - Pulsa Total Limit",
                type=RuleType.FREQUENCY,
                conditions=RuleConditions(
                    product_type=ProductType.PULSA,
                    time_window=TimeWindow.MONTH,
                    max_count=50,
                ),
                action=ActionType.ALERT,
                priority=4,
            ),
            FraudRule(
                id="R008",
                name="Monthly - Paket Data Total Limit",
                type=RuleType.FREQUENCY,
                conditions=RuleConditions(
                    product_type=ProductType.PAKET_DATA,
                    time_window=TimeWindow.MONTH,
                    max_count=100,
                ),
                action=ActionType.ALERT,
                priority=4,
            ),
            # Amount Threshold
            FraudRule(
                id="R009",
                name="Unusual Transaction Amount",
                type=RuleType.AMOUNT_THRESHOLD,
                conditions=RuleConditions(
                    deviation_multiplier=2.5,
                    lookback_days=30,
                ),
                action=ActionType.ALERT,
                priority=5,
            ),
        ]
    
    async def add_rule(self, rule: FraudRule) -> None:
        async with self.rules_lock:
            self.rules.append(rule)
            self.rules.sort(key=lambda x: x.priority)
    
    async def get_rule(self, rule_id: str) -> Optional[FraudRule]:
        async with self.rules_lock:
            return next((r for r in self.rules if r.id == rule_id), None)
    
    async def update_rule(self, rule_id: str, updated_rule: FraudRule) -> bool:
        async with self.rules_lock:
            for i, rule in enumerate(self.rules):
                if rule.id == rule_id:
                    self.rules[i] = updated_rule
                    self.rules.sort(key=lambda x: x.priority)
                    return True
            return False
    
    async def delete_rule(self, rule_id: str) -> bool:
        async with self.rules_lock:
            for i, rule in enumerate(self.rules):
                if rule.id == rule_id:
                    self.rules.pop(i)
                    return True
            return False
    
    async def _get_enabled_rules(self) -> List[FraudRule]:
        async with self.rules_lock:
            enabled = [r for r in self.rules if r.enabled]
            enabled.sort(key=lambda x: x.priority)
            return enabled
    
    async def check_frequency_rule(
        self,
        rule: FraudRule,
        transaction: TransactionRequest,
    ) -> Optional[Dict[str, Any]]:
        if rule.conditions.product_type and rule.conditions.product_type != transaction.product_type:
            return None
        
        count = await storage.get_transaction_count(
            customer_id=transaction.customer_id,
            window=rule.conditions.time_window,
            product_type=transaction.product_type,
        )
        
        if rule.conditions.max_count is not None and count >= rule.conditions.max_count:
            return {
                "rule_id": rule.id,
                "rule_name": rule.name,
                "action": rule.action,
                "reason": f"Exceeded {rule.conditions.max_count} transactions in {rule.conditions.time_window}",
                "current_count": count,
                "window": rule.conditions.time_window,
            }
        
        return None
    
    async def check_denom_frequency_rule(
        self,
        rule: FraudRule,
        transaction: TransactionRequest,
    ) -> Optional[Dict[str, Any]]:
        if rule.conditions.product_type and rule.conditions.product_type != transaction.product_type:
            return None
        
        count = await storage.get_transaction_count(
            customer_id=transaction.customer_id,
            window=rule.conditions.time_window,
            product_type=transaction.product_type,
            denom=transaction.denom,
        )
        
        if rule.conditions.max_count is not None and count >= rule.conditions.max_count:
            return {
                "rule_id": rule.id,
                "rule_name": rule.name,
                "action": rule.action,
                "reason": (
                    f"Exceeded {rule.conditions.max_count} transactions "
                    f"for denom {transaction.denom} in {rule.conditions.time_window}"
                ),
                "current_count": count,
                "denom": transaction.denom,
                "window": rule.conditions.time_window,
            }
        
        return None
    
    async def check_amount_threshold_rule(
        self,
        rule: FraudRule,
        transaction: TransactionRequest,
    ) -> Optional[Dict[str, Any]]:
        avg_amount = await storage.get_average_amount(
            customer_id=transaction.customer_id,
            product_type=transaction.product_type,
            denom=transaction.denom,
            lookback_days=rule.conditions.lookback_days or 30,
        )
        
        if avg_amount > 0 and rule.conditions.deviation_multiplier:
            threshold = avg_amount * rule.conditions.deviation_multiplier
            if transaction.amount > threshold:
                return {
                    "rule_id": rule.id,
                    "rule_name": rule.name,
                    "action": rule.action,
                    "reason": f"Unusual amount: {transaction.amount} vs avg {avg_amount:.2f}",
                    "amount": transaction.amount,
                    "average": avg_amount,
                    "threshold": threshold,
                }
        
        return None
    
    async def check_transaction(self, transaction: TransactionRequest) -> FDSResult:
        """Main checking logic with sliding window."""
        start_time = time.time()
        triggered_rules: List[Dict[str, Any]] = []
        final_action = ActionType.ALLOW
        
        # Gunakan waktu server UTC untuk rule
        transaction.timestamp = utc_now()
        
        enabled_rules = await self._get_enabled_rules()
        
        for rule in enabled_rules:
            violation: Optional[Dict[str, Any]] = None
            
            if rule.type == RuleType.FREQUENCY:
                violation = await self.check_frequency_rule(rule, transaction)
            elif rule.type == RuleType.DENOM_FREQUENCY:
                violation = await self.check_denom_frequency_rule(rule, transaction)
            elif rule.type == RuleType.AMOUNT_THRESHOLD:
                violation = await self.check_amount_threshold_rule(rule, transaction)
            
            if violation:
                triggered_rules.append(violation)
                if violation["action"] == ActionType.BLOCK:
                    final_action = ActionType.BLOCK
                    break
                elif violation["action"] == ActionType.ALERT and final_action == ActionType.ALLOW:
                    final_action = ActionType.ALERT
        
        if final_action != ActionType.BLOCK:
            await storage.add_transaction(TransactionRecord(**transaction.dict()))
        
        processing_time = (time.time() - start_time) * 1000
        
        return FDSResult(
            allowed=(final_action != ActionType.BLOCK),
            action=final_action,
            triggered_rules=triggered_rules,
            processing_time_ms=round(processing_time, 2),
            metadata={
                "customer_id": transaction.customer_id,
                "transaction_id": transaction.transaction_id,
                "product_type": transaction.product_type.value,
            },
        )


rule_engine = RuleEngine()
