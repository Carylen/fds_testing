# app/fds/rules.py
from __future__ import annotations

import asyncio
import time
import logging
import pytz
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

from sqlalchemy import select, update, delete
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.time_utils import jkt_utc_now, ensure_jkt_utc
from app.db.session import AsyncSessionLocal
from app.db.models import FDSRuleDB
from app.fds.schemas import (
    ProductType,
    ActionType,
    RuleType,
    TimeWindow,
    TransactionRequest,
    RuleConditions,
    FraudRule,
    FDSResult,
)
from app.fds.history import CustomerHistory, load_customer_history

logger = logging.getLogger(__name__)

JAKARTA_TZ = pytz.timezone("Asia/Jakarta")
UTC = pytz.UTC

def db_to_pydantic(db_rule: FDSRuleDB) -> FraudRule:
    return FraudRule(
        id=db_rule.rule_id,
        name=db_rule.name,
        type=RuleType(db_rule.type),
        action=ActionType(db_rule.action),
        enabled=db_rule.enabled,
        priority=db_rule.priority,
        conditions=RuleConditions(**db_rule.conditions),
    )


def pydantic_to_db(rule: FraudRule) -> FDSRuleDB:
    return FDSRuleDB(
        rule_id=rule.id,
        name=rule.name,
        type=rule.type.value,
        action=rule.action.value,
        enabled=rule.enabled,
        priority=rule.priority,
        conditions=rule.conditions.dict(),
    )


def get_window_bounds(now: datetime, window: TimeWindow):
    """
    Mengembalikan (start, end) untuk window:
    - 1m  -> 1 menit ke belakang sampai sekarang
    - 1d  -> hari kalender yang sama (00:00 s/d besok 00:00)
    - 1M  -> bulan kalender yang sama (tgl 1 s/d awal bulan berikutnya)
    """
    # now = ensure_jkt_utc(now)
    # if window == TimeWindow.MINUTE:
    #     start = now - timedelta(minutes=1)
    #     end = now
    # elif window == TimeWindow.DAY:
    #     # awal hari (UTC) hari ini
    #     start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    #     end = start + timedelta(days=1)
    # elif window == TimeWindow.MONTH:
    #     # awal bulan ini
    #     start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    #     # awal bulan depan
    #     if start.month == 12:
    #         end = start.replace(year=start.year + 1, month=1)
    #     else:
    #         end = start.replace(month=start.month + 1)
    # else:
    #     # default fallback: 1 hari ke belakang
    #     start = now - timedelta(days=1)
    #     end = now

    # return start, end

    if now.tzinfo is None:
        now_utc = UTC.localize(now)
    else:
        now_utc = now.astimezone(UTC)

    if window == TimeWindow.MINUTE:
        start_utc = now_utc - timedelta(minutes=1)
        end_utc = now_utc
        return start_utc, end_utc

    # convert ke waktu lokal Jakarta
    now_local = now_utc.astimezone(JAKARTA_TZ)

    if window == TimeWindow.DAY:
        start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        end_local = start_local + timedelta(days=1)
    elif window == TimeWindow.MONTH:
        start_local = now_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if start_local.month == 12:
            end_local = start_local.replace(year=start_local.year + 1, month=1)
        else:
            end_local = start_local.replace(month=start_local.month + 1)
    else:
        # fallback: 1 hari terakhir
        start_local = now_local - timedelta(days=1)
        end_local = now_local

    # convert balik ke UTC untuk dipakai banding created_on (yang sudah UTC)
    start_utc = start_local.astimezone(UTC)
    end_utc = end_local.astimezone(UTC)
    return start_utc, end_utc
class RuleEngine:
    def __init__(self) -> None:
        self.rules: List[FraudRule] = []
        self.rules_lock = asyncio.Lock()

    def _default_rules(self) -> List[FraudRule]:
        """Seed default rules (dipakai kalau DB kosong)."""
        return [
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
            # FraudRule(
            #     id="R007",
            #     name="Monthly - Pulsa Total Limit",
            #     type=RuleType.FREQUENCY,
            #     conditions=RuleConditions(
            #         product_type=ProductType.PULSA,
            #         time_window=TimeWindow.MONTH,
            #         max_count=50,
            #     ),
            #     action=ActionType.ALERT,
            #     priority=4,
            # ),
            # FraudRule(
            #     id="R008",
            #     name="Monthly - Paket Data Total Limit",
            #     type=RuleType.FREQUENCY,
            #     conditions=RuleConditions(
            #         product_type=ProductType.PAKET_DATA,
            #         time_window=TimeWindow.MONTH,
            #         max_count=100,
            #     ),
            #     action=ActionType.ALERT,
            #     priority=4,
            # ),

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

    async def init_from_db(self) -> None:
        """
        Load rules dari database.
        - Kalau tabel kosong: seed default rules + simpan ke DB.
        - Kalau ada: pakai rules dari DB.
        """
        async with AsyncSessionLocal() as session:
            res = await session.execute(select(FDSRuleDB))
            rows: list[FDSRuleDB] = res.scalars().all()

        if not rows:
            logger.info("ℹ️ fds_rules empty, seeding default rules to DB")
            default_rules = self._default_rules()
            async with self.rules_lock:
                self.rules = default_rules
                self.rules.sort(key=lambda x: x.priority)

            async with AsyncSessionLocal() as session:
                for rule in default_rules:
                    session.add(pydantic_to_db(rule))
                await session.commit()
        else:
            async with self.rules_lock:
                self.rules = [db_to_pydantic(r) for r in rows]
                self.rules.sort(key=lambda x: x.priority)
            logger.info(f"ℹ️ Loaded {len(self.rules)} rules from DB")
            logger.info(f"ℹ️ Loaded Rules : {self.rules}")

    async def add_rule(self, rule: FraudRule) -> None:
        async with self.rules_lock:
            if any(r.id == rule.id for r in self.rules):
                raise ValueError(f"Rule with id {rule.id} already exists")
            self.rules.append(rule)
            self.rules.sort(key=lambda x: x.priority)

        async with AsyncSessionLocal() as session:
            session.add(pydantic_to_db(rule))
            await session.commit()

    async def get_rule(self, rule_id: str) -> Optional[FraudRule]:
        async with self.rules_lock:
            return next((r for r in self.rules if r.id == rule_id), None)

    async def update_rule(self, rule_id: str, updated_rule: FraudRule) -> bool:
        updated = False
        async with self.rules_lock:
            for i, rule in enumerate(self.rules):
                if rule.id == rule_id:
                    self.rules[i] = updated_rule
                    self.rules.sort(key=lambda x: x.priority)
                    updated = True
                    break

        if not updated:
            return False

        async with AsyncSessionLocal() as session:
            await session.execute(
                update(FDSRuleDB)
                .where(FDSRuleDB.rule_id == rule_id)
                .values(
                    rule_id=updated_rule.id,
                    name=updated_rule.name,
                    type=updated_rule.type.value,
                    action=updated_rule.action.value,
                    enabled=updated_rule.enabled,
                    priority=updated_rule.priority,
                    conditions=updated_rule.conditions.dict(),
                )
            )
            await session.commit()

        return True

    async def delete_rule(self, rule_id: str) -> bool:
        deleted = False
        async with self.rules_lock:
            for i, rule in enumerate(self.rules):
                if rule.id == rule_id:
                    self.rules.pop(i)
                    deleted = True
                    break

        if not deleted:
            return False

        async with AsyncSessionLocal() as session:
            await session.execute(
                delete(FDSRuleDB).where(FDSRuleDB.rule_id == rule_id)
            )
            await session.commit()

        return True

    async def _get_enabled_rules(self) -> List[FraudRule]:
        async with self.rules_lock:
            enabled = [r for r in self.rules if r.enabled]
            enabled.sort(key=lambda x: x.priority)
            return enabled

    # ---------- RULE CHECKS PAKAI HISTORY (tanpa counter) ----------

    def _check_frequency_rule(
        self,
        rule: FraudRule,
        tx_req: TransactionRequest,
        history: CustomerHistory,
        now,
    ) -> Optional[Dict[str, Any]]:
        if rule.conditions.product_type and rule.conditions.product_type != tx_req.product_type:
            return None
        logger.info(f"NOW : {now}")
        # win_start = _window_start(now, rule.conditions.time_window)
        win_start, win_end = get_window_bounds(now, rule.conditions.time_window)
        logger.info(f"WIN START: {win_start}")
        logger.info(f"WIN END: {win_end}")
        max_count = rule.conditions.max_count or 0
        logger.info(f"MAX COUNTS : {max_count}")

        count = 0
        for row in history.rows:
            if row.created_on < win_start or row.created_on >= win_end:
                continue
            # kalau mau filter cuma success, cek row.source == "pay"
            count += 1

        if count >= max_count:
            return {
                "rule_id": rule.id,
                "rule_name": rule.name,
                "action": rule.action,
                "reason": (
                    f"Exceeded {max_count} transactions in window "
                    f"{rule.conditions.time_window.value}"
                ),
                "current_count": count,
                "window": rule.conditions.time_window.value,
            }
        return None

    def _check_denom_frequency_rule(
        self,
        rule: FraudRule,
        tx_req: TransactionRequest,
        history: CustomerHistory,
        now,
    ) -> Optional[Dict[str, Any]]:
        if rule.conditions.product_type and rule.conditions.product_type != tx_req.product_type:
            return None

        # win_start = _window_start(now, rule.conditions.time_window)
        win_start, win_end = get_window_bounds(now, rule.conditions.time_window)
        max_count = rule.conditions.max_count or 0

        count = 0
        for row in history.rows:
            if row.created_on < win_start or row.created_on >= win_end:
                continue
            if rule.conditions.check_per_denom and row.amount != tx_req.denom:
                continue
            # kalau mau hanya success: if row.source != "pay": continue
            count += 1

        if count >= max_count:
            return {
                "rule_id": rule.id,
                "rule_name": rule.name,
                "action": rule.action,
                "reason": (
                    f"Exceeded {max_count} transactions "
                    f"for denom {tx_req.denom} in window {rule.conditions.time_window.value}"
                ),
                "current_count": count,
                "denom": tx_req.denom,
                "window": rule.conditions.time_window.value,
            }
        return None

    def _check_amount_threshold_rule(
        self,
        rule: FraudRule,
        tx_req: TransactionRequest,
        history: CustomerHistory,
        now,
    ) -> Optional[Dict[str, Any]]:
        lookback_days = rule.conditions.lookback_days or 30
        cutoff = now - timedelta(days=lookback_days)
        mult = rule.conditions.deviation_multiplier
        if not mult:
            return None

        amounts: List[int] = []
        for row in history.rows:
            if row.created_on < cutoff:
                continue
            if row.amount != tx_req.denom:
                continue
            # kalau mau hanya success: if row.source != "pay": continue
            amounts.append(row.amount)

        if not amounts:
            return None

        avg_amount = sum(amounts) / len(amounts)
        threshold = avg_amount * mult

        if tx_req.amount > threshold:
            return {
                "rule_id": rule.id,
                "rule_name": rule.name,
                "action": rule.action,
                "reason": f"Unusual amount: {tx_req.amount} vs avg {avg_amount:.2f}",
                "amount": tx_req.amount,
                "average": avg_amount,
                "threshold": threshold,
            }
        return None

    # ---------- MAIN CHECK ----------

    async def check_transaction(self, tx_req: TransactionRequest, db_session: AsyncSession) -> FDSResult:
        t0 = time.perf_counter()
        start_time = time.time()
        triggered_rules: List[Dict[str, Any]] = []
        final_action = ActionType.ALLOW

        # pakai waktu server UTC
        # tx_req.timestamp = jkt_utc_now()
        tx_req.timestamp = ensure_jkt_utc(tx_req.timestamp if tx_req.timestamp else jkt_utc_now())

        # 1 query ke DB untuk load history 30 hari
        history = await load_customer_history(
            customer_id=tx_req.customer_id,
            partner_id=tx_req.partner_id,
            session=db_session
        )
        t1 = time.perf_counter()
        # logger.info(f"🔎DATA CUST : {history}")
        history_count = len(history.rows)
        logger.info(f"🔎DATA CUST : {history_count}")
        now = tx_req.timestamp

        enabled_rules = await self._get_enabled_rules()
        t2 = time.perf_counter()
        
        for rule in enabled_rules:
            violation: Optional[Dict[str, Any]] = None

            if rule.type == RuleType.FREQUENCY:
                violation = self._check_frequency_rule(rule, tx_req, history, now)
            elif rule.type == RuleType.DENOM_FREQUENCY:
                violation = self._check_denom_frequency_rule(rule, tx_req, history, now)
            elif rule.type == RuleType.AMOUNT_THRESHOLD:
                violation = self._check_amount_threshold_rule(rule, tx_req, history, now)

            if violation:
                logger.info(f"⚠️ VIOLATION : {violation}")
                triggered_rules.append(violation)
                if violation["action"] == ActionType.BLOCK:
                    final_action = ActionType.BLOCK
                    break
                elif violation["action"] == ActionType.ALERT and final_action == ActionType.ALLOW:
                    final_action = ActionType.ALERT

        processing_time = (time.time() - start_time) * 1000.0

        db_ms = (t1 - t0) * 1000
        rule_ms = (t2 - t1) * 1000
        total_ms = (t2 - t0) * 1000
        logger.info(f"[PERF] db={db_ms:.1f}ms rules={rule_ms:.1f}ms total={total_ms:.1f}ms")
        
        return FDSResult(
            allowed=(final_action != ActionType.BLOCK),
            action=final_action,
            triggered_rules=triggered_rules,
            processing_time_ms=round(processing_time, 2),
            metadata={
                "customer_id": tx_req.customer_id,
                "transaction_id": tx_req.transaction_id,
                "product_type": tx_req.product_type.value,
                "rules_loaded": len(enabled_rules),     # 👈 berapa rule yang aktif
                "history_rows": len(history.rows),      # 👈 berapa transaksi yang kebaca dari DB log
            },
        )


rule_engine = RuleEngine()
