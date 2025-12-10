# app/fds/storage.py
from __future__ import annotations

from collections import deque
from typing import Dict, List

import asyncio
from datetime import timedelta

from app.core.time_utils import utc_now, ensure_utc
from app.fds.schemas import (
    TransactionRecord,
    ProductType,
    TimeWindow,
)


class SlidingWindowStore:
    """
    In-memory sliding window storage (async + UTC)
    Menyimpan transaksi per customer dengan retention max_history_days.
    """
    
    def __init__(self) -> None:
        self.transactions: Dict[str, deque[TransactionRecord]] = {}
        self.lock = asyncio.Lock()
        self.max_history_days = 30  # align dengan 1M / lookback 30 hari
    
    def _get_window_duration(self, window: TimeWindow) -> timedelta:
        if window == TimeWindow.MINUTE:
            return timedelta(minutes=1)
        elif window == TimeWindow.DAY:
            return timedelta(days=1)
        elif window == TimeWindow.MONTH:
            return timedelta(days=30)
        return timedelta(days=1)
    
    async def _clean_all_old_transactions(self) -> int:
        """Hapus transaksi yang lebih tua daripada max_history_days (untuk semua customer)."""
        cutoff = utc_now() - timedelta(days=self.max_history_days)
        total_removed = 0
        
        async with self.lock:
            for customer_id in list(self.transactions.keys()):
                dq = self.transactions[customer_id]
                while dq and ensure_utc(dq[0].timestamp) < cutoff:
                    dq.popleft()
                    total_removed += 1
                if not dq:
                    del self.transactions[customer_id]
        
        print(f"🧹 Cleanup removed {total_removed} old transactions")
        return total_removed
    
    async def cleanup_loop(self, interval_seconds: int = 3600) -> None:
        """Periodic cleanup loop (dipanggil dari lifespan)."""
        while True:
            await asyncio.sleep(interval_seconds)
            await self._clean_all_old_transactions()
    
    async def add_transaction(self, transaction: TransactionRecord) -> None:
        """Tambah transaksi ke store (timestamp di-normalisasi ke UTC)."""
        transaction.timestamp = ensure_utc(transaction.timestamp)
        async with self.lock:
            dq = self.transactions.setdefault(transaction.customer_id, deque())
            dq.append(transaction)
    
    async def get_transactions_in_window(
        self,
        customer_id: str,
        window: TimeWindow,
        product_type: ProductType | None = None,
        denom: int | None = None,
    ) -> List[TransactionRecord]:
        """Ambil transaksi dalam window tertentu & filter optional."""
        cutoff_time = utc_now() - self._get_window_duration(window)
        
        async with self.lock:
            if customer_id not in self.transactions:
                return []
            
            filtered: List[TransactionRecord] = []
            for txn in reversed(self.transactions[customer_id]):
                txn_time = ensure_utc(txn.timestamp)
                if txn_time < cutoff_time:
                    break
                if product_type and txn.product_type != product_type:
                    continue
                if denom is not None and txn.denom != denom:
                    continue
                filtered.append(txn)
        
        return filtered
    
    async def get_transaction_count(
        self,
        customer_id: str,
        window: TimeWindow,
        product_type: ProductType | None = None,
        denom: int | None = None,
    ) -> int:
        txns = await self.get_transactions_in_window(
            customer_id=customer_id,
            window=window,
            product_type=product_type,
            denom=denom,
        )
        return len(txns)
    
    async def get_average_amount(
        self,
        customer_id: str,
        product_type: ProductType,
        denom: int,
        lookback_days: int = 30,
    ) -> float:
        """Hitung rata-rata amount dalam lookback_days."""
        cutoff_time = utc_now() - timedelta(days=lookback_days)
        
        async with self.lock:
            if customer_id not in self.transactions:
                return 0.0
            
            amounts = [
                txn.amount
                for txn in self.transactions[customer_id]
                if (
                    ensure_utc(txn.timestamp) >= cutoff_time
                    and txn.product_type == product_type
                    and txn.denom == denom
                )
            ]
        
        return float(sum(amounts)) / len(amounts) if amounts else 0.0
    
    async def get_stats(self) -> dict:
        """Statistik sederhana storage."""
        async with self.lock:
            total_transactions = sum(len(txns) for txns in self.transactions.values())
            total_customers = len(self.transactions)
        
        return {
            "total_customers": total_customers,
            "total_transactions": total_transactions,
            "avg_transactions_per_customer": (
                total_transactions / total_customers if total_customers else 0
            ),
        }


# singleton store
storage = SlidingWindowStore()
