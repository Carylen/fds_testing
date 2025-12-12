# app/fds/history.py
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Literal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import AsyncSessionLocal, PPOBAsyncSessionLocal
from app.core.config import MAX_HISTORY_DAYS
from app.core.time_utils import ensure_utc, utc_now

SourceType = Literal["pay", "suspect"]


@dataclass
class TxRow:
    partner_id: str
    customer_id: str
    switch_id: str
    amount: int
    created_on: datetime
    transaction_id: str
    tracking_ref: str
    source: SourceType  # 'pay' atau 'suspect'

@dataclass
class CustomerHistory:
    customer_id: str
    partner_id: str
    rows: List[TxRow]


async def load_customer_history(customer_id: str, partner_id: str, session: AsyncSession) -> CustomerHistory:
    """
    Load semua transaksi customer dalam MAX_HISTORY_DAYS terakhir
    dari dua tabel:
    - ppobprod.tb_r_logpaydata
    - ppobprod.tb_r_logsuspectdata

    Sesuaikan nama schema/tabel/kolom dengan DB kamu.
    """
    now = utc_now()
    start = now - timedelta(days=MAX_HISTORY_DAYS)

    sql = text("""
        SELECT
            partnerid   AS partner_id,
            customerid  AS customer_id,
            switchid    AS switch_id,
            amount      AS amount,
            CreatedOn   AS created_on,
            trxid       AS transaction_id,
            tracking_ref AS tracking_ref,
            'pay'       AS source
        FROM
            ppobprod.tb_r_logpaydata
        WHERE
            customerid = :customer_id
            AND transactiondate >= :start

        UNION ALL

        SELECT
            partnerid   AS partner_id,
            customerid  AS customer_id,
            switchid    AS switch_id,
            amount      AS amount,
            CreatedOn   AS created_on,
            trxid       AS transaction_id,
            tracking_ref AS tracking_ref,
            'suspect'   AS source
        FROM
            ppobprod.tb_r_logsuspectdata
        WHERE
            customerid = :customer_id
            AND transactiondate >= :start
    """)

    # async with PPOBAsyncSessionLocal() as session:
    #     result = await session.execute(sql, {
    #         "customer_id": customer_id,
    #         "start": start,
    #     })
    #     rows = result.fetchall()

    result = await session.execute(
        sql,
        {
            "customer_id": customer_id,
            "partner_id": partner_id,
            "start": start,
        },
    )
    rows = result.fetchall()

    tx_rows: List[TxRow] = []
    for r in rows:
        created = ensure_utc(r.created_on)
        tx_rows.append(
            TxRow(
                partner_id=r.partner_id,
                customer_id=r.customer_id,
                switch_id=r.switch_id,
                amount=int(r.amount),
                created_on=created,
                transaction_id=r.transaction_id,
                tracking_ref=r.tracking_ref,
                source=r.source,
            )
        )

    return CustomerHistory(customer_id=customer_id, partner_id=partner_id, rows=tx_rows)
