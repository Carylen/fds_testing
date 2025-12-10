# app/api/routes.py
from fastapi import APIRouter, BackgroundTasks, HTTPException

from app.fds.schemas import (
    TransactionRequest,
    FDSResult,
    FraudRule,
    ProductType,
    TimeWindow,
)
from app.fds.rules import rule_engine
from app.fds.storage import storage
from app.fds.notifications import notification_service
from app.fds.services import log_fraud_event_to_db

router = APIRouter(prefix="/fds", tags=["fds"])


@router.post("/check", response_model=FDSResult)
async def check_transaction(
    transaction: TransactionRequest,
    background_tasks: BackgroundTasks,
):
    """
    Check transaction against fraud rules using sliding window.
    Target: < 200ms processing time.
    """
    result = await rule_engine.check_transaction(transaction)
    
    if result.action != result.action.ALLOW:
        background_tasks.add_task(
            notification_service.send_notification,
            result,
            transaction,
        )
        background_tasks.add_task(
            log_fraud_event_to_db,
            transaction,
            result,
        )
    
    return result


@router.get("/rules", response_model=list[FraudRule])
async def get_rules():
    """Get all fraud rules (read-only)."""
    return rule_engine.rules


@router.post("/rules", response_model=FraudRule)
async def create_rule(rule: FraudRule):
    """Create new fraud rule (instantly applied)."""
    await rule_engine.add_rule(rule)
    return rule


@router.put("/rules/{rule_id}", response_model=FraudRule)
async def update_rule(rule_id: str, rule: FraudRule):
    """Update existing fraud rule (instantly applied)."""
    updated = await rule_engine.update_rule(rule_id, rule)
    if not updated:
        raise HTTPException(status_code=404, detail="Rule not found")
    return rule


@router.delete("/rules/{rule_id}")
async def delete_rule(rule_id: str):
    """Delete fraud rule (instantly applied)."""
    deleted = await rule_engine.delete_rule(rule_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Rule not found")
    return {"message": "Rule deleted successfully"}


@router.get("/health")
async def health_check():
    """Health check endpoint."""
    stats = await storage.get_stats()
    return {
        "status": "healthy",
        "storage": "in-memory",
        "rules_count": len(rule_engine.rules),
        "storage_stats": stats,
    }


@router.get("/stats/{customer_id}")
async def get_customer_stats(customer_id: str):
    """Get customer transaction statistics with sliding windows."""
    stats: dict[str, dict[str, int]] = {}
    
    for window in TimeWindow:
        stats[window.value] = {}
        for product_type in ProductType:
            count = await storage.get_transaction_count(
                customer_id=customer_id,
                window=window,
                product_type=product_type,
            )
            stats[window.value][product_type.value] = count
    
    return {
        "customer_id": customer_id,
        "windows": stats,
    }


@router.get("/customer/{customer_id}/history")
async def get_customer_history(
    customer_id: str,
    window: TimeWindow = TimeWindow.DAY,
    product_type: ProductType | None = None,
):
    """Get customer transaction history."""
    transactions = await storage.get_transactions_in_window(
        customer_id=customer_id,
        window=window,
        product_type=product_type,
    )
    
    return {
        "customer_id": customer_id,
        "window": window,
        "product_type": product_type,
        "count": len(transactions),
        "transactions": [t.dict() for t in transactions],
    }


@router.get("/storage/stats")
async def get_storage_stats():
    """Get storage statistics."""
    return await storage.get_stats()


@router.post("/storage/cleanup")
async def manual_cleanup():
    """Manually trigger cleanup of old transactions."""
    removed = await storage._clean_all_old_transactions()
    return {
        "message": "Cleanup completed",
        "transactions_removed": removed,
        "retention_days": storage.max_history_days,
    }
