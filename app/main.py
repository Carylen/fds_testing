# app/main.py
from contextlib import asynccontextmanager, suppress

import asyncio
from fastapi import FastAPI

from app.api.routes import router as fds_router
from app.db.session import init_db
from app.fds.storage import storage


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup & shutdown: init DB + start/stop cleanup loop."""
    # Init DB (create tables)
    await init_db()
    print("✅ DB initialized (fraud_events table ready)")
    
    # Start periodic cleanup (1 jam)
    cleanup_task = asyncio.create_task(storage.cleanup_loop(interval_seconds=3600))
    print("✅ FDS System Started - Cleanup job running every 1 hour")
    try:
        yield
    finally:
        cleanup_task.cancel()
        with suppress(asyncio.CancelledError):
            await cleanup_task
        print("✅ FDS System Stopped")


app = FastAPI(
    title="FDS Rule-Based System - Sliding Window",
    description="Fraud Detection System with In-Memory Sliding Window (1m, 1d, 1M) + Async SQL logging",
    version="3.0.0",
    lifespan=lifespan,
)

app.include_router(fds_router)


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
