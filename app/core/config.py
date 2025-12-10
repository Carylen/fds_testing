# app/core/config.py
import os
from dotenv import load_dotenv

load_dotenv()

# URL DB async SQLAlchemy (sesuaikan sendiri)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    # "postgresql+asyncpg://user:password@localhost:5432/fds_db",
)

# Webhook Google Chat
GCHAT_SPACE = os.getenv("GCHAT_SPACE")
