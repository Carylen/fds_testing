# app/core/config.py
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "mysql+aiomysql://root:@localhost:3306/fds",
)

DATABASE_PPOB_URL = os.getenv(
    "DATABASE_PPOB_URL",
    "mysql+aiomysql://user:password@log-host:3306/ppobprod",
)

# Webhook Google Chat
GCHAT_SPACE = os.getenv("GCHAT_SPACE")
# MAX_HISTORY_DAYS = int(os.getenv("FDS_MAX_HISTORY_DAYS", "30"))
MAX_HISTORY_DAYS = 30