# app/fds/schemas.py
from enum import Enum
from typing import Optional, List, Dict, Any

from pydantic import BaseModel, Field
from datetime import datetime

from app.core.time_utils import utc_now


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
    timestamp: Optional[datetime] = Field(default_factory=utc_now)


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
    triggered_rules: List[Dict[str, Any]] = Field(default_factory=list)
    processing_time_ms: float
    metadata: Dict[str, Any] = Field(default_factory=dict)
