from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from pydantic import model_validator


class PriceTrackerData(BaseModel):
    symbol: str
    bid: float
    bid_qty: float
    ask_qty: float
    last: float
    volume: float
    low: float
    high: float
    timestamp: datetime


class PriceTrackerMessage(BaseModel):
    channel: Optional[str] = None
    data: Optional[list[dict]] = None
    error: Optional[str] = None

    # add post validation to get symbol, bid, bid_qty, ask_qty, last, volume, low, high, timestamp
    @model_validator(mode="after")
    def validate_data(self):
        if self.data is not None:
            for item in self.data:
                if not isinstance(item, dict):
                    raise ValueError("Data must be a list of dictionaries")
                PriceTrackerData(**item)  # validate structure
        return self
