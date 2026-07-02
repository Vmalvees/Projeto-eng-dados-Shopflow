from pydantic import BaseModel, Field
from datetime import date

class DimDate(BaseModel):
    """Pydantic model representing a date dimension record in the Gold layer."""
    date_key: int = Field(..., description="Surrogate key (YYYYMMDD)")
    full_date: date
    day_of_week: int = Field(..., ge=0, le=6)
    day_name: str = Field(..., max_length=15)
    day_of_month: int = Field(..., ge=1, le=31)
    month: int = Field(..., ge=1, le=12)
    month_name: str = Field(..., max_length=15)
    quarter: int = Field(..., ge=1, le=4)
    year: int
    is_weekend: bool
    is_month_start: bool
    is_month_end: bool
    is_holiday: bool

    class Config:
        from_attributes = True
