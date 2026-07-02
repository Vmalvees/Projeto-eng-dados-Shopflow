from pydantic import BaseModel, Field
from datetime import date
from typing import Optional

class DimCustomer(BaseModel):
    """Pydantic model representing a customer dimension record in the Gold layer."""
    customer_key: int = Field(..., description="Surrogate key")
    customer_id: str = Field(..., max_length=36, description="Natural key (UUID)")
    first_name: str = Field(..., max_length=100)
    last_name: str = Field(..., max_length=100)
    email: Optional[str] = Field(None, max_length=255)
    phone: Optional[str] = Field(None, max_length=50)
    address: Optional[str] = Field(None, max_length=255)
    city: Optional[str] = Field(None, max_length=100)
    state: Optional[str] = Field(None, max_length=50)
    segment: Optional[str] = Field(None, max_length=50)
    valid_from: date
    valid_to: date
    is_current: bool
class Config:
        from_attributes = True
