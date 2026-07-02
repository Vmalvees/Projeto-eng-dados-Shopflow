from pydantic import BaseModel, Field
from typing import Optional

class FactOrders(BaseModel):
    """Pydantic model representing an order fact record in the Gold layer."""
    order_key: int = Field(..., description="Surrogate key")
    order_id: str = Field(..., max_length=36, description="Natural key (UUID)")
    customer_key: int = Field(..., description="Foreign key to dim_customer")
    product_key: int = Field(..., description="Foreign key to dim_product")
    date_key: int = Field(..., description="Foreign key to dim_date")
    quantity: int = Field(..., gt=0)
    unit_price: float = Field(..., gt=0)
    discount_percent: float = Field(default=0.0, ge=0.0, le=100.0)
    discount_amount: float = Field(default=0.0, ge=0.0)
    total_amount: float = Field(..., ge=0.0)
    net_amount: float = Field(..., ge=0.0)
    shipping_cost: float = Field(default=0.0, ge=0.0)
    payment_method: Optional[str] = Field(None, max_length=50)
    order_status: Optional[str] = Field(None, max_length=50)

    class Config:
        from_attributes = True
