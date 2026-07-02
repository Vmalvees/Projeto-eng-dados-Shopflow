from pydantic import BaseModel, Field
from typing import Optional

class DimProduct(BaseModel):
    """Pydantic model representing a product dimension record in the Gold layer."""
    product_key: int = Field(..., description="Surrogate key")
    product_id: str = Field(..., max_length=36, description="Natural key (UUID)")
    name: str = Field(..., max_length=255)
    category: str = Field(..., max_length=100)
    subcategory: str = Field(..., max_length=100)
    price: float = Field(..., gt=0)
    cost_price: float = Field(..., gt=0)
    price_range: str = Field(..., max_length=50)
    margin_percent: float
    supplier: Optional[str] = Field(None, max_length=255)
    rating: Optional[float] = Field(None, ge=1.0, le=5.0)

    class Config:
        from_attributes = True
