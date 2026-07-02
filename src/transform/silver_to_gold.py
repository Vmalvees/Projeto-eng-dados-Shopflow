import logging
from pathlib import Path
import pandas as pd
import numpy as np

logger = logging.getLogger("etl_pipeline.transform.silver_to_gold")

class SilverToGold:
    """Transforms clean silver datasets into structured Gold dimension and fact tables."""

    def __init__(self, config=None):
        """Initializes the SilverToGold transformer.

        Args:
            config: Optional Settings config object.
        """
        self.config = config

    def create_dim_date(self, start_date: str = "2023-01-01", end_date: str = "2027-12-31") -> pd.DataFrame:
        """Generates a complete date dimension table.

        Args:
            start_date: Start of date range (YYYY-MM-DD).
            end_date: End of date range (YYYY-MM-DD).

        Returns:
            pd.DataFrame representing dim_date.
        """
        logger.info(f"Generating date dimension from {start_date} to {end_date}...")
        dates = pd.date_range(start=start_date, end=end_date, freq="D")
        
        df = pd.DataFrame({"full_date": dates})
        
        # 1. Generate keys and attributes
        df["date_key"] = df["full_date"].dt.strftime("%Y%m%d").astype(int)
        df["day_of_week"] = df["full_date"].dt.dayofweek  # 0=Monday, 6=Sunday
        df["day_name"] = df["full_date"].dt.day_name()
        df["day_of_month"] = df["full_date"].dt.day
        df["month"] = df["full_date"].dt.month
        df["month_name"] = df["full_date"].dt.month_name()
        df["quarter"] = df["full_date"].dt.quarter
        df["year"] = df["full_date"].dt.year
        df["is_weekend"] = df["day_of_week"].isin([5, 6])
        df["is_month_start"] = df["full_date"].dt.is_month_start
        df["is_month_end"] = df["full_date"].dt.is_month_end
        
        # We can also hardcode some Brazilian holidays for demonstration purposes
        # (New Year, Labor Day, Independence Day, Republic Day, Christmas)
        def check_holiday(d):
            # Format: (month, day)
            holidays = [(1, 1), (5, 1), (9, 7), (10, 12), (11, 2), (11, 15), (12, 25)]
            return (d.month, d.day) in holidays

        df["is_holiday"] = df["full_date"].apply(check_holiday)
        
        logger.info(f"Generated dim_date with {len(df)} rows.")
        return df

    def create_dim_customer(self, customers_df: pd.DataFrame) -> pd.DataFrame:
        """Transforms clean customers into dim_customer using SCD Type 2 format.

        Args:
            customers_df: Cleaned customers DataFrame.

        Returns:
            Customers dimension DataFrame.
        """
        logger.info("Creating dim_customer dimension...")
        if customers_df.empty:
            return pd.DataFrame()

        df = customers_df.copy()
        
        # 1. Generate auto-increment surrogate keys
        df = df.reset_index(drop=True)
        df["customer_key"] = df.index + 1
        
        # 2. Re-arrange columns for dimension
        dim_cols = [
            "customer_key", "customer_id", "first_name", "last_name", 
            "email", "phone", "address", "city", "state", "segment"
        ]
        df = df[dim_cols]
        
        # 3. Add SCD Type 2 tracking fields
        # In a real pipeline, we would merge with existing data warehouse records.
        # Since this is a batch load, we initialize everything as active.
        df["valid_from"] = pd.Timestamp("2024-01-01").date()
        df["valid_to"] = pd.Timestamp("9999-12-31").date()
        df["is_current"] = True
        
        logger.info(f"Created dim_customer with {len(df)} rows.")
        return df

    def create_dim_product(self, products_df: pd.DataFrame) -> pd.DataFrame:
        """Transforms clean products into dim_product.

        Args:
            products_df: Cleaned products DataFrame.

        Returns:
            Products dimension DataFrame.
        """
        logger.info("Creating dim_product dimension...")
        if products_df.empty:
            return pd.DataFrame()

        df = products_df.copy()
        
        # 1. Generate auto-increment surrogate keys
        df = df.reset_index(drop=True)
        df["product_key"] = df.index + 1
        
        # 2. Add derived analytics columns
        # Price range buckets
        def calculate_price_range(price):
            if price <= 50.0:
                return "Budget"
            elif price <= 250.0:
                return "Mid-Range"
            elif price <= 1000.0:
                return "Premium"
            return "Luxury"
            
        df["price_range"] = df["price"].apply(calculate_price_range)
        
        # Margin percentage: (price - cost_price) / price
        df["margin_percent"] = df.apply(
            lambda r: round(((r["price"] - r["cost_price"]) / r["price"]) * 100, 2) if r["price"] > 0 else 0.0,
            axis=1
        )
        
        # 3. Re-arrange columns for dimension
        dim_cols = [
            "product_key", "product_id", "name", "category", "subcategory", 
            "price", "cost_price", "price_range", "margin_percent", "supplier", "rating"
        ]
        df = df[dim_cols]
        
        logger.info(f"Created dim_product with {len(df)} rows.")
        return df

    def create_fact_orders(self, orders_df: pd.DataFrame, dim_customer: pd.DataFrame, 
                           dim_product: pd.DataFrame, dim_date: pd.DataFrame) -> pd.DataFrame:
        """Generates fact_orders table by joining orders with dimension surrogate keys.

        Args:
            orders_df: Cleaned orders DataFrame.
            dim_customer: Customer dimension table.
            dim_product: Product dimension table.
            dim_date: Date dimension table.

        Returns:
            Orders fact DataFrame.
        """
        logger.info("Creating fact_orders fact table...")
        if orders_df.empty:
            return pd.DataFrame()

        # Join to get customer surrogate key
        merged = orders_df.merge(
            dim_customer[["customer_key", "customer_id"]],
            on="customer_id",
            how="left"
        )
        
        # Join to get product surrogate key
        merged = merged.merge(
            dim_product[["product_key", "product_id"]],
            on="product_id",
            how="left"
        )
        
        # Extract date key from order date (YYYYMMDD)
        merged["date_key"] = pd.to_datetime(merged["order_date"]).dt.strftime("%Y%m%d").astype(int)
        
        # Validate that date keys exist in dim_date, if not fallback to default date key or drop
        valid_date_keys = set(dim_date["date_key"].unique())
        merged["date_key"] = merged["date_key"].apply(lambda x: x if x in valid_date_keys else 19000101)

        # 1. Fill missing keys with sentinel values (typically 0 or -1 in DW patterns)
        merged["customer_key"] = merged["customer_key"].fillna(-1).astype(int)
        merged["product_key"] = merged["product_key"].fillna(-1).astype(int)
        
        # 2. Add derived financial metrics
        # discount_amount = quantity * unit_price * (discount_percent / 100)
        # net_amount = total_amount
        merged["discount_amount"] = merged.apply(
            lambda r: round(r["quantity"] * r["unit_price"] * (r["discount_percent"] / 100), 2),
            axis=1
        )
        merged["net_amount"] = merged["total_amount"]
        
        # 3. Create auto-increment order_key
        merged = merged.reset_index(drop=True)
        merged["order_key"] = merged.index + 1
        
        # 4. Re-arrange and select final columns for fact table
        fact_cols = [
            "order_key", "order_id", "customer_key", "product_key", "date_key", 
            "quantity", "unit_price", "discount_percent", "discount_amount", 
            "total_amount", "net_amount", "shipping_cost", "payment_method", "status"
        ]
        
        # Rename status to order_status to match the target DDL
        fact_df = merged[fact_cols].rename(columns={"status": "order_status"})
        
        logger.info(f"Created fact_orders with {len(fact_df)} rows. (Orphan customers: {(fact_df['customer_key'] == -1).sum()}, Orphan products: {(fact_df['product_key'] == -1).sum()})")
        return fact_df

    def create_all(self, silver_data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        """Transforms all silver data into gold tables.

        Args:
            silver_data: Dict of clean DataFrames.

        Returns:
            Dict of Gold dimension and fact DataFrames.
        """
        gold_data = {}
        
        # 1. Create independent dimensions
        gold_data["dim_date"] = self.create_dim_date()
        
        if "customers" in silver_data:
            gold_data["dim_customer"] = self.create_dim_customer(silver_data["customers"])
            
        if "products" in silver_data:
            gold_data["dim_product"] = self.create_dim_product(silver_data["products"])
            
        # 2. Create fact tables (requires dimensions)
        if "orders" in silver_data and "dim_customer" in gold_data and "dim_product" in gold_data:
            gold_data["fact_orders"] = self.create_fact_orders(
                silver_data["orders"],
                gold_data["dim_customer"],
                gold_data["dim_product"],
                gold_data["dim_date"]
            )
            
        return gold_data

    def save_to_parquet(self, datasets: dict[str, pd.DataFrame], output_dir: Path) -> None:
        """Saves Gold dimensional tables to Parquet.

        Args:
            datasets: Transformed Gold datasets.
            output_dir: Path to gold data directory.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        for name, df in datasets.items():
            if df.empty:
                continue
            output_path = output_dir / f"{name}.parquet"
            logger.info(f"Saving Gold table '{name}' to {output_path}...")
            df.to_parquet(output_path, compression="snappy", index=False)
