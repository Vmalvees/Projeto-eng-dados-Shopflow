import logging
from pathlib import Path
import pandas as pd
from src.transform import transformers as t

logger = logging.getLogger("etl_pipeline.transform.bronze_to_silver")

class BronzeToSilver:
    """Transforms raw bronze data into clean, typed, and deduplicated silver data."""

    def __init__(self, config=None):
        """Initializes the BronzeToSilver transformer.

        Args:
            config: Optional Settings config object.
        """
        self.config = config

    def transform_customers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans and normalizes customer data.

        Args:
            df: Raw customer DataFrame.

        Returns:
            Cleaned customers DataFrame.
        """
        if df.empty:
            logger.warning("Empty customers DataFrame received for transformation.")
            return df

        logger.info(f"Transforming {len(df)} customers from Bronze to Silver...")
        
        # 1. Deduplicate by email (email should be unique per customer)
        df = t.remove_duplicates(df, subset=["email"], keep="first")
        
        # 2. Normalize and standardize names
        df = t.normalize_names(df, ["first_name", "last_name"])
        
        # 3. Clean and validate emails (nullify invalid ones)
        df = t.clean_email(df, "email")
        
        # 4. Fill missing phone numbers with 'N/A'
        df = t.handle_nulls(df, {"phone": "fill_value:N/A"})
        
        # 5. Standardize string columns
        df = t.standardize_strings(df, ["segment", "state"])
        
        # 6. Validate segment values
        valid_segments = ["b2c", "b2b", "premium", "enterprise"]
        def clean_segment(val):
            val_str = str(val).lower().strip()
            return val_str if val_str in valid_segments else "b2c"
            
        df["segment"] = df["segment"].apply(clean_segment)
        
        # 7. Parse registration dates
        df = t.parse_dates(df, ["registration_date"])
        
        # 8. Add processing timestamp
        df["processed_at"] = pd.Timestamp.now()
        
        logger.info(f"Customers Bronze->Silver transformation complete. Row count: {len(df)}")
        return df

    def transform_products(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans and normalizes product data.

        Args:
            df: Raw product DataFrame.

        Returns:
            Cleaned products DataFrame.
        """
        if df.empty:
            logger.warning("Empty products DataFrame received for transformation.")
            return df

        logger.info(f"Transforming {len(df)} products from Bronze to Silver...")
        
        # 1. Deduplicate by product_id
        df = t.remove_duplicates(df, subset=["product_id"], keep="first")
        
        # 2. Fix negative prices and cost prices (take absolute values)
        df = t.fix_negative_values(df, ["price", "cost_price"], strategy="absolute")
        
        # 3. Round currency columns
        df = t.round_currency(df, ["price", "cost_price"], decimals=2)
        
        # 4. Handle null values (fill null stock with 0)
        df = t.handle_nulls(df, {"stock_quantity": "fill_value:0"})
        df["stock_quantity"] = df["stock_quantity"].astype("int64")
        
        # 5. Standardize category names
        df = t.normalize_names(df, ["category", "subcategory"])
        
        # 6. Ensure price is greater than cost_price. If not, set cost_price to 70% of price
        def adjust_cost_price(row):
            price = row["price"]
            cost = row["cost_price"]
            if cost >= price or cost <= 0:
                return round(price * 0.7, 2)
            return cost
            
        df["cost_price"] = df.apply(adjust_cost_price, axis=1)
        
        # 7. Parse created_at dates
        df = t.parse_dates(df, ["created_at"])
        
        # 8. Add processing timestamp
        df["processed_at"] = pd.Timestamp.now()
        
        logger.info(f"Products Bronze->Silver transformation complete. Row count: {len(df)}")
        return df

    def transform_orders(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans, recalculates, and normalizes order data.

        Args:
            df: Raw orders DataFrame.

        Returns:
            Cleaned orders DataFrame.
        """
        if df.empty:
            logger.warning("Empty orders DataFrame received for transformation.")
            return df

        logger.info(f"Transforming {len(df)} orders from Bronze to Silver...")
        
        # 1. Deduplicate by order_id
        df = t.remove_duplicates(df, subset=["order_id"], keep="first")
        
        # 2. Standardize status values (all lowercase)
        df = t.standardize_strings(df, ["status", "payment_method"])
        
        # Map statuses to standard values
        status_map = {
            "completed": "completed",
            "pending": "pending",
            "shipped": "shipped",
            "cancelled": "cancelled",
            "returned": "returned"
        }
        df["status"] = df["status"].apply(lambda x: status_map.get(str(x).lower().strip(), "pending"))
        
        # 3. Handle date fields (parse and drop future dates)
        df = t.parse_dates(df, ["order_date"])
        df = t.remove_future_dates(df, "order_date")
        
        # 4. Filter out invalid quantities (must be > 0)
        invalid_qty = (df["quantity"] <= 0) | df["quantity"].isnull()
        if invalid_qty.sum() > 0:
            logger.warning(f"Removing {invalid_qty.sum()} orders with quantity <= 0 or null.")
            df = df[~invalid_qty]
            
        # 5. Fix unit price negative values
        df = t.fix_negative_values(df, ["unit_price"], strategy="absolute")
        df = t.round_currency(df, ["unit_price", "discount_percent", "shipping_cost"], decimals=2)
        
        # 6. Recalculate total_amount based on quantity, unit price, and discount
        # formula: quantity * unit_price * (1 - discount_percent / 100)
        df["total_amount"] = df.apply(
            lambda r: round(r["quantity"] * r["unit_price"] * (1 - r["discount_percent"] / 100), 2),
            axis=1
        )
        
        # 7. Add processing timestamp
        df["processed_at"] = pd.Timestamp.now()
        
        logger.info(f"Orders Bronze->Silver transformation complete. Row count: {len(df)}")
        return df

    def transform_all(self, datasets: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        """Transforms all datasets from Bronze to Silver.

        Args:
            datasets: Dict mapping dataset name ('customers', 'products', 'orders') to DataFrame.

        Returns:
            Dict mapping dataset name to cleaned DataFrame.
        """
        transformed = {}
        
        if "customers" in datasets:
            transformed["customers"] = self.transform_customers(datasets["customers"])
        if "products" in datasets:
            transformed["products"] = self.transform_products(datasets["products"])
        if "orders" in datasets:
            # We transform orders using cleaned customer and product IDs to ensure referential integrity
            orders_df = datasets["orders"]
            
            if "customers" in transformed:
                orders_df = t.validate_foreign_keys(orders_df, "customer_id", transformed["customers"], "customer_id")
            if "products" in transformed:
                orders_df = t.validate_foreign_keys(orders_df, "product_id", transformed["products"], "product_id")
                
            transformed["orders"] = self.transform_orders(orders_df)
            
        return transformed

    def save_to_parquet(self, datasets: dict[str, pd.DataFrame], output_dir: Path) -> None:
        """Saves clean datasets to Parquet files with Snappy compression.

        Args:
            datasets: Transformed datasets.
            output_dir: Path to directory where Parquet files should be written.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        
        for name, df in datasets.items():
            if df.empty:
                logger.warning(f"Skipping writing empty DataFrame '{name}' to Parquet.")
                continue
                
            output_path = output_dir / f"{name}_clean.parquet"
            logger.info(f"Saving '{name}' to Parquet format at {output_path}...")
            
            try:
                # Save with snappy compression, compatible with AWS Glue/Athena
                df.to_parquet(output_path, compression="snappy", index=False)
                logger.info(f"Successfully saved '{name}' parquet file.")
            except Exception as e:
                logger.error(f"Failed to write Parquet file for '{name}': {e}")
                raise
