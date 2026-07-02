import logging
import time
import requests
import pandas as pd
from typing import List, Dict, Any, Optional

logger = logging.getLogger("etl_pipeline.extractor.api")

class ApiExtractor:
    """Extracts data from public REST APIs (like Fake Store API) with robust handling."""

    def __init__(self, rate_limit_pause_seconds: float = 1.0):
        """Initializes the API extractor.

        Args:
            rate_limit_pause_seconds: Time to sleep between API calls to avoid rate limits.
        """
        self.base_url = "https://fakestoreapi.com"
        self.rate_limit_pause = rate_limit_pause_seconds

    def _make_request(self, endpoint: str, retries: int = 3, backoff: float = 2.0) -> List[Dict[str, Any]]:
        """Makes an HTTP request with retry logic and exponential backoff.

        Args:
            endpoint: API endpoint path (e.g. '/products').
            retries: Number of retry attempts.
            backoff: Exponential backoff factor.

        Returns:
            JSON response parsed into a list or dict.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        attempt = 0
        
        while attempt <= retries:
            try:
                logger.info(f"Requesting {url} (Attempt {attempt + 1}/{retries + 1})")
                response = requests.get(url, timeout=15)
                
                # Check for rate limiting or server errors
                response.raise_for_status()
                
                time.sleep(self.rate_limit_pause)
                return response.json()
                
            except requests.exceptions.RequestException as e:
                attempt += 1
                if attempt > retries:
                    logger.error(f"Failed to fetch {url} after {retries + 1} attempts. Error: {e}")
                    raise
                
                sleep_time = backoff ** attempt
                logger.warning(f"Request failed: {e}. Retrying in {sleep_time:.2f}s...")
                time.sleep(sleep_time)
        
        raise RuntimeError(f"Unexpected termination of request loop for {url}")

    def extract_products(self) -> pd.DataFrame:
        """Extracts products dataset from the Fake Store API.

        Returns:
            pd.DataFrame containing products.
        """
        try:
            logger.info("Extracting products from Fake Store API...")
            products_data = self._make_request("products")
            df = pd.DataFrame(products_data)
            
            # Simple flattening/validation of fields if nested (like 'rating' which is a dict)
            if not df.empty:
                if "rating" in df.columns:
                    # Extract rating rate and count into separate columns
                    df["rating"] = df["rating"].apply(lambda x: x.get("rate") if isinstance(x, dict) else None)
                
                # Align API schema with expected products schema
                rename_map = {
                    "id": "product_id",
                    "title": "name"
                }
                df = df.rename(columns=rename_map)
                
                # Cast product_id to string to match UUID type from generator
                df["product_id"] = df["product_id"].astype(str)
                
                # Fill in missing columns expected by pipeline
                if "cost_price" not in df.columns:
                    df["cost_price"] = (df["price"] * 0.65).round(2)
                if "stock_quantity" not in df.columns:
                    df["stock_quantity"] = 150
                if "category" not in df.columns:
                    df["category"] = "Uncategorized"
                if "subcategory" not in df.columns:
                    df["subcategory"] = df["category"]
                if "supplier" not in df.columns:
                    df["supplier"] = "FakeStoreAPI"
                if "sku" not in df.columns:
                    df["sku"] = df["product_id"].apply(lambda x: f"API-PRD-{x}")
                if "created_at" not in df.columns:
                    df["created_at"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

            logger.info(f"Successfully extracted {len(df)} products from API.")
            return df
        except Exception as e:
            logger.error(f"Failed to extract products from API: {e}")
            # Return empty DataFrame as fallback so pipeline doesn't crash completely
            return pd.DataFrame()

    def extract_categories(self) -> List[str]:
        """Extracts product categories from Fake Store API.

        Returns:
            List of category strings.
        """
        try:
            logger.info("Extracting product categories from Fake Store API...")
            categories = self._make_request("products/categories")
            return [str(cat) for cat in categories]
        except Exception as e:
            logger.error(f"Failed to extract categories from API: {e}")
            return []

    def extract_users(self) -> pd.DataFrame:
        """Extracts user (customer) data from Fake Store API.

        Returns:
            pd.DataFrame of users.
        """
        try:
            logger.info("Extracting users from Fake Store API...")
            users_data = self._make_request("users")
            df = pd.DataFrame(users_data)
            logger.info(f"Successfully extracted {len(df)} users from API.")
            return df
        except Exception as e:
            logger.error(f"Failed to extract users from API: {e}")
            return pd.DataFrame()
