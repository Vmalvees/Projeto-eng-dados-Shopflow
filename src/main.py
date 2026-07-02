import argparse
import logging
from pathlib import Path
import pandas as pd
from src.utils.config import get_settings
from src.utils.logger import get_logger
from src.extract.data_generator import EcommerceDataGenerator
from src.extract.api_extractor import ApiExtractor
from src.transform.bronze_to_silver import BronzeToSilver
from src.transform.silver_to_gold import SilverToGold
from src.quality.data_quality_checker import DataQualityChecker
from src.load.s3_loader import S3Loader
from src.load.rds_loader import RDSLoader

# Setup logger
logger = get_logger("etl_pipeline.main")

def run_local_pipeline(volume: int, skip_api: bool, local_only: bool):
    """Orchestrates the entire ETL pipeline flow locally.

    Args:
        volume: Data volume (number of orders) to generate.
        skip_api: If True, skips calling Fake Store API.
        local_only: If True, saves to local disk and skips S3/RDS loads.
    """
    settings = get_settings()
    logger.info("=" * 60)
    logger.info("STARTING E-COMMERCE ETL PIPELINE")
    logger.info(f"Environment: {settings.environment} | Volume: {volume}")
    logger.info("=" * 60)

    # 1. SETUP PATHS
    base_data_path = Path("data")
    raw_dir = base_data_path / "raw"
    clean_dir = base_data_path / "clean"
    gold_dir = base_data_path / "gold"
    reports_dir = base_data_path / "reports"
    
    for d in [raw_dir, clean_dir, gold_dir, reports_dir]:
        d.mkdir(parents=True, exist_ok=True)

    # 2. PHASE 1: EXTRACTION
    logger.info("--- PHASE 1: EXTRACTION ---")
    generator = EcommerceDataGenerator(volume=volume)
    
    # A. Generate customers first
    logger.info("Generating mock customers...")
    customers_raw = generator.generate_customers()
    
    # B. Get products (from API or fallback to generator)
    products_raw = pd.DataFrame()
    if not skip_api:
        logger.info("Fetching products from Fake Store API...")
        api = ApiExtractor()
        products_raw = api.extract_products()
        
        # Save raw api data to raw_dir
        if not products_raw.empty:
            products_raw.to_json(raw_dir / "products_sample.json", orient="records", indent=2)
            logger.info("API Products saved to bronze raw storage.")
            
    if products_raw.empty:
        logger.warning("API data missing or skipped. Generating products...")
        products_raw = generator.generate_products()
        products_raw.to_csv(raw_dir / "products_sample.csv", index=False)

    # C. Generate orders referencing the actual customers and products
    logger.info("Generating mock orders referencing products and customers...")
    orders_raw = generator.generate_orders(customers_raw, products_raw)
    
    # Save raw CSVs to disk for audit
    customers_raw.to_csv(raw_dir / "customers_sample.csv", index=False)
    orders_raw.to_csv(raw_dir / "orders_sample.csv", index=False)

    datasets = {
        "customers": customers_raw,
        "products": products_raw,
        "orders": orders_raw
    }

    # 3. PHASE 2: BRONZE TO SILVER TRANSFORMATION (CLEANING)
    logger.info("--- PHASE 2: BRONZE TO SILVER TRANSFORMATION ---")
    bronze_to_silver = BronzeToSilver(settings)
    silver_data = bronze_to_silver.transform_all(datasets)
    
    # Save clean silver datasets
    bronze_to_silver.save_to_parquet(silver_data, clean_dir)

    # 4. PHASE 3: DATA QUALITY CHECKS
    logger.info("--- PHASE 3: DATA QUALITY CHECKING ---")
    dq = DataQualityChecker()
    dq_results = []
    
    # Run quality suite for customers
    cust_suite = Path("src/quality/expectations/customers_expectations.json")
    if cust_suite.exists() and "customers" in silver_data:
        dq_results.extend(dq.run_suite(silver_data["customers"], cust_suite))
        
    # Run quality suite for products
    prod_suite = Path("src/quality/expectations/products_expectations.json")
    if prod_suite.exists() and "products" in silver_data:
        dq_results.extend(dq.run_suite(silver_data["products"], prod_suite))
        
    # Run quality suite for orders
    order_suite = Path("src/quality/expectations/orders_expectations.json")
    if order_suite.exists() and "orders" in silver_data:
        dq_results.extend(dq.run_suite(silver_data["orders"], order_suite))

    # Generate and print report
    report_path = reports_dir / f"dq_report_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.txt"
    report = dq.generate_report(dq_results, report_path)
    print(report)

    # Check for critical failures (if uniqueness checks on ID fail, we stop)
    critical_failed = [r for r in dq_results if not r.passed and ("uniqueness" in r.check_name or "id" in r.check_name)]
    if critical_failed:
        logger.error(f"Pipeline stopped due to critical Data Quality failures: {[r.check_name for r in critical_failed]}")
        return False

    # 5. PHASE 4: SILVER TO GOLD TRANSFORMATION (DIMENSIONAL MODELING)
    logger.info("--- PHASE 4: SILVER TO GOLD TRANSFORMATION ---")
    silver_to_gold = SilverToGold(settings)
    gold_data = silver_to_gold.create_all(silver_data)
    
    # Save gold tables to local storage
    silver_to_gold.save_to_parquet(gold_data, gold_dir)

    # 6. PHASE 5: LOAD TO S3 AND RDS
    if local_only:
        logger.info("Local-only mode enabled. Skipping S3 and RDS upload.")
    else:
        logger.info("--- PHASE 5: DESTINATION LOADING ---")
        
        # A. Load to S3
        logger.info("Uploading clean Parquet layers to S3...")
        try:
            s3_loader = S3Loader(settings)
            s3_loader.create_bucket_if_not_exists()
            
            for name, df in silver_data.items():
                s3_key = f"silver/{name}/{name}_clean.parquet"
                s3_loader.upload_dataframe(df, s3_key)
                
            for name, df in gold_data.items():
                s3_key = f"gold/{name}/{name}.parquet"
                s3_loader.upload_dataframe(df, s3_key)
        except Exception as e:
            logger.error(f"S3 Load phase encountered an error: {e}. Continuing to database phase.")

        # B. Load to RDS PostgreSQL database
        logger.info("Loading dimension and fact tables to RDS PostgreSQL...")
        try:
            with RDSLoader(settings) as rds:
                if rds.health_check():
                    # Initialize tables
                    schema_sql_path = Path("data/schemas/star_schema.sql")
                    rds.create_tables(schema_sql_path)
                    
                    # Upsert dimensions (dim_date, dim_customer, dim_product)
                    rds.upsert_dataframe(gold_data["dim_date"], "dim_date", ["date_key"])
                    rds.upsert_dataframe(gold_data["dim_customer"], "dim_customer", ["customer_key"])
                    rds.upsert_dataframe(gold_data["dim_product"], "dim_product", ["product_key"])
                    
                    # Fact tables are transactional. In a batch scenario, we append or upsert.
                    # Since order_key is auto-generated and order_id is natural key:
                    rds.upsert_dataframe(gold_data["fact_orders"], "fact_orders", ["order_key"])
                    
                    # Log counts
                    logger.info(f"dim_date count: {rds.get_table_row_count('dim_date')}")
                    logger.info(f"dim_customer count: {rds.get_table_row_count('dim_customer')}")
                    logger.info(f"dim_product count: {rds.get_table_row_count('dim_product')}")
                    logger.info(f"fact_orders count: {rds.get_table_row_count('fact_orders')}")
                else:
                    logger.error("Could not reach RDS. Database loading phase skipped.")
        except Exception as e:
            logger.error(f"RDS Load phase failed: {e}")
            raise

    logger.info("=" * 60)
    logger.info("E-COMMERCE ETL PIPELINE COMPLETED SUCCESSFULLY!")
    logger.info("=" * 60)
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ShopFlow ETL Pipeline locally")
    parser.add_argument("--volume", type=int, default=1000, help="Order volume to process")
    parser.add_argument("--skip-api", action="store_true", help="Skip Fake Store API calls")
    parser.add_argument("--local-only", action="store_true", help="Skip S3/RDS loading, local files only")
    args = parser.parse_args()

    run_local_pipeline(args.volume, args.skip_api, args.local_only)
