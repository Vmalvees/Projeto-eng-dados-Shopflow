import logging
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException

# Airflow logging setup
logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
}

def extract_generate_data(**kwargs):
    """Airflow task to generate and save mock sales/customer CSV files."""
    try:
        from src.extract.data_generator import EcommerceDataGenerator
        from src.utils.config import get_settings
        
        settings = get_settings()
        volume = settings.data_volume
        
        logger.info(f"Generating mock data. Scale volume: {volume}")
        
        # In Airflow Docker Compose, data directory is mounted to /opt/airflow/data
        output_dir = Path("/opt/airflow/data/raw")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        generator = EcommerceDataGenerator(volume=volume)
        generator.save_to_csv(output_dir)
        
        logger.info("CSV mock data generation complete.")
        return str(output_dir)
    except Exception as e:
        logger.error(f"Error in extract_generate_data: {e}")
        raise AirflowException(e)

def extract_api_data(**kwargs):
    """Airflow task to extract product catalog from API."""
    try:
        from src.extract.api_extractor import ApiExtractor
        from src.utils.config import get_settings
        
        logger.info("Fetching products from Fake Store API...")
        extractor = ApiExtractor()
        products_df = extractor.extract_products()
        
        output_dir = Path("/opt/airflow/data/raw")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save raw api data
        if not products_df.empty:
            products_df.to_json(output_dir / "products_sample.json", orient="records", indent=2)
            logger.info(f"API Extraction complete. Saved {len(products_df)} products.")
            return len(products_df)
        else:
            logger.warning("API returned no product data.")
            return 0
    except Exception as e:
        logger.error(f"Error in extract_api_data: {e}")
        raise AirflowException(e)

def transform_bronze_to_silver(**kwargs):
    """Airflow task to clean Bronze raw data and write to Silver Parquet files."""
    try:
        from src.transform.bronze_to_silver import BronzeToSilver
        from src.utils.config import get_settings
        
        settings = get_settings()
        raw_dir = Path("/opt/airflow/data/raw")
        silver_dir = Path("/opt/airflow/data/clean")
        
        # Load raw files
        cust_df = pd.read_csv(raw_dir / "customers_sample.csv")
        orders_df = pd.read_csv(raw_dir / "orders_sample.csv")
        
        # API file might be JSON. If API failed, fallback to CSV products generated
        api_path = raw_dir / "products_sample.json"
        if api_path.exists():
            prod_df = pd.read_json(api_path)
            # Flatten rating if not already done by ApiExtractor
            if not prod_df.empty and "rating" in prod_df.columns:
                prod_df["rating_rate"] = prod_df["rating"].apply(lambda x: x.get("rate") if isinstance(x, dict) else None)
                prod_df = prod_df.drop(columns=["rating"])
        else:
            prod_df = pd.read_csv(raw_dir / "products_sample.csv")

        datasets = {
            "customers": cust_df,
            "products": prod_df,
            "orders": orders_df
        }

        # Transform
        bronze_to_silver = BronzeToSilver(settings)
        silver_data = bronze_to_silver.transform_all(datasets)
        
        # Save clean silver datasets
        bronze_to_silver.save_to_parquet(silver_data, silver_dir)
        logger.info("Bronze to Silver cleaning complete.")
    except Exception as e:
        logger.error(f"Error in transform_bronze_to_silver: {e}")
        raise AirflowException(e)

def run_data_quality_checks(**kwargs):
    """Airflow task to perform data quality checks and validations."""
    try:
        from src.quality.data_quality_checker import DataQualityChecker
        
        silver_dir = Path("/opt/airflow/data/clean")
        reports_dir = Path("/opt/airflow/data/reports")
        reports_dir.mkdir(parents=True, exist_ok=True)
        
        # Load clean Parquet files
        cust_df = pd.read_parquet(silver_dir / "customers_clean.parquet")
        prod_df = pd.read_parquet(silver_dir / "products_clean.parquet")
        orders_df = pd.read_parquet(silver_dir / "orders_clean.parquet")
        
        dq = DataQualityChecker()
        results = []
        
        # Run expectations
        results.extend(dq.run_suite(cust_df, Path("/opt/airflow/src/quality/expectations/customers_expectations.json")))
        results.extend(dq.run_suite(prod_df, Path("/opt/airflow/src/quality/expectations/products_expectations.json")))
        results.extend(dq.run_suite(orders_df, Path("/opt/airflow/src/quality/expectations/orders_expectations.json")))
        
        # Report
        report_path = reports_dir / f"dq_report_airflow_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.txt"
        dq.generate_report(results, report_path)
        
        # Evaluate failures
        failed = [r for r in results if not r.passed]
        critical_failed = [r for r in failed if "uniqueness" in r.check_name or "id" in r.check_name]
        
        if critical_failed:
            err_msg = f"Data Quality Check failed. Critical failures: {[r.check_name for r in critical_failed]}"
            logger.error(err_msg)
            raise ValueError(err_msg)
            
        logger.info(f"Data Quality Check complete. All checks passed: {len(results) - len(failed)}/{len(results)}")
    except Exception as e:
        logger.error(f"Error in run_data_quality_checks: {e}")
        raise AirflowException(e)

def transform_silver_to_gold(**kwargs):
    """Airflow task to transform silver data into structured gold dimensions/fact."""
    try:
        from src.transform.silver_to_gold import SilverToGold
        from src.utils.config import get_settings
        
        settings = get_settings()
        silver_dir = Path("/opt/airflow/data/clean")
        gold_dir = Path("/opt/airflow/data/gold")
        
        # Load clean Parquet files
        silver_data = {
            "customers": pd.read_parquet(silver_dir / "customers_clean.parquet"),
            "products": pd.read_parquet(silver_dir / "products_clean.parquet"),
            "orders": pd.read_parquet(silver_dir / "orders_clean.parquet")
        }
        
        silver_to_gold = SilverToGold(settings)
        gold_data = silver_to_gold.create_all(silver_data)
        
        # Save gold tables to local storage
        silver_to_gold.save_to_parquet(gold_data, gold_dir)
        logger.info("Silver to Gold dimensional transformation complete.")
    except Exception as e:
        logger.error(f"Error in transform_silver_to_gold: {e}")
        raise AirflowException(e)

def load_to_s3(**kwargs):
    """Airflow task to upload Parquet datasets to S3/MinIO storage."""
    try:
        from src.load.s3_loader import S3Loader
        from src.utils.config import get_settings
        
        settings = get_settings()
        silver_dir = Path("/opt/airflow/data/clean")
        gold_dir = Path("/opt/airflow/data/gold")
        
        s3_loader = S3Loader(settings)
        s3_loader.create_bucket_if_not_exists()
        
        # Upload Silver clean files
        for f in silver_dir.glob("*.parquet"):
            s3_key = f"silver/{f.stem.replace('_clean', '')}/{f.name}"
            s3_loader.upload_file(f, s3_key)
            
        # Upload Gold DW files
        for f in gold_dir.glob("*.parquet"):
            s3_key = f"gold/{f.stem}/{f.name}"
            s3_loader.upload_file(f, s3_key)
            
        logger.info("S3 storage load phase complete.")
    except Exception as e:
        logger.error(f"Error in load_to_s3: {e}")
        raise AirflowException(e)

def load_to_rds(**kwargs):
    """Airflow task to load and upsert Gold dimensional datasets into PostgreSQL DWH."""
    try:
        from src.load.rds_loader import RDSLoader
        from src.utils.config import get_settings
        
        settings = get_settings()
        gold_dir = Path("/opt/airflow/data/gold")
        
        # Load parquet datasets into dataframes for loading
        dim_date = pd.read_parquet(gold_dir / "dim_date.parquet")
        dim_customer = pd.read_parquet(gold_dir / "dim_customer.parquet")
        dim_product = pd.read_parquet(gold_dir / "dim_product.parquet")
        fact_orders = pd.read_parquet(gold_dir / "fact_orders.parquet")
        
        with RDSLoader(settings) as rds:
            if not rds.health_check():
                raise ConnectionError("Database RDS check failed.")
                
            # Execute schemas DDL
            schema_path = Path("/opt/airflow/data/schemas/star_schema.sql")
            rds.create_tables(schema_path)
            
            # Upsert Gold datasets
            rds.upsert_dataframe(dim_date, "dim_date", ["date_key"])
            rds.upsert_dataframe(dim_customer, "dim_customer", ["customer_key"])
            rds.upsert_dataframe(dim_product, "dim_product", ["product_key"])
            rds.upsert_dataframe(fact_orders, "fact_orders", ["order_key"])
            
            logger.info("DW PostgreSQL database load phase complete.")
    except Exception as e:
        logger.error(f"Error in load_to_rds: {e}")
        raise AirflowException(e)

# DAG Definition
with DAG(
    dag_id="ecommerce_etl_pipeline",
    default_args=default_args,
    description="End-to-end batch ETL pipeline for e-commerce transactions analytics",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "medallion", "s3", "rds"],
) as dag:

    start_task = EmptyOperator(task_id="start")

    with TaskGroup("extraction_group", tooltip="Extract raw data sources") as extraction_group:
        task_gen_csv = PythonOperator(
            task_id="generate_transactions_csv",
            python_callable=extract_generate_data,
        )
        task_get_api = PythonOperator(
            task_id="fetch_products_api",
            python_callable=extract_api_data,
        )

    task_silver = PythonOperator(
        task_id="bronze_to_silver_cleaning",
        python_callable=transform_bronze_to_silver,
    )

    task_dq_check = PythonOperator(
        task_id="data_quality_suite",
        python_callable=run_data_quality_checks,
    )

    task_gold = PythonOperator(
        task_id="silver_to_gold_modeling",
        python_callable=transform_silver_to_gold,
    )

    with TaskGroup("loading_group", tooltip="Load clean layers to target data stores") as loading_group:
        task_load_s3 = PythonOperator(
            task_id="load_to_s3_storage",
            python_callable=load_to_s3,
        )
        task_load_rds = PythonOperator(
            task_id="load_to_rds_dw",
            python_callable=load_to_rds,
        )

    end_task = EmptyOperator(task_id="end")

    # Pipeline task ordering
    start_task >> extraction_group >> task_silver >> task_dq_check >> task_gold >> loading_group >> end_task
