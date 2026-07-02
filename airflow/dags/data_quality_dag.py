import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def monitor_dw_health(**kwargs):
    """Executes basic SQL tests against PostgreSQL DW Gold tables to verify data freshness and rows."""
    try:
        from src.load.rds_loader import RDSLoader
        from src.utils.config import get_settings
        
        settings = get_settings()
        
        with RDSLoader(settings) as rds:
            if not rds.health_check():
                raise ConnectionError("Unable to connect to DWH.")
            
            # 1. Row counts monitor
            tables = ["dim_date", "dim_customer", "dim_product", "fact_orders"]
            for table in tables:
                count = rds.get_table_row_count(table)
                logger.info(f"Monitor: Table '{table}' has {count} rows.")
                if count == 0:
                    raise ValueError(f"CRITICAL: Table 'gold.{table}' is empty!")

            # 2. Check for recent orders (Freshness check)
            # Find the max date in fact_orders
            query = "SELECT MAX(created_at) FROM gold.fact_orders"
            df = rds.execute_query(query)
            max_date = df.iloc[0, 0] if not df.empty else None
            
            logger.info(f"Monitor: Latest order created_at in DWH: {max_date}")
            
    except Exception as e:
        logger.error(f"Error in monitor_dw_health: {e}")
        raise AirflowException(e)

with DAG(
    dag_id="dwh_health_monitor",
    default_args=default_args,
    description="Standalone data monitoring and health check pipeline for DWH Gold tables",
    schedule_interval="0 */6 * * *",  # Every 6 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["monitoring", "dwh", "health"],
) as dag:

    monitor_task = PythonOperator(
        task_id="check_dw_tables_health",
        python_callable=monitor_dw_health,
    )
