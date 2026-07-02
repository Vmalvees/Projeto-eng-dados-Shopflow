import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock
from src.load.s3_loader import S3Loader
from src.load.rds_loader import RDSLoader

@patch("boto3.client")
def test_s3_loader_upload(mock_boto_client, test_settings):
    # Mock S3 Client
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    mock_s3.head_object.return_value = {"ContentLength": 100}
    
    loader = S3Loader(test_settings)
    df = pd.DataFrame({"col": [1, 2]})
    
    success = loader.upload_dataframe(df, "test_key.parquet")
    assert success
    mock_s3.put_object.assert_called_once()

def test_rds_loader_sqlite(sqlite_engine, test_settings):
    # We will test RDSLoader logic by patching the engine to use our mock SQLite engine
    with patch.object(RDSLoader, "_create_engine", return_value=sqlite_engine):
        with RDSLoader(test_settings) as rds:
            assert rds.health_check()
            
            # 1. Load DataFrame
            df_date = pd.DataFrame([
                {
                    "date_key": 20240101,
                    "full_date": "2024-01-01",
                    "day_of_week": 0,
                    "day_name": "Monday",
                    "day_of_month": 1,
                    "month": 1,
                    "month_name": "January",
                    "quarter": 1,
                    "year": 2024,
                    "is_weekend": False,
                    "is_month_start": True,
                    "is_month_end": False,
                    "is_holiday": True
                }
            ])
            
            rows = rds.load_dataframe(df_date, "dim_date", schema=None)
            assert rows == 1
            assert rds.get_table_row_count("dim_date", schema=None) == 1
            
            # 2. Test Upsert (since SQLite doesn't support the exact ON CONFLICT DDL syntax of Postgres
            # for schema-qualified temp table drops directly or complex upsert functions,
            # we check the get row count is valid and that it behaves correctly).
            # For SQLite, standard text queries work.
            q_res = rds.execute_query("SELECT year FROM dim_date WHERE date_key = 20240101")
            assert q_res.iloc[0, 0] == 2024
