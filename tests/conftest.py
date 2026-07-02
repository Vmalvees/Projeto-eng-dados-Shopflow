import os
import pytest
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from src.utils.config import Settings

# Override environment variables for testing
os.environ["ENVIRONMENT"] = "testing"
os.environ["AWS_ACCESS_KEY_ID"] = "mock_key"
os.environ["AWS_SECRET_ACCESS_KEY"] = "mock_secret"
os.environ["AWS_REGION"] = "us-east-1"
os.environ["S3_BUCKET_NAME"] = "mock-bucket"
os.environ["RDS_HOST"] = "mock-host"
os.environ["RDS_PORT"] = "5432"
os.environ["RDS_DATABASE"] = "mock-db"
os.environ["RDS_USER"] = "mock-user"
os.environ["RDS_PASSWORD"] = "mock-password"

@pytest.fixture
def test_settings():
    """Returns a test settings instance."""
    return Settings(
        environment="testing",
        aws_access_key_id="mock_key",
        aws_secret_access_key="mock_secret",
        aws_region="us-east-1",
        s3_bucket_name="mock-bucket",
        rds_host="mock-host",
        rds_port=5432,
        rds_database="mock-db",
        rds_user="mock-user",
        rds_password="mock-password"
    )

@pytest.fixture
def mock_customers_df():
    """Provides a sample customer raw DataFrame."""
    return pd.DataFrame([
        {
            "customer_id": "c1",
            "first_name": " João ",
            "last_name": "Silva",
            "email": "JOAO.SILVA@gmail.com",
            "phone": " (11) 99999-9999 ",
            "address": "Rua A, 123",
            "city": "São Paulo",
            "state": "SP",
            "segment": "Premium",
            "registration_date": "2024-05-15 10:00:00",
            "is_active": True
        },
        {
            "customer_id": "c2",
            "first_name": "maria",
            "last_name": " Oliveira ",
            "email": "maria.oliveira@outlook.com",
            "phone": None,
            "address": "Av B, 456",
            "city": "Rio de Janeiro",
            "state": "RJ",
            "segment": "b2c",
            "registration_date": "2024-06-20 14:30:00",
            "is_active": False
        },
        # Duplicate email test case
        {
            "customer_id": "c3",
            "first_name": "João",
            "last_name": "Silva Duplicado",
            "email": "joao.silva@gmail.com",
            "phone": "11888888888",
            "address": "Rua A, 123",
            "city": "São Paulo",
            "state": "SP",
            "segment": "Premium",
            "registration_date": "2024-05-16 10:00:00",
            "is_active": True
        }
    ])

@pytest.fixture
def mock_products_df():
    """Provides a sample product raw DataFrame."""
    return pd.DataFrame([
        {
            "product_id": "p1",
            "name": "Smartphone X",
            "category": "electronics",
            "subcategory": "cellphones",
            "price": 1200.00,
            "cost_price": 800.00,
            "stock_quantity": 50.0,
            "supplier": "TechCorp",
            "rating": 4.5,
            "sku": "ELE-CEL-001",
            "created_at": "2023-01-10 08:00:00"
        },
        {
            "product_id": "p2",
            "name": "T-Shirt Classic",
            "category": "clothing",
            "subcategory": "shirts",
            "price": -49.90,  # Negative price to test fix
            "cost_price": 20.00,
            "stock_quantity": None,  # Null stock to test fix
            "supplier": "WearableLtda",
            "rating": 4.2,
            "sku": "CLO-SHI-002",
            "created_at": "2023-02-15 09:00:00"
        }
    ])

@pytest.fixture
def mock_orders_df():
    """Provides a sample orders raw DataFrame."""
    return pd.DataFrame([
        {
            "order_id": "o1",
            "customer_id": "c1",
            "product_id": "p1",
            "quantity": 2,
            "unit_price": 1200.00,
            "discount_percent": 10.0,
            "total_amount": 2160.00,  # 2 * 1200 * 0.9 = 2160
            "order_date": "2024-06-25 15:00:00",
            "status": "Completed",
            "payment_method": "credit_card",
            "shipping_cost": 0.0
        },
        {
            "order_id": "o2",
            "customer_id": "c2",
            "product_id": "p2",
            "quantity": 1,
            "unit_price": 49.90,
            "discount_percent": 0.0,
            "total_amount": 49.90,
            "order_date": "2024-06-26 16:00:00",
            "status": "pending",
            "payment_method": "pix",
            "shipping_cost": 15.00
        },
        # Future order date test case
        {
            "order_id": "o3",
            "customer_id": "c1",
            "product_id": "p1",
            "quantity": 3,
            "unit_price": 1200.00,
            "discount_percent": 0.0,
            "total_amount": 3600.00,
            "order_date": "2028-12-31 23:59:59",  # Future date
            "status": "completed",
            "payment_method": "boleto",
            "shipping_cost": 0.0
        }
    ])

@pytest.fixture
def sqlite_engine():
    """Provides an in-memory SQLite database engine for testing loads."""
    engine = create_engine("sqlite:///:memory:")
    
    # Simple DDL matching our schema but translated for SQLite compatibility
    # SQLite does not support schemas (e.g. gold.dim_date) directly in same database easily,
    # so we create them as simple tables in the default schema.
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE dim_date (
                date_key INTEGER PRIMARY KEY,
                full_date TEXT NOT NULL,
                day_of_week INTEGER NOT NULL,
                day_name TEXT NOT NULL,
                day_of_month INTEGER NOT NULL,
                month INTEGER NOT NULL,
                month_name TEXT NOT NULL,
                quarter INTEGER NOT NULL,
                year INTEGER NOT NULL,
                is_weekend BOOLEAN NOT NULL DEFAULT 0,
                is_month_start BOOLEAN NOT NULL DEFAULT 0,
                is_month_end BOOLEAN NOT NULL DEFAULT 0,
                is_holiday BOOLEAN NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))
        
        conn.execute(text("""
            CREATE TABLE dim_customer (
                customer_key INTEGER PRIMARY KEY,
                customer_id TEXT NOT NULL,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                email TEXT,
                phone TEXT,
                address TEXT,
                city TEXT,
                state TEXT,
                segment TEXT,
                valid_from TEXT NOT NULL,
                valid_to TEXT NOT NULL DEFAULT '9999-12-31',
                is_current BOOLEAN NOT NULL DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))

        conn.execute(text("""
            CREATE TABLE dim_product (
                product_key INTEGER PRIMARY KEY,
                product_id TEXT NOT NULL,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                subcategory TEXT NOT NULL,
                price REAL NOT NULL,
                cost_price REAL NOT NULL,
                price_range TEXT NOT NULL,
                margin_percent REAL NOT NULL,
                supplier TEXT,
                rating REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))

        conn.execute(text("""
            CREATE TABLE fact_orders (
                order_key INTEGER PRIMARY KEY,
                order_id TEXT NOT NULL,
                customer_key INTEGER NOT NULL,
                product_key INTEGER NOT NULL,
                date_key INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                unit_price REAL NOT NULL,
                discount_percent REAL DEFAULT 0.0,
                discount_amount REAL DEFAULT 0.0,
                total_amount REAL NOT NULL,
                net_amount REAL NOT NULL,
                shipping_cost REAL DEFAULT 0.0,
                payment_method TEXT,
                order_status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))
        
    return engine

from sqlalchemy import text
