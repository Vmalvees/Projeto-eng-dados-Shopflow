-- Create database for Airflow Metadata
CREATE DATABASE airflow_db;

-- Create user for Airflow Metadata DB
CREATE USER airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow;

-- Connect to shopflow_dw (default POSTGRES_DB) and grant access to user airflow
GRANT ALL PRIVILEGES ON SCHEMA public TO postgres;
