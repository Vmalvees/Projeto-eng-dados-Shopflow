-- Create database for Airflow Metadata
CREATE DATABASE airflow_db;

-- Create user for Airflow Metadata DB
CREATE USER airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow;

-- Connect to airflow_db and grant schema privileges to user airflow (needed in PG 15+)
\c airflow_db;
GRANT ALL ON SCHEMA public TO airflow;

-- Connect to shopflow_dw and grant access to user postgres
\c shopflow_dw;
GRANT ALL PRIVILEGES ON SCHEMA public TO postgres;
