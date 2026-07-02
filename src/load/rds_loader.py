import logging
import re
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger("etl_pipeline.load.rds")

class RDSLoader:
    """Manages database connection and loading structured gold datasets into PostgreSQL/RDS."""

    def __init__(self, config):
        """Initializes the RDSLoader.

        Args:
            config: Settings configuration object.
        """
        self.config = config
        self.connection_string = config.get_rds_connection_string()
        self.engine: Engine = self._create_engine()

    def _create_engine(self) -> Engine:
        """Creates an SQLAlchemy engine with connection pooling.

        Returns:
            SQLAlchemy Engine instance.
        """
        # Mask password in logs
        masked_conn = re.sub(r":([^@]+)@", ":****@", self.connection_string)
        logger.info(f"Connecting to database with: {masked_conn}")
        
        try:
            return create_engine(
                self.connection_string,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=1800
            )
        except Exception as e:
            logger.error(f"Failed to create database engine: {e}")
            raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self) -> None:
        """Disposes the SQLAlchemy engine connection pool."""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection pool disposed.")

    def health_check(self) -> bool:
        """Tests the database connection.

        Returns:
            True if connection is successful, False otherwise.
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except SQLAlchemyError as e:
            logger.error(f"Database connection health check failed: {e}")
            return False

    def create_tables(self, schema_path: Path) -> None:
        """Executes DDL statements from a schema SQL file to initialize tables.

        Args:
            schema_path: Path to the star_schema.sql file.
        """
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema file not found at {schema_path}")

        logger.info(f"Executing schema from {schema_path}...")
        
        try:
            with open(schema_path, "r", encoding="utf-8") as f:
                sql_statements = f.read()

            # Split SQL file into individual statements to execute separately
            # We split by semi-colons, but ignore semi-colons inside comments/strings
            # A simple split is usually sufficient for simple schema files
            statements = [stmt.strip() for stmt in sql_statements.split(";") if stmt.strip()]

            with self.engine.begin() as conn:
                for statement in statements:
                    conn.execute(text(statement))
            
            logger.info("Database schema initialized successfully.")
            
        except Exception as e:
            logger.error(f"Failed to initialize database schema: {e}")
            raise

    def load_dataframe(self, df: pd.DataFrame, table_name: str, schema: str = "gold", if_exists: str = "append") -> int:
        """Loads a DataFrame into the target table in bulk.

        Args:
            df: DataFrame to load.
            table_name: Name of the target table.
            schema: Database schema name (default 'gold').
            if_exists: 'append', 'replace', or 'fail'.

        Returns:
            Number of rows loaded.
        """
        if df.empty:
            logger.warning(f"DataFrame for table '{schema}.{table_name}' is empty. Skipping load.")
            return 0

        logger.info(f"Loading {len(df)} rows into target table '{schema}.{table_name}'...")
        
        try:
            # We load using pandas to_sql with method='multi' for bulk insert speed
            rows_loaded = df.to_sql(
                name=table_name,
                con=self.engine,
                schema=schema,
                if_exists=if_exists,
                index=False,
                chunksize=1000,
                method="multi"
            )
            
            # pandas to_sql returns None or row count. Let's count rows if None is returned
            actual_count = len(df)
            logger.info(f"Successfully loaded {actual_count} rows into '{schema}.{table_name}'.")
            return actual_count
            
        except SQLAlchemyError as e:
            logger.error(f"SQLAlchemy Error loading table '{schema}.{table_name}': {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error loading table '{schema}.{table_name}': {e}")
            raise

    def upsert_dataframe(self, df: pd.DataFrame, table_name: str, conflict_columns: list[str], schema: str = "gold") -> int:
        """Upserts a DataFrame into the database using INSERT ON CONFLICT DO UPDATE.

        Args:
            df: DataFrame containing data to upsert.
            table_name: Name of the target database table.
            conflict_columns: Columns representing unique keys / indexes.
            schema: Database schema name (default 'gold').

        Returns:
            Number of rows upserted.
        """
        if df.empty:
            return 0

        logger.info(f"Performing upsert of {len(df)} rows into '{schema}.{table_name}' on conflict columns {conflict_columns}...")
        
        # To perform clean upserts in pandas -> PostgreSQL:
        # 1. Load DataFrame into a staging temporary table
        # 2. Run INSERT INTO target SELECT * FROM staging ON CONFLICT (keys) DO UPDATE...
        # 3. Drop staging table
        
        temp_table_name = f"temp_{table_name}_{pd.Timestamp.now().strftime('%H%M%S')}"
        
        try:
            # Create staging table with same schema (without data)
            with self.engine.begin() as conn:
                # 1. Load to temp table
                df.to_sql(
                    name=temp_table_name,
                    con=conn,
                    schema=schema,
                    if_exists="replace",
                    index=False
                )
                
                # Get column list (excluding auto-increment keys like serials, but here we specify all cols from df)
                cols = list(df.columns)
                cols_str = ", ".join([f'"{c}"' for c in cols])
                
                # Exclude conflict columns from update statement
                update_cols = [c for c in cols if c not in conflict_columns]
                update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
                
                conflict_str = ", ".join([f'"{c}"' for c in conflict_columns])
                
                # 2. Build and run SQL Upsert
                upsert_query = f"""
                    INSERT INTO "{schema}"."{table_name}" ({cols_str})
                    SELECT {cols_str} FROM "{schema}"."{temp_table_name}"
                    ON CONFLICT ({conflict_str})
                    DO UPDATE SET {update_str};
                """
                
                result = conn.execute(text(upsert_query))
                
                # 3. Clean up temp table
                conn.execute(text(f'DROP TABLE "{schema}"."{temp_table_name}"'))
                
            logger.info(f"Upsert complete for table '{schema}.{table_name}'.")
            return len(df)
            
        except SQLAlchemyError as e:
            logger.error(f"SQLAlchemy Error during upsert into '{schema}.{table_name}': {e}")
            # Try to drop temp table just in case
            try:
                with self.engine.begin() as conn:
                    conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{temp_table_name}"'))
            except Exception:
                pass
            raise

    def get_table_row_count(self, table_name: str, schema: str = "gold") -> int:
        """Returns the number of rows in the specified table.

        Args:
            table_name: Table name.
            schema: Schema name.

        Returns:
            Row count.
        """
        try:
            with self.engine.connect() as conn:
                table_ref = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_ref}"))
                count = result.scalar()
                return count if count is not None else 0
        except SQLAlchemyError as e:
            logger.error(f"Error checking row count for '{schema}.{table_name}': {e}")
            return -1

    def execute_query(self, query: str, params: dict = None) -> pd.DataFrame:
        """Executes an arbitrary SQL query and returns the results as a DataFrame.

        Args:
            query: SQL Query string.
            params: Query parameters.

        Returns:
            pd.DataFrame containing the query results.
        """
        try:
            return pd.read_sql_query(query, self.engine, params=params)
        except SQLAlchemyError as e:
            logger.error(f"Error executing custom query: {e}")
            raise
