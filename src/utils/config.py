"""Configuration management for the ETL pipeline.

Uses pydantic-settings to load configuration from environment variables
and .env files. Provides a singleton pattern via lru_cache for consistent
settings access throughout the application.

Example:
    >>> from src.utils.config import get_settings
    >>> settings = get_settings()
    >>> print(settings.environment)
    'development'
"""

from functools import lru_cache
from pathlib import Path
from typing import Optional, Any

import boto3
from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables and .env file.

    Attributes:
        aws_access_key_id: AWS access key for authentication.
        aws_secret_access_key: AWS secret key for authentication.
        aws_region: AWS region for services.
        s3_bucket_name: Target S3 bucket for the data lake.
        rds_host: RDS instance hostname.
        rds_port: RDS instance port number.
        rds_database: RDS database name.
        rds_user: RDS authentication username.
        rds_password: RDS authentication password.
        environment: Deployment environment identifier.
        log_level: Logging verbosity level.
        data_volume: Number of records to generate/process per batch.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- AWS Credentials ---
    aws_access_key_id: str = Field(
        default="",
        description="AWS access key ID for authentication.",
    )
    aws_secret_access_key: str = Field(
        default="",
        description="AWS secret access key for authentication.",
    )
    aws_region: str = Field(
        default="us-east-1",
        description="AWS region for all services.",
    )

    # --- S3 Configuration ---
    s3_bucket_name: str = Field(
        default="shopflow-data-lake",
        description="S3 bucket name for the data lake.",
    )

    # --- RDS Configuration ---
    rds_host: str = Field(
        default="localhost",
        description="RDS instance hostname.",
    )
    rds_port: int = Field(
        default=5432,
        description="RDS instance port number.",
    )
    rds_database: str = Field(
        default="ecommerce",
        description="RDS database name.",
    )
    rds_user: str = Field(
        default="admin",
        description="RDS authentication username.",
    )
    rds_password: str = Field(
        default="",
        description="RDS authentication password.",
    )

    # --- Application Settings ---
    environment: str = Field(
        default="development",
        description="Deployment environment (development, staging, production).",
    )
    log_level: str = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).",
    )
    data_volume: int = Field(
        default=10_000,
        description="Number of records to generate/process per batch.",
    )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def base_path(self) -> Path:
        """Project root directory, resolved from this file's location.

        Returns:
            Path to the project root (two levels up from src/utils/).
        """
        return Path(__file__).resolve().parent.parent.parent

    @computed_field  # type: ignore[prop-decorator]
    @property
    def bronze_path(self) -> Path:
        """Path to the Bronze layer (raw ingested data).

        Returns:
            Path to data/bronze/ directory.
        """
        return self.base_path / "data" / "bronze"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def silver_path(self) -> Path:
        """Path to the Silver layer (cleaned/validated data).

        Returns:
            Path to data/silver/ directory.
        """
        return self.base_path / "data" / "silver"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def gold_path(self) -> Path:
        """Path to the Gold layer (aggregated/analytics-ready data).

        Returns:
            Path to data/gold/ directory.
        """
        return self.base_path / "data" / "gold"

    def get_rds_connection_string(self) -> str:
        """Build a SQLAlchemy-compatible PostgreSQL connection string.

        Returns:
            Connection string in the format:
            ``postgresql+psycopg2://user:password@host:port/database``

        Example:
            >>> settings = Settings()
            >>> settings.get_rds_connection_string()
            'postgresql+psycopg2://admin:@localhost:5432/ecommerce'
        """
        return (
            f"postgresql+psycopg2://{self.rds_user}:{self.rds_password}"
            f"@{self.rds_host}:{self.rds_port}/{self.rds_database}"
        )

    def get_s3_client(self) -> Any:
        """Create and return a configured boto3 S3 client.

        Uses the AWS credentials stored in settings. If credentials are
        empty strings, boto3 will fall back to its default credential
        chain (env vars, ~/.aws/credentials, IAM role, etc.).

        Returns:
            A boto3 S3 client instance.

        Raises:
            botocore.exceptions.NoCredentialsError: If no valid
                credentials are found through any provider.
            botocore.exceptions.ClientError: If the client cannot
                be created due to AWS service issues.
        """
        client_kwargs: dict[str, str] = {"region_name": self.aws_region}

        if self.aws_access_key_id and self.aws_secret_access_key:
            client_kwargs["aws_access_key_id"] = self.aws_access_key_id
            client_kwargs["aws_secret_access_key"] = self.aws_secret_access_key

        return boto3.client("s3", **client_kwargs)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached singleton instance of Settings.

    Uses ``functools.lru_cache`` to ensure only one Settings instance
    exists throughout the application lifecycle, preventing redundant
    .env file reads and object creation.

    Returns:
        The singleton Settings instance.

    Example:
        >>> settings = get_settings()
        >>> settings.environment
        'development'
    """
    return Settings()
