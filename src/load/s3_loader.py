import io
import logging
from pathlib import Path
import pandas as pd
import boto3
from botocore.exceptions import ClientError, BotoCoreError

logger = logging.getLogger("etl_pipeline.load.s3")

class S3Loader:
    """Manages writing cleaned dataframes and files to AWS S3 or MinIO S3-compatible storage."""

    def __init__(self, config):
        """Initializes the S3Loader.

        Args:
            config: Settings configuration object.
        """
        self.config = config
        self.bucket_name = config.s3_bucket_name

        # For local development with MinIO, we override the endpoint URL
        # We check if we are in development environment and endpoint is local
        endpoint_url = None
        if config.environment == "development":
            # If using Docker Compose, MinIO endpoint will be http://minio:9000
            # If running locally outside docker, it is http://localhost:9000
            # Let's check AWS environment variables or default to localhost
            endpoint_url = "http://localhost:9000"

        # Initialize boto3 S3 client
        try:
            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=config.aws_access_key_id,
                aws_secret_access_key=config.aws_secret_access_key,
                region_name=config.aws_region,
                endpoint_url=endpoint_url
            )
            logger.info(f"Initialized S3 Client (Endpoint: {endpoint_url or 'AWS Default'}, Bucket: {self.bucket_name})")
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            raise

    def create_bucket_if_not_exists(self) -> None:
        """Creates the target S3 bucket if it doesn't already exist."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Bucket '{self.bucket_name}' already exists.")
        except ClientError as e:
            # If bucket doesn't exist, code is 404
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "404":
                logger.info(f"Bucket '{self.bucket_name}' not found. Creating bucket...")
                try:
                    # In us-east-1, LocationConstraint is not allowed
                    if self.config.aws_region == "us-east-1":
                        self.s3_client.create_bucket(Bucket=self.bucket_name)
                    else:
                        self.s3_client.create_bucket(
                            Bucket=self.bucket_name,
                            CreateBucketConfiguration={"LocationConstraint": self.config.aws_region}
                        )
                    logger.info(f"Bucket '{self.bucket_name}' created successfully.")
                except Exception as create_err:
                    logger.error(f"Failed to create bucket '{self.bucket_name}': {create_err}")
                    raise
            else:
                logger.error(f"Error checking bucket '{self.bucket_name}': {e}")
                raise

    def upload_dataframe(self, df: pd.DataFrame, s3_key: str, data_format: str = "parquet") -> bool:
        """Converts DataFrame to bytes and uploads directly to S3.

        Args:
            df: DataFrame to upload.
            s3_key: Destination object key in the S3 bucket.
            data_format: Output format ('parquet' or 'csv').

        Returns:
            True if upload succeeded, False otherwise.
        """
        if df.empty:
            logger.warning(f"Attempted to upload empty DataFrame to key '{s3_key}'. Skipping.")
            return False

        logger.info(f"Uploading DataFrame ({len(df)} rows) to s3://{self.bucket_name}/{s3_key} as {data_format}...")
        
        # 1. Convert DataFrame to in-memory bytes buffer
        buffer = io.BytesIO()
        try:
            if data_format.lower() == "parquet":
                df.to_parquet(buffer, compression="snappy", index=False)
            elif data_format.lower() == "csv":
                df.to_csv(buffer, index=False)
            else:
                raise ValueError(f"Unsupported upload format: {data_format}")
                
            buffer.seek(0)
            
            # 2. Upload bytes buffer to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=buffer.getvalue()
            )
            
            # 3. Verify upload
            verified = self._verify_upload(s3_key)
            if verified:
                logger.info(f"Successfully uploaded and verified s3://{self.bucket_name}/{s3_key}")
                return True
            else:
                logger.error(f"Upload verification failed for s3://{self.bucket_name}/{s3_key}")
                return False
                
        except (ClientError, BotoCoreError) as aws_err:
            logger.error(f"AWS Error uploading to s3://{self.bucket_name}/{s3_key}: {aws_err}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error uploading to s3://{self.bucket_name}/{s3_key}: {e}")
            return False

    def upload_file(self, local_path: Path, s3_key: str) -> bool:
        """Uploads a local file to S3.

        Args:
            local_path: Local Path of the file.
            s3_key: Destination object key.

        Returns:
            True if upload succeeded.
        """
        if not local_path.exists():
            logger.error(f"Local file does not exist: {local_path}")
            return False
            
        logger.info(f"Uploading local file {local_path} to s3://{self.bucket_name}/{s3_key}...")
        
        try:
            self.s3_client.upload_file(
                Filename=str(local_path),
                Bucket=self.bucket_name,
                Key=s3_key
            )
            return self._verify_upload(s3_key)
        except Exception as e:
            logger.error(f"Error uploading file {local_path} to S3: {e}")
            return False

    def upload_partitioned(self, df: pd.DataFrame, base_key: str, partition_cols: list[str]) -> list[str]:
        """Partitions a DataFrame and uploads each partition to S3 in Hive format.

        Args:
            df: DataFrame to partition.
            base_key: Base S3 prefix key.
            partition_cols: List of column names to partition by.

        Returns:
            List of successfully uploaded S3 keys.
        """
        uploaded_keys = []
        if df.empty:
            return uploaded_keys

        logger.info(f"Partitioning DataFrame by columns {partition_cols} and uploading to base key '{base_key}'...")
        
        # Group by partition columns
        grouped = df.groupby(partition_cols)
        
        for keys_val, group_df in grouped:
            # Handle single vs multiple partition columns
            if isinstance(keys_val, tuple):
                parts = [f"{col}={val}" for col, val in zip(partition_cols, keys_val)]
            else:
                parts = [f"{partition_cols[0]}={keys_val}"]
                
            partition_path = "/".join(parts)
            s3_key = f"{base_key.rstrip('/')}/{partition_path}/data.parquet"
            
            # Reset index of partition group df
            group_df = group_df.reset_index(drop=True)
            
            # Upload partition
            if self.upload_dataframe(group_df, s3_key, data_format="parquet"):
                uploaded_keys.append(s3_key)
                
        logger.info(f"Partitioned upload complete. Uploaded {len(uploaded_keys)} partitions.")
        return uploaded_keys

    def list_objects(self, prefix: str) -> list[dict]:
        """Lists objects under a specific S3 prefix.

        Args:
            prefix: Prefix filter.

        Returns:
            List of dicts representing listed objects.
        """
        objects = []
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
            
            for page in pages:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        objects.append({
                            "key": obj["Key"],
                            "size_bytes": obj["Size"],
                            "last_modified": obj["LastModified"]
                        })
            return objects
        except Exception as e:
            logger.error(f"Error listing S3 objects under prefix '{prefix}': {e}")
            return []

    def _verify_upload(self, s3_key: str) -> bool:
        """Verifies that an object exists in the S3 bucket and has content.

        Args:
            s3_key: Object key to verify.

        Returns:
            True if the object exists and size > 0.
        """
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            size = response.get("ContentLength", 0)
            return size > 0
        except ClientError:
            return False
