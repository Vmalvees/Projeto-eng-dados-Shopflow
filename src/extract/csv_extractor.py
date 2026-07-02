import logging
from pathlib import Path
from typing import Dict, Any, Tuple, Optional
import pandas as pd
import chardet

logger = logging.getLogger("etl_pipeline.extractor.csv")

class CsvExtractor:
    """Extracts, validates, and profiles data from local CSV files."""

    def __init__(self):
        """Initializes the CSV extractor."""
        pass

    def detect_encoding(self, file_path: Path) -> str:
        """Automatically detects the character encoding of a file.

        Args:
            file_path: Absolute or relative Path to the file.

        Returns:
            Detected encoding string (e.g. 'utf-8', 'ISO-8859-1').
        """
        try:
            with open(file_path, "rb") as f:
                raw_data = f.read(20000)  # Read first 20KB for detection
            result = chardet.detect(raw_data)
            encoding = result.get("encoding", "utf-8")
            logger.info(f"Detected encoding for {file_path.name}: {encoding} (confidence: {result.get('confidence'):.2f})")
            return encoding if encoding is not None else "utf-8"
        except Exception as e:
            logger.warning(f"Error detecting encoding for {file_path.name}: {e}. Defaulting to utf-8.")
            return "utf-8"

    def extract(self, file_path: Path, expected_columns: Optional[list] = None) -> pd.DataFrame:
        """Reads a CSV file into a pandas DataFrame.

        Args:
            file_path: Path to the CSV file.
            expected_columns: Optional list of column names that must be present.

        Returns:
            pd.DataFrame of loaded data.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        encoding = self.detect_encoding(file_path)
        
        # Auto-detect delimiter by trying common ones
        delimiters = [",", ";", "\t"]
        df = pd.DataFrame()
        
        for sep in delimiters:
            try:
                # Read just 2 rows to verify columns
                sample_df = pd.read_csv(file_path, sep=sep, encoding=encoding, nrows=2)
                # If it split into multiple columns, it's likely the right separator
                if len(sample_df.columns) > 1:
                    logger.info(f"Detected separator '{sep}' for CSV file: {file_path.name}")
                    df = pd.read_csv(file_path, sep=sep, encoding=encoding)
                    break
            except Exception:
                continue
                
        # Fallback if delimiter detection failed
        if df.empty:
            logger.warning(f"Delimiter detection failed for {file_path.name}. Defaulting to comma.")
            try:
                df = pd.read_csv(file_path, encoding=encoding)
            except Exception as e:
                logger.error(f"Failed to read CSV {file_path}: {e}")
                raise

        # Schema validation
        if expected_columns:
            is_valid, missing_cols = self.validate_schema(df, expected_columns)
            if not is_valid:
                logger.warning(f"CSV {file_path.name} schema validation failed. Missing columns: {missing_cols}")
                # We do not crash the pipeline here, but log a warnings so downstream components know

        logger.info(f"Successfully extracted CSV {file_path.name} with {len(df)} rows and {len(df.columns)} columns.")
        return df

    def extract_directory(self, dir_path: Path, pattern: str = "*.csv") -> Dict[str, pd.DataFrame]:
        """Reads all matching CSV files in a directory.

        Args:
            dir_path: Path to the directory.
            pattern: Glob pattern to filter files.

        Returns:
            Dict mapping filename to DataFrame.
        """
        results = {}
        if not dir_path.is_dir():
            logger.warning(f"Directory {dir_path} does not exist or is not a directory.")
            return results

        for file in dir_path.glob(pattern):
            try:
                # Use file stem as key
                results[file.stem] = self.extract(file)
            except Exception as e:
                logger.error(f"Error extracting CSV file {file.name}: {e}")
                
        return results

    def validate_schema(self, df: pd.DataFrame, expected_columns: list) -> Tuple[bool, list]:
        """Validates that all expected columns are present in the DataFrame.

        Args:
            df: DataFrame to validate.
            expected_columns: List of columns to check.

        Returns:
            Tuple (is_valid: bool, missing_columns: list)
        """
        missing_columns = [col for col in expected_columns if col not in df.columns]
        return len(missing_columns) == 0, missing_columns

    def get_file_metadata(self, file_path: Path) -> Dict[str, Any]:
        """Collects metadata about a file.

        Args:
            file_path: Path to the file.

        Returns:
            Dict containing metadata like size, file name, rows, etc.
        """
        if not file_path.exists():
            return {}

        stats = file_path.stat()
        
        try:
            df = self.extract(file_path)
            rows, cols = df.shape
            dtypes = {col: str(dtype) for col, dtype in df.dtypes.items()}
        except Exception:
            rows, cols = -1, -1
            dtypes = {}

        return {
            "filename": file_path.name,
            "size_bytes": stats.st_size,
            "last_modified": stats.st_mtime,
            "row_count": rows,
            "column_count": cols,
            "data_types": dtypes
        }
