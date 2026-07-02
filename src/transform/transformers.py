import logging
import re
import pandas as pd
from typing import List, Union, Dict, Any, Optional

logger = logging.getLogger("etl_pipeline.transform.helpers")

def standardize_strings(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """Strips whitespace and converts values to lowercase for specified columns.

    Args:
        df: Input DataFrame.
        columns: List of columns to standardize.

    Returns:
        pd.DataFrame with modified columns.
    """
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.lower()
    return df

def normalize_names(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """Converts strings in the specified columns to title case.

    Args:
        df: Input DataFrame.
        columns: List of columns to normalize.

    Returns:
        pd.DataFrame with normalized columns.
    """
    df = df.copy()
    for col in columns:
        if col in df.columns:
            # Handle nulls cleanly
            df[col] = df[col].apply(lambda x: str(x).strip().title() if pd.notnull(x) else x)
    return df

def clean_email(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Cleans and standardizes email strings. Invalid emails are set to None.

    Args:
        df: Input DataFrame.
        column: Email column name.

    Returns:
        pd.DataFrame with cleaned emails.
    """
    df = df.copy()
    if column in df.columns:
        # Standardize email string representation
        df[column] = df[column].astype(str).str.strip().str.lower()
        
        # Regex for simple email validation
        email_regex = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
        
        def validate_email(email: str) -> Optional[str]:
            if not isinstance(email, str) or email == "nan" or email == "none":
                return None
            if re.match(email_regex, email):
                return email
            return None

        df[column] = df[column].apply(validate_email)
    return df

def fix_negative_values(df: pd.DataFrame, columns: List[str], strategy: str = "absolute") -> pd.DataFrame:
    """Fixes negative values in numeric columns using a specified strategy.

    Args:
        df: Input DataFrame.
        columns: List of numeric columns.
        strategy: 'absolute' to take the absolute value, 'zero' to cap at 0.

    Returns:
        pd.DataFrame with fixed numeric values.
    """
    df = df.copy()
    for col in columns:
        if col in df.columns:
            neg_count = (df[col] < 0).sum()
            if neg_count > 0:
                logger.warning(f"Found {neg_count} negative values in '{col}'. Applying strategy '{strategy}'.")
                if strategy == "absolute":
                    df[col] = df[col].abs()
                elif strategy == "zero":
                    df[col] = df[col].clip(lower=0)
    return df

def round_currency(df: pd.DataFrame, columns: List[str], decimals: int = 2) -> pd.DataFrame:
    """Rounds numeric columns representing money to specified decimal points.

    Args:
        df: Input DataFrame.
        columns: List of columns.
        decimals: Decimal precision (default 2).

    Returns:
        pd.DataFrame with rounded values.
    """
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = df[col].round(decimals)
    return df

def parse_dates(df: pd.DataFrame, columns: List[str], date_format: Optional[str] = None) -> pd.DataFrame:
    """Converts specified columns to datetime.

    Args:
        df: Input DataFrame.
        columns: List of columns.
        date_format: Format string, or None for automatic parsing.

    Returns:
        pd.DataFrame with datetime columns.
    """
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format=date_format, errors="coerce")
    return df

def remove_future_dates(df: pd.DataFrame, date_column: str, reference_date: Optional[Union[str, pd.Timestamp]] = None) -> pd.DataFrame:
    """Filters out rows with date values in the future.

    Args:
        df: Input DataFrame.
        date_column: Column name containing dates.
        reference_date: Cutoff date. If None, uses current system time.

    Returns:
        pd.DataFrame containing only non-future dates.
    """
    df = df.copy()
    if date_column in df.columns:
        ref_dt = pd.to_datetime(reference_date) if reference_date else pd.Timestamp.now()
        df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
        
        future_mask = df[date_column] > ref_dt
        future_count = future_mask.sum()
        if future_count > 0:
            logger.warning(f"Filtering out {future_count} rows with future dates in '{date_column}' (cutoff: {ref_dt}).")
            df = df[~future_mask]
    return df

def remove_duplicates(df: pd.DataFrame, subset: Optional[List[str]] = None, keep: str = "first") -> pd.DataFrame:
    """Removes duplicate rows from the DataFrame and logs details.

    Args:
        df: Input DataFrame.
        subset: Columns to consider for uniqueness.
        keep: 'first', 'last', or False.

    Returns:
        pd.DataFrame without duplicates.
    """
    initial_rows = len(df)
    df = df.drop_duplicates(subset=subset, keep=keep)
    deduped_rows = len(df)
    
    dupes_removed = initial_rows - deduped_rows
    if dupes_removed > 0:
        logger.info(f"Removed {dupes_removed} duplicate rows (subset={subset}). Rows remaining: {deduped_rows}")
    return df

def handle_nulls(df: pd.DataFrame, strategy_map: Dict[str, str]) -> pd.DataFrame:
    """Applies specific null-handling strategies to columns.

    Args:
        df: Input DataFrame.
        strategy_map: Dict mapping col name to strategy ('drop', 'fill_mean', 'fill_median', 'fill_mode', 'fill_value:X').

    Returns:
        pd.DataFrame with handled nulls.
    """
    df = df.copy()
    for col, strategy in strategy_map.items():
        if col not in df.columns:
            continue
            
        null_count = df[col].isnull().sum()
        if null_count == 0:
            continue
            
        logger.info(f"Handling {null_count} nulls in '{col}' using strategy '{strategy}'.")
        
        if strategy == "drop":
            df = df.dropna(subset=[col])
        elif strategy == "fill_mean":
            df[col] = df[col].fillna(df[col].mean())
        elif strategy == "fill_median":
            df[col] = df[col].fillna(df[col].median())
        elif strategy == "fill_mode":
            mode_val = df[col].mode()
            if not mode_val.empty:
                df[col] = df[col].fillna(mode_val[0])
        elif strategy.startswith("fill_value:"):
            val_str = strategy.split(":", 1)[1]
            # Convert value to correct type based on column type
            if df[col].dtype == "float64":
                val = float(val_str)
            elif df[col].dtype == "int64":
                val = int(val_str)
            elif df[col].dtype == "bool":
                val = val_str.lower() in ("true", "1", "yes")
            else:
                val = val_str
            df[col] = df[col].fillna(val)
            
    return df

def validate_foreign_keys(df: pd.DataFrame, fk_column: str, reference_df: pd.DataFrame, pk_column: str) -> pd.DataFrame:
    """Filters out rows where the foreign key does not exist in the reference table.

    Args:
        df: DataFrame containing the foreign key.
        fk_column: Column name of the foreign key.
        reference_df: Reference DataFrame (dimension table).
        pk_column: Column name of the primary key in the reference.

    Returns:
        pd.DataFrame containing only valid rows.
    """
    df = df.copy()
    if fk_column in df.columns and pk_column in reference_df.columns:
        valid_keys = set(reference_df[pk_column].unique())
        initial_count = len(df)
        
        # Keep keys that are in the set or nulls (we validate nulls elsewhere)
        valid_mask = df[fk_column].isin(valid_keys) | df[fk_column].isnull()
        df = df[valid_mask]
        
        removed_count = initial_count - len(df)
        if removed_count > 0:
            logger.warning(f"Filtered out {removed_count} rows due to foreign key constraint violation in '{fk_column}'.")
    return df
