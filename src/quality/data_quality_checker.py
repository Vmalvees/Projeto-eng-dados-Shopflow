import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
import pandas as pd

logger = logging.getLogger("etl_pipeline.quality.checker")

@dataclass
class QualityCheckResult:
    """Represents the results of a single data quality check."""
    check_name: str
    table_name: str
    passed: bool
    details: Dict[str, Any]
    timestamp: datetime = datetime.now()


class DataQualityChecker:
    """Performs light-weight and structured data quality checks on pandas DataFrames."""

    def __init__(self):
        """Initializes the DataQualityChecker."""
        pass

    def check_completeness(self, df: pd.DataFrame, table_name: str, columns: List[str], threshold: float = 0.98) -> List[QualityCheckResult]:
        """Validates that columns do not contain too many null values.

        Args:
            df: DataFrame to check.
            table_name: Table name for logging.
            columns: Columns to check for nulls.
            threshold: Minimum percentage of non-null values required (0.0 to 1.0).

        Returns:
            List of QualityCheckResult objects.
        """
        results = []
        total_rows = len(df)
        
        if total_rows == 0:
            logger.warning(f"Skipping completeness checks for empty table '{table_name}'.")
            return results

        for col in columns:
            if col not in df.columns:
                results.append(QualityCheckResult(
                    check_name="completeness",
                    table_name=table_name,
                    passed=False,
                    details={"column": col, "error": "Column not found in DataFrame"}
                ))
                continue

            non_null_count = df[col].notnull().sum()
            completeness_pct = non_null_count / total_rows
            passed = completeness_pct >= threshold

            details = {
                "column": col,
                "total_rows": total_rows,
                "non_null_count": non_null_count,
                "null_count": total_rows - non_null_count,
                "actual_percentage": round(completeness_pct * 100, 2),
                "threshold_percentage": round(threshold * 100, 2)
            }

            logger.info(f"[{table_name}.{col}] Completeness check: {'PASSED' if passed else 'FAILED'} "
                        f"({details['actual_percentage']}% non-null vs threshold {details['threshold_percentage']}%)")

            results.append(QualityCheckResult(
                check_name=f"completeness_{col}",
                table_name=table_name,
                passed=passed,
                details=details
            ))

        return results

    def check_uniqueness(self, df: pd.DataFrame, table_name: str, columns: List[str]) -> List[QualityCheckResult]:
        """Validates that specified columns contain unique values (no duplicates).

        Args:
            df: DataFrame to check.
            table_name: Table name.
            columns: Columns that must contain unique values.

        Returns:
            List of QualityCheckResult objects.
        """
        results = []
        total_rows = len(df)

        if total_rows == 0:
            return results

        for col in columns:
            if col not in df.columns:
                results.append(QualityCheckResult(
                    check_name="uniqueness",
                    table_name=table_name,
                    passed=False,
                    details={"column": col, "error": "Column not found"}
                ))
                continue

            non_null_series = df[col].dropna()
            non_null_count = len(non_null_series)
            unique_count = non_null_series.nunique()
            passed = unique_count == non_null_count
            
            duplicate_count = non_null_count - unique_count

            details = {
                "column": col,
                "total_rows": total_rows,
                "non_null_rows": non_null_count,
                "unique_rows": unique_count,
                "duplicate_rows": duplicate_count,
                "duplicate_percentage": round((duplicate_count / non_null_count) * 100, 2) if non_null_count > 0 else 0
            }

            logger.info(f"[{table_name}.{col}] Uniqueness check: {'PASSED' if passed else 'FAILED'} "
                        f"({duplicate_count} duplicate rows found)")

            results.append(QualityCheckResult(
                check_name=f"uniqueness_{col}",
                table_name=table_name,
                passed=passed,
                details=details
            ))

        return results

    def check_value_ranges(self, df: pd.DataFrame, table_name: str, range_specs: Dict[str, Tuple[float, float]]) -> List[QualityCheckResult]:
        """Checks if numeric column values lie within specified min and max bounds.

        Args:
            df: DataFrame to check.
            table_name: Table name.
            range_specs: Dict mapping column name to a tuple of (min_value, max_value).

        Returns:
            List of QualityCheckResult objects.
        """
        results = []

        if df.empty:
            return results

        for col, bounds in range_specs.items():
            if col not in df.columns:
                continue

            min_val, max_val = bounds
            
            # Count values out of bounds
            out_of_bounds_mask = (df[col] < min_val) | (df[col] > max_val)
            out_of_bounds_count = out_of_bounds_mask.sum()
            passed = out_of_bounds_count == 0

            details = {
                "column": col,
                "min_allowed": min_val,
                "max_allowed": max_val,
                "out_of_bounds_count": int(out_of_bounds_count),
                "actual_min": float(df[col].min()) if not df[col].empty else None,
                "actual_max": float(df[col].max()) if not df[col].empty else None
            }

            logger.info(f"[{table_name}.{col}] Range check [{min_val}, {max_val}]: {'PASSED' if passed else 'FAILED'} "
                        f"({out_of_bounds_count} rows out of bounds)")

            results.append(QualityCheckResult(
                check_name=f"range_{col}",
                table_name=table_name,
                passed=passed,
                details=details
            ))

        return results

    def check_allowed_values(self, df: pd.DataFrame, table_name: str, value_specs: Dict[str, List[Any]]) -> List[QualityCheckResult]:
        """Checks if column values belong to a set of allowed values.

        Args:
            df: DataFrame to check.
            table_name: Table name.
            value_specs: Dict mapping column name to list of allowed values.

        Returns:
            List of QualityCheckResult objects.
        """
        results = []

        if df.empty:
            return results

        for col, allowed in value_specs.items():
            if col not in df.columns:
                continue

            # Standardize comparison if categorical lists are strings
            invalid_mask = ~df[col].isin(allowed) & df[col].notnull()
            invalid_count = invalid_mask.sum()
            passed = invalid_count == 0

            # Collect a sample of invalid values
            invalid_sample = df[invalid_mask][col].unique()[:5].tolist() if invalid_count > 0 else []

            details = {
                "column": col,
                "allowed_values": allowed,
                "invalid_count": int(invalid_count),
                "invalid_sample": invalid_sample
            }

            logger.info(f"[{table_name}.{col}] Allowed values check: {'PASSED' if passed else 'FAILED'} "
                        f"({invalid_count} invalid values found)")

            results.append(QualityCheckResult(
                check_name=f"allowed_values_{col}",
                table_name=table_name,
                passed=passed,
                details=details
            ))

        return results

    def check_freshness(self, df: pd.DataFrame, table_name: str, date_column: str, max_age_hours: int = 24) -> QualityCheckResult:
        """Validates that the latest date in the dataset is within max_age_hours of current time.

        Args:
            df: DataFrame.
            table_name: Table name.
            date_column: Column name containing dates.
            max_age_hours: Maximum allowed hours since the latest update.

        Returns:
            QualityCheckResult.
        """
        if df.empty or date_column not in df.columns:
            return QualityCheckResult(
                check_name="freshness",
                table_name=table_name,
                passed=False,
                details={"error": "Table empty or date column missing"}
            )

        latest_date = pd.to_datetime(df[date_column]).max()
        current_time = pd.Timestamp.now()
        
        age_hours = (current_time - latest_date).total_seconds() / 3600
        passed = age_hours <= max_age_hours

        details = {
            "date_column": date_column,
            "latest_date": str(latest_date),
            "current_time": str(current_time),
            "age_hours": round(age_hours, 2),
            "max_allowed_hours": max_age_hours
        }

        logger.info(f"[{table_name}] Freshness check: {'PASSED' if passed else 'FAILED'} "
                    f"(Data age: {details['age_hours']} hours vs threshold {max_age_hours} hours)")

        return QualityCheckResult(
            check_name="freshness",
            table_name=table_name,
            passed=passed,
            details=details
        )

    def run_suite(self, df: pd.DataFrame, suite_path: Path) -> List[QualityCheckResult]:
        """Runs a complete test suite defined in a JSON file against a DataFrame.

        Args:
            df: Input DataFrame.
            suite_path: Path to expectations JSON suite file.

        Returns:
            List of QualityCheckResult objects.
        """
        if not suite_path.exists():
            raise FileNotFoundError(f"Expectations suite not found: {suite_path}")

        with open(suite_path, "r", encoding="utf-8") as f:
            suite = json.load(f)

        table_name = suite.get("table_name", suite_path.stem.split("_")[0])
        checks = suite.get("checks", {})
        results = []

        # 1. Run Completeness Checks
        if "completeness" in checks:
            comp = checks["completeness"]
            results.extend(self.check_completeness(
                df, table_name, comp.get("columns", []), comp.get("threshold", 0.98)
            ))

        # 2. Run Uniqueness Checks
        if "uniqueness" in checks:
            uniq = checks["uniqueness"]
            results.extend(self.check_uniqueness(
                df, table_name, uniq.get("columns", [])
            ))

        # 3. Run Range Checks
        if "ranges" in checks:
            ranges = checks["ranges"]
            # Convert range lists [min, max] to tuples
            range_specs = {col: (val[0], val[1]) for col, val in ranges.items()}
            results.extend(self.check_value_ranges(
                df, table_name, range_specs
            ))

        # 4. Run Allowed Values Checks
        if "allowed_values" in checks:
            allowed = checks["allowed_values"]
            results.extend(self.check_allowed_values(
                df, table_name, allowed
            ))

        # 5. Run Freshness Checks
        if "freshness" in checks:
            fresh = checks["freshness"]
            results.append(self.check_freshness(
                df, table_name, fresh.get("column"), fresh.get("max_age_hours", 24)
            ))

        return results

    def generate_report(self, results: List[QualityCheckResult], output_path: Optional[Path] = None) -> str:
        """Generates a text summary report of all quality check results.

        Args:
            results: List of check results.
            output_path: Optional file path to save the report.

        Returns:
            Report summary string.
        """
        passed_count = sum(1 for r in results if r.passed)
        failed_count = len(results) - passed_count
        
        report_lines = [
            "=" * 60,
            "DATA QUALITY CHECK REPORT",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "-" * 60,
            f"Total Checks  : {len(results)}",
            f"Passed Checks : {passed_count} ({(passed_count/len(results)*100):.1f}% if len(results) > 0 else 100%)",
            f"Failed Checks : {failed_count}",
            "=" * 60,
            "",
            "Detailed Results:"
        ]

        # Group by table
        table_results = {}
        for r in results:
            table_results.setdefault(r.table_name, []).append(r)

        for table, check_list in table_results.items():
            report_lines.append(f"\nTable: {table.upper()}")
            report_lines.append("-" * 30)
            for r in check_list:
                status = "PASS" if r.passed else "FAIL"
                report_lines.append(f"  [{status}] {r.check_name}")
                if not r.passed:
                    report_lines.append(f"     Details: {r.details}")

        report = "\n".join(report_lines)

        if output_path:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(report)
            logger.info(f"Data quality report saved to {output_path}")

        return report
