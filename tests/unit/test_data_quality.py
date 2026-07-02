import pytest
import pandas as pd
from src.quality.data_quality_checker import DataQualityChecker

def test_quality_completeness():
    df = pd.DataFrame({"col1": [1, 2, None, 4]})
    dq = DataQualityChecker()
    
    # 75% completeness. If threshold is 0.70, it should pass. If 0.80, it should fail.
    res_pass = dq.check_completeness(df, "test", ["col1"], threshold=0.70)
    assert res_pass[0].passed
    
    res_fail = dq.check_completeness(df, "test", ["col1"], threshold=0.80)
    assert not res_fail[0].passed

def test_quality_uniqueness():
    df = pd.DataFrame({"id": [1, 2, 3, 3]})
    dq = DataQualityChecker()
    
    res = dq.check_uniqueness(df, "test", ["id"])
    assert not res[0].passed
    assert res[0].details["duplicate_rows"] == 1

def test_quality_ranges():
    df = pd.DataFrame({"age": [18, 25, 99, 120]})
    dq = DataQualityChecker()
    
    # Range check [10, 100]. Row with 120 should fail.
    res = dq.check_value_ranges(df, "test", {"age": (10, 100)})
    assert not res[0].passed
    assert res[0].details["out_of_bounds_count"] == 1
