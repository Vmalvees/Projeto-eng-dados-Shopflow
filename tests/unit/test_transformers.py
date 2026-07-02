import pytest
import pandas as pd
from src.transform import transformers as t

def test_standardize_strings():
    df = pd.DataFrame({"col": ["  HELLO ", "world  ", "Test"]})
    res = t.standardize_strings(df, ["col"])
    assert res["col"].tolist() == ["hello", "world", "test"]

def test_normalize_names():
    df = pd.DataFrame({"col": [" joão silva ", "MARIA oliveira", None]})
    res = t.normalize_names(df, ["col"])
    assert res["col"].iloc[0] == "João Silva"
    assert res["col"].iloc[1] == "Maria Oliveira"
    assert pd.isna(res["col"].iloc[2])

def test_clean_email():
    df = pd.DataFrame({"email": [" TEST@domain.com  ", "invalid-email", "valid@domain.co.uk", None]})
    res = t.clean_email(df, "email")
    assert res["email"].iloc[0] == "test@domain.com"
    assert pd.isna(res["email"].iloc[1])
    assert res["email"].iloc[2] == "valid@domain.co.uk"
    assert pd.isna(res["email"].iloc[3])

def test_fix_negative_values():
    df = pd.DataFrame({"val": [-10.5, 20.0, -5.0]})
    res_abs = t.fix_negative_values(df, ["val"], strategy="absolute")
    assert res_abs["val"].tolist() == [10.5, 20.0, 5.0]

    res_zero = t.fix_negative_values(df, ["val"], strategy="zero")
    assert res_zero["val"].tolist() == [0.0, 20.0, 0.0]

def test_round_currency():
    df = pd.DataFrame({"val": [10.555, 20.1234, 5.0]})
    res = t.round_currency(df, ["val"], decimals=2)
    assert res["val"].tolist() == [10.56, 20.12, 5.0]

def test_remove_future_dates():
    ref_date = pd.Timestamp("2024-06-01")
    df = pd.DataFrame({"date": ["2024-05-15", "2024-06-15", "2024-05-30"]})
    res = t.remove_future_dates(df, "date", reference_date=ref_date)
    assert len(res) == 2
    assert "2024-06-15" not in res["date"].astype(str).tolist()

def test_remove_duplicates():
    df = pd.DataFrame({"id": [1, 2, 2, 3], "val": ["a", "b", "c", "d"]})
    res = t.remove_duplicates(df, subset=["id"])
    assert len(res) == 3
    assert res["val"].tolist() == ["a", "b", "d"]

def test_handle_nulls():
    df = pd.DataFrame({"val": [1.0, None, 3.0], "val_str": ["a", None, "c"]})
    res = t.handle_nulls(df, {"val": "fill_median", "val_str": "fill_value:missing"})
    assert res["val"].tolist() == [1.0, 2.0, 3.0]
    assert res["val_str"].tolist() == ["a", "missing", "c"]
