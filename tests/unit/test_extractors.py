import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock
from src.extract.api_extractor import ApiExtractor
from src.extract.csv_extractor import CsvExtractor
from src.extract.data_generator import EcommerceDataGenerator

def test_data_generator():
    generator = EcommerceDataGenerator(volume=100)
    data = generator.generate_all()
    
    assert "customers" in data
    assert "products" in data
    assert "orders" in data
    
    assert len(data["customers"]) == 100
    assert len(data["products"]) == 50
    assert len(data["orders"]) >= 100  # volume 100 plus 1% duplicates

@patch("requests.get")
def test_api_extractor(mock_get):
    # Mock API response
    mock_response = MagicMock()
    mock_response.json.return_value = [
        {"id": 1, "title": "Product 1", "price": 10.0, "rating": {"rate": 4.5, "count": 10}},
        {"id": 2, "title": "Product 2", "price": 20.0, "rating": {"rate": 3.8, "count": 5}}
    ]
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response
    
    api = ApiExtractor(rate_limit_pause_seconds=0.0)
    df = api.extract_products()
    
    assert not df.empty
    assert len(df) == 2
    assert "rating" in df.columns
    assert df["rating"].tolist() == [4.5, 3.8]

def test_csv_extractor(tmp_path):
    csv_file = tmp_path / "test.csv"
    df_raw = pd.DataFrame({"id": [1, 2], "val": ["a", "b"]})
    df_raw.to_csv(csv_file, index=False, sep=";")
    
    extractor = CsvExtractor()
    df_extracted = extractor.extract(csv_file, expected_columns=["id", "val"])
    
    assert len(df_extracted) == 2
    assert df_extracted["val"].tolist() == ["a", "b"]
