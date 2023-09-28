import json
import pandas as pd
import pytest
from unittest.mock import patch, Mock
from api.api import get_data

@pytest.fixture
def mock_http_client():
    with patch("api.api.get_http_client") as mock_get_http_client:
        http_mock = Mock()
        http_mock.request.return_value = Mock(data=json.dumps({
            "features": [
                {
                    "properties": {
                        "id": 1,
                        "name": "Test 1"
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": [1.0, 2.0]
                    }
                },
                {
                    "properties": {
                        "id": 2,
                        "name": "Test 2"
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": [3.0, 4.0]
                    }
                }
            ]
        }).encode())
        mock_get_http_client.return_value = http_mock
        yield http_mock

def test_get_data_without_params(mock_http_client):
    # Test with no query params
    result = get_data("test_table", {})
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert set(result.columns) == {"id", "name", "coordinates"}

def test_get_data_with_params(mock_http_client):
    # Test with query params
    result = get_data("test_table", {"name": "Test 1"})
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert set(result.columns) == {"id", "name", "coordinates"}

def test_get_data_error(mock_http_client):
    # Test with error
    mock_http_client.request.side_effect = Exception("Test error")
    result = get_data("test_table", {})
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0