import pandas as pd
import pytest
from api.request_storage import RequestStorage


@pytest.fixture
def request_storage():
    return RequestStorage()


def test_add_request(request_storage):
    request1 = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    request2 = pd.DataFrame({'col1': [5, 6], 'col2': [7, 8]})
    request3 = pd.DataFrame({'col1': [9, 10], 'col2': [11, 12]})
    request4 = pd.DataFrame({'col1': [13, 14], 'col2': [15, 16]})
    request5 = pd.DataFrame({'col1': [17, 18], 'col2': [19, 20]})

    request_storage.add_request(request1)
    request_storage.add_request(request2)
    request_storage.add_request(request3)
    request_storage.add_request(request4)
    assert len(request_storage.request_storage) == 4

    request_storage.add_request(request5)
    assert len(request_storage.request_storage) == 4
    assert request_storage.request_storage[0].equals(request2)


def test_get_combined_dataframe(request_storage):
    request1 = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    request2 = pd.DataFrame({'col1': [5, 6], 'col2': [7, 8]})
    request3 = pd.DataFrame({'col1': [9, 10], 'col2': [11, 12]})
    request4 = pd.DataFrame({'col1': [13, 14], 'col2': [15, 16]})

    request_storage.add_request(request1)
    request_storage.add_request(request2)
    request_storage.add_request(request3)
    request_storage.add_request(request4)

    combined_df = request_storage.get_combined_dataframe()
    assert isinstance(combined_df, pd.DataFrame)
    assert combined_df.equals(pd.concat([request1, request2, request3, request4]))