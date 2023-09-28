from tables.sv_horai_a import load_data
from pytest_mock import MockerFixture
from datetime import datetime
import pandas as pd

def test_load_data(sv_horai_a_1:pd.DataFrame, sv_horai_a_2:pd.DataFrame, sv_horai_a_3:pd.DataFrame,mocker:MockerFixture):
    start_date = datetime.now()
    mock1 = mocker.patch('tables.sv_horai_a.get_data',
                         side_effect=[sv_horai_a_1, sv_horai_a_2, sv_horai_a_3])
    
    result = load_data(start_date=start_date, step_size=24, max_days=1, use_request_storage=False)

    mock1.ass("sv_horai_a", {"backintime": start_date.strftime("%Y-%m-%dT%H:%M:%SZ")})

def test_load_data2(sv_horai_a_1:pd.DataFrame, sv_horai_a_2:pd.DataFrame, sv_horai_a_3:pd.DataFrame,mocker:MockerFixture):
    start_date = datetime.now()
    mock1 = mocker.patch('tables.sv_horai_a.get_data',
                         side_effect=[sv_horai_a_1, sv_horai_a_2, sv_horai_a_3])
    
    result = load_data(start_date=start_date, step_size=24, max_days=3, use_request_storage=False)

    assert mock1.call_count == 3

    