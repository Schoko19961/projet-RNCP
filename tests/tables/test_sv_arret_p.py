from tables.sv_arret_p import load_sv_arret_p
from pytest_mock import mocker
import pytest
import datetime

@pytest.mark.parametrize(("start_date", "expected"), [
    (None, {}),
    (datetime.datetime(2022, 3,12,23,59,00), {"backintime": datetime.datetime(2022, 3,12,23,59,00).strftime("%Y-%m-%dT%H:%M:%SZ")})
])
def test_load_sv_arret_p(start_date, expected, mocker):
    mock1 = mocker.patch('tables.sv_arret_p.get_data')

    
    load_sv_arret_p(start_date=start_date)
    mock1.assert_called_once_with("sv_arret_p", expected)