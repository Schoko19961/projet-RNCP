from tables.sv_chem_l import load_sv_chem_l
from pytest_mock import MockerFixture
import pytest
import datetime

@pytest.mark.parametrize(("start_date", "expected"), [
    (None, {}),
    (datetime.datetime(2022, 3,12,23,59,00), {"backintime": datetime.datetime(2022, 3,12,23,59,00).strftime("%Y-%m-%dT%H:%M:%SZ")})
])
def test_load_sv_chem_l(start_date, expected, mocker:MockerFixture):
    mock1 = mocker.patch('tables.sv_chem_l.get_data')

    
    load_sv_chem_l(start_date=start_date)
    mock1.assert_called_once_with("sv_chem_l", expected)