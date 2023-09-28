from tables.sv_cours_a import load_sv_cours_a
from pytest_mock import MockerFixture
import pytest
import datetime

@pytest.mark.parametrize(("start_date", "expected"), [
    (None, {}),
    (datetime.datetime(2022, 3,12,23,59,00), {"backintime": datetime.datetime(2022, 3,12,23,59,00).strftime("%Y-%m-%dT%H:%M:%SZ")})
])
def test_load_sv_cours_a(start_date, expected, mocker:MockerFixture):
    mock1 = mocker.patch('tables.sv_cours_a.get_data')

    
    load_sv_cours_a(start_date=start_date)
    mock1.assert_called_once_with("sv_cours_a", expected)