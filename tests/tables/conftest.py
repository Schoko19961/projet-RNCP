import pandas as pd
import pytest

@pytest.fixture
def sv_horai_a_1() -> pd.DataFrame:
    return pd.DataFrame(data={
        "hor_theo": ["2022-01-01T00:00:00Z"],
        "etat": ["etat"],
        "type": ["type"],
        "source": ["source"],
        "rs_sv_arret_p": ["rs_sv_arret_p"],
        "rs_sv_cours_a": ["rs_sv_cours_a"],
        "hor_real": ["hor_real"],
    })

@pytest.fixture
def sv_horai_a_2() -> pd.DataFrame:
    return pd.DataFrame(data={
        "hor_theo": ["2022-01-01T20:00:00Z"],
        "etat": ["etat"],
        "type": ["type"],
        "source": ["source"],
        "rs_sv_arret_p": ["rs_sv_arret_p"],
        "rs_sv_cours_a": ["rs_sv_cours_a"],
        "hor_real": ["hor_real"],
    })

@pytest.fixture
def sv_horai_a_3() -> pd.DataFrame:
    return pd.DataFrame(data={
        "hor_theo": ["2022-01-03T00:00:00Z"],
        "etat": ["etat"],
        "type": ["type"],
        "source": ["source"],
        "rs_sv_arret_p": ["rs_sv_arret_p"],
        "rs_sv_cours_a": ["rs_sv_cours_a"],
        "hor_real": ["hor_real"],
    })