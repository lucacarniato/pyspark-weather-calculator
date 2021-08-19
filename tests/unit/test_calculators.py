from datetime import datetime as dt

import pandas as pd
from weathercalculator.Calculators import compute_cold_waves, compute_heat_waves


def test_compute_heat_waves():
    """Tests `compute_heat_waves` computes the heat wave dates, duration and number of tropical days correctly on the
    reduced data
    """
    path = "../data/2003_4_1_2019_3_31"
    df = pd.read_csv(path)
    df["Dates"] = pd.to_datetime(df["Dates"])
    df = df.set_index("Dates").sort_index(ascending=True)

    heat_waves = compute_heat_waves(
        df=df,
        duration=5,
        temperature=25,
        min_tropical_days_num=3,
        max_temperature=30,
    )

    assert len(heat_waves) == 9
    assert heat_waves[0] == [
        dt(2003, 7, 31),
        dt(2003, 8, 13),
        13,
        7,
        35,
    ]


def test_compute_cold_waves():
    """Tests `compute_heat_waves` computes the heat wave dates, duration and number of tropical days correctly on the
    reduced data
    """
    path = "../data/2003_4_1_2019_3_31"
    df = pd.read_csv(path)
    df["Dates"] = pd.to_datetime(df["Dates"])
    df = df.set_index("Dates").sort_index(ascending=True)

    cold_waves = compute_cold_waves(
        df=df,
        duration=5,
        temperature=0.0,
        min_high_frost_days_num=3,
        high_frost_temperature=-10.0,
    )

    assert len(cold_waves) == 1
    assert cold_waves[0] == [
        dt(2012, 1, 30),
        dt(2012, 2, 8),
        9,
        6,
        -18.8,
    ]
