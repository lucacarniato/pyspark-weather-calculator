import pandas as pd
from weathercalculator.App import compute_heat_waves


def test_compute_heat_waves():
    path = "../data/daily_min_max_temperatures.csv"
    df = pd.read_csv(path)
    df["Dates"] = pd.to_datetime(df["Dates"])
    df = df.set_index("Dates").sort_index(ascending=True)

    compute_heat_waves(
        df=df,
        duration=5,
        temperature=25,
        duration_max_temperature=3,
        max_temperature=30,
    )
