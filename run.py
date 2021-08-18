import os

import pandas as pd
from tabulate import tabulate

from weathercalculator import App, Utils

pyspark_submit_args = "--executor-memory 10g pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args

if __name__ == "__main__":
    # data_dir_path = "data/uncompressed"
    # df = App.daily_min_max(data_dir_path)

    path = "tests/data/daily_min_max_temperatures.csv"
    df = pd.read_csv(path)
    df["Dates"] = pd.to_datetime(df["Dates"])
    df = df.set_index("Dates").sort_index(ascending=True)

    heat_waves = App.compute_heat_waves(df)
    waves_date_formatted = Utils.format_waves(heat_waves)
    print("Hot waves")
    print(
        tabulate(
            waves_date_formatted,
            headers=[
                "From date ",
                "To date (inc.)",
                "Duration (in days)",
                "Number of tropical days",
                "Max temperature",
            ],
            tablefmt="orgtbl",
        )
    )

    cold_waves = App.compute_cold_waves(df)
    waves_date_formatted = Utils.format_waves(cold_waves)
    print("Cold waves")
    print(
        tabulate(
            waves_date_formatted,
            headers=[
                "From date ",
                "To date (inc.)",
                "Duration (in days)",
                "Number of high frost days",
                "Min temperature",
            ],
            tablefmt="orgtbl",
        )
    )
