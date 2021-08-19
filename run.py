import os

import pandas as pd
from tabulate import tabulate

from weathercalculator import Calculators, Transformers, Utils

raw_data_path = "./data/raw_data"

if __name__ == "__main__":

    # activate in production
    # df = Transformers.daily_min_max()

    # used cached data
    cache_path = "./data/cache/2003_4_1_2019_3_31"
    df = pd.read_csv(cache_path)
    df["Dates"] = pd.to_datetime(df["Dates"])
    df = df.set_index("Dates").sort_index(ascending=True)

    heat_waves = Calculators.compute_heat_waves(df)
    waves_date_formatted = Utils.format_waves(heat_waves)
    print("Heat waves")
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

    cold_waves = Calculators.compute_cold_waves(df)
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
