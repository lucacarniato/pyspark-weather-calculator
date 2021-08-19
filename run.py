import os

import pandas as pd
from tabulate import tabulate

from weathercalculator import Calculators, Transformers, Utils
from os.path import join, abspath


if __name__ == "__main__":

    # activate in production
    raw_data_path = "/job/raw_data/uncompressed"
    df = Transformers.daily_min_max(raw_data_path)

    # used cached data
    # cache_path = "/job/data/cache/2003_4_1_2019_3_31"
    # abs_cache_path = abspath(cache_path)
    # print("Abs file path for chache file is "+abs_cache_path)
    # df = pd.read_csv(abs_cache_path)
    # df["Dates"] = pd.to_datetime(df["Dates"])
    # df = df.set_index("Dates").sort_index(ascending=True)

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
