from datetime import timedelta

import numpy as np


def compute_heat_waves(
    df, duration=5, temperature=25, duration_max_temperature=3, max_temperature=30
):
    """ "Heat wave is a period of at least 5 consecutive days in which the maximum temperature in De Bilt exceeds 25 °C.
    Additionally, during this 5 day period, the maximum temperature in De Bilt should exceed 30 °C for at least 3 days."""

    are_last_days_hot = False
    start_hot_days = None
    end_hot_days = None
    num_tropical_days = 0
    max_temp_in_heat_wave = -1000.0
    heat_waves = []
    max_temp_column = "max(TX_DRYB_10)"

    for date in df.index[duration - 1 :]:

        start_date = date + timedelta(days=-(duration - 1))
        df_slice = df.loc[start_date:date]
        min_temp = np.min(df_slice[max_temp_column].values)

        if min_temp > temperature and not are_last_days_hot:
            are_last_days_hot = True
            start_hot_days = start_date
            end_hot_days = date
            tropical_temperatures = df_slice.loc[
                df_slice[max_temp_column] > max_temperature
            ].values
            max_temp_in_heat_wave = max(
                max_temp_in_heat_wave, np.max(df_slice[max_temp_column].values)
            )
            num_tropical_days = num_tropical_days + len(tropical_temperatures)

        if min_temp > temperature and are_last_days_hot:
            end_hot_days = date
            last_max_temperature = df_slice.loc[date][max_temp_column]
            if last_max_temperature > max_temperature:
                max_temp_in_heat_wave = max(max_temp_in_heat_wave, last_max_temperature)
                num_tropical_days = num_tropical_days + 1

        if min_temp <= temperature and are_last_days_hot:
            are_last_days_hot = False
            if num_tropical_days >= duration_max_temperature:
                heat_wave_duration = end_hot_days - start_hot_days
                heat_waves.append(
                    [
                        start_hot_days,
                        end_hot_days,
                        heat_wave_duration.days,
                        num_tropical_days,
                        max_temp_in_heat_wave,
                    ]
                )
            start_hot_days = None
            end_hot_days = None
            num_tropical_days = 0
            max_temp_in_heat_wave = -1000.0

    return heat_waves


def compute_cold_waves(
    df, duration=5, temperature=0, duration_min_temperature=3, min_temperature=-10.0
):
    """A coldwave is a period of excessively cold weather with a minimum of five consecutive days below freezing
    (max temperature below 0.0 °C) and at least three days with high frost (min temperature is lower than -10.0 °C)."""

    are_last_days_freezing = False
    start_freezing_days = None
    end_freezing_days = None
    num_high_frost_days = 0
    min_temp_in_cold_wave = 1000.0
    cold_waves = []
    max_temp_column = "max(TX_DRYB_10)"
    min_temp_column = "min(TX_DRYB_10)"

    for date in df.index[duration - 1 :]:

        start_date = date + timedelta(days=-(duration - 1))
        df_slice = df.loc[start_date:date]

        max_temp = np.max(df_slice[max_temp_column].values)

        if max_temp < temperature and not are_last_days_freezing:
            are_last_days_freezing = True
            start_freezing_days = start_date
            end_freezing_days = date
            high_frost_temperatures = df_slice.loc[
                df_slice[min_temp_column] < min_temperature
            ].values
            num_high_frost_days = num_high_frost_days + len(high_frost_temperatures)
            min_temp_in_cold_wave = min(
                min_temp_in_cold_wave, np.min(df_slice[min_temp_column].values)
            )

        if max_temp < temperature and are_last_days_freezing:
            end_freezing_days = date
            last_min_temperature = df_slice.loc[date][min_temp_column]
            if last_min_temperature < min_temperature:
                num_high_frost_days = num_high_frost_days + 1
                min_temp_in_cold_wave = min(min_temp_in_cold_wave, last_min_temperature)

        if max_temp >= temperature and are_last_days_freezing:
            are_last_days_freezing = False
            if num_high_frost_days >= duration_min_temperature:
                high_frost_duration = end_freezing_days - start_freezing_days
                cold_waves.append(
                    [
                        start_freezing_days,
                        end_freezing_days,
                        high_frost_duration.days,
                        num_high_frost_days,
                        min_temp_in_cold_wave,
                    ]
                )
            start_freezing_days = None
            end_freezing_days = None
            num_high_frost_days = 0
            min_temp_in_cold_wave = 1000.0

    return cold_waves
