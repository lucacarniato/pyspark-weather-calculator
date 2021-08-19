from datetime import timedelta

import numpy as np


def compute_heat_waves(
    df, duration=5, temperature=25, min_tropical_days_num=3, max_temperature=30
):
    """Computes the heatwaves periods from a provided data frame

    Args:
        df (DataFrame): The data frame containing the dates and the minimum and maximum temperatures.
        duration (int): The minimum duration of a cold wave in days.
        temperature (float): The minimum temperature during heatwaves.
        min_tropical_days_num (int): The minimum number of tropical days.
        max_temperature (float): The min temperature during tropical days.

    Returns:
        A list containing the start and end dates of all heatwaves, their durations, the number of tropical days
        and the maximum temperatures during the heatwaves.
    """

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
            if num_tropical_days >= min_tropical_days_num:
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
    df,
    duration=5,
    temperature=0,
    min_high_frost_days_num=3,
    high_frost_temperature=-10.0,
):
    """Computes the heatwaves periods from a provided data frame

    Args:
        df (DataFrame): The data frame containing the dates and the minimum and maximum temperatures.
        duration (int): The minimum duration of a cold wave in days.
        temperature (float): The maximum temperature during the cold waves.
        min_high_frost_days_num (int): The minimum number of high frost days.
        high_frost_temperature (float): The maximum temperature during high frost days.

    Returns:
        A list containing the start and end dates of all cold waves, their duration, the number of high frost days
        and the minimum temperatures during the cold waves.
    """

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
                df_slice[min_temp_column] < high_frost_temperature
            ].values
            num_high_frost_days = num_high_frost_days + len(high_frost_temperatures)
            min_temp_in_cold_wave = min(
                min_temp_in_cold_wave, np.min(df_slice[min_temp_column].values)
            )

        if max_temp < temperature and are_last_days_freezing:
            end_freezing_days = date
            last_min_temperature = df_slice.loc[date][min_temp_column]
            if last_min_temperature < high_frost_temperature:
                num_high_frost_days = num_high_frost_days + 1
                min_temp_in_cold_wave = min(min_temp_in_cold_wave, last_min_temperature)

        if max_temp >= temperature and are_last_days_freezing:
            are_last_days_freezing = False
            if num_high_frost_days >= min_high_frost_days_num:
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
