import os
from os import listdir
from os.path import isfile, join
from collections import deque
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
from pyspark.sql.functions import max as pyspark_max
from pyspark.sql.functions import min as pyspark_min
from pyspark.sql.functions import row_number, to_date
from pyspark.sql.types import (
    FloatType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from weathercalculator.FileParser import FileParser
from weathercalculator.Utils import pairwise_union
from weathercalculator.ValueTypes import ValueTypes


def daily_max_min(dir_path):
    spark = SparkSession.builder.appName("compute_heat_waves").getOrCreate()
    spark_context = spark.sparkContext

    column_names = ["DTG", "NAME", "TX_DRYB_10"]
    column_types = {
        column_names[0]: ValueTypes.TimeStamp,
        column_names[1]: ValueTypes.String,
        column_names[2]: ValueTypes.Float,
    }
    data_frame_schema = [
        StructField(column_names[0], TimestampType(), True),
        StructField(column_names[1], StringType(), True),
        StructField(column_names[2], FloatType(), True),
    ]
    schema = StructType(data_frame_schema)

    abs_dir_path = os.path.abspath(dir_path)
    all_files_path = [
        join(abs_dir_path, f)
        for f in listdir(abs_dir_path)
        if isfile(join(abs_dir_path, f))
    ]
    dfs = []
    for iteration, file_path in enumerate(all_files_path):
        try:
            file_parser = FileParser(
                file_path=file_path,
                spark_context=spark_context,
                header_character="#",
                filter_column="NAME",
                filter_value="De Bilt",
                header_estimated_length=100,
                column_names=column_names,
                column_types=column_types,
            )
            df = file_parser.parse().toDF(schema=schema)
            print("Iteration {}".format(iteration))
            dfs.append(df)
        except ValueError:
            print("error found for file {}, iteration {}".format(file_path, iteration))

    # Unify all dataframes
    union_df = pairwise_union(dfs)

    # Add a dates column
    # count the distinct timestamps within the day
    # find the maximum and the minimum temperature
    # order by dates
    union_df = (
        union_df.withColumn("Dates", to_date(col("DTG")))
        .groupBy("Dates")
        .agg(countDistinct("DTG"), pyspark_max("TX_DRYB_10"), pyspark_min("TX_DRYB_10"))
        .orderBy("Dates")
    )

    # Now the dataset is reduced to a small size panda dataframe, where we can to iterate over
    union_df_final_pd = union_df.toPandas().set_index("Dates")

    return union_df_final_pd

def compute_heat_waves(dir_path, duration=5, temperature=25, duration_max_temperature=3, max_temperature=30):

    df = daily_max_min(dir_path)
    #union_df_final_pd.to_csv("union_df_final_pd.csv", header=True)

    are_last_days_hot = False
    start_hot_days = None
    end_hot_days = None
    num_tropical_days = 0
    heat_waves = []
    max_temp_column = 'max(TX_DRYB_10)'

    index_deque = deque(maxlen=duration)
    for date in df.index:
        index_deque.append(date)
        if len(index_deque) == duration:
            df_slice = df.loc[index_deque]
            min_temp = np.min(df_slice[max_temp_column].values)

            if min_temp > temperature and not are_last_days_hot:
                start_hot_days = index_deque[0]
                end_hot_days = index_deque[-1]
                num_tropical_days = num_tropical_days + len(
                    df_slice.loc[df[max_temp_column] > max_temperature].values)
            if min_temp > temperature and are_last_days_hot:
                end_hot_days = index_deque[-1]
                if df_slice.loc[index_deque[-1]][max_temp_column] > max_temperature:
                    num_tropical_days = num_tropical_days + 1
            if min_temp <= temperature and are_last_days_hot:
                are_last_days_hot = False
                if num_tropical_days >= duration_max_temperature:
                    heat_waves.append([start_hot_days, end_hot_days, num_tropical_days])
                    
    return heat_waves

def compute_cold_waves(dir_path, duration=5, temperature=25):
