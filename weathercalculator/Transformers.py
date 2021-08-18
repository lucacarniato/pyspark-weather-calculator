import os
from datetime import datetime as dt
from os import listdir
from os.path import isfile, join

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
from pyspark.sql.functions import max as pyspark_max
from pyspark.sql.functions import min as pyspark_min
from pyspark.sql.functions import to_date
from pyspark.sql.types import (FloatType, StringType, StructField, StructType,
                               TimestampType)

from weathercalculator.FileParsers import FileParser
from weathercalculator.Utils import pairwise_union
from weathercalculator.ValueTypes import ValueTypes


def daily_min_max(
    raw_data_path,
    start_date=dt(2003, 4, 1, 00, 00, 00),
    end_date=dt(2019, 3, 31, 00, 00, 00),
):
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

    abs_dir_path = os.path.abspath(raw_data_path)
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

            # process only files required by the time interval
            start_date_local_file, end_date_local_file = file_parser.parse_period()
            if start_date_local_file >= start_date and end_date_local_file <= end_date:
                df = file_parser.parse().toDF(schema=schema)
                print("Iteration {}".format(iteration))
                dfs.append(df)
        except ValueError:
            print(
                "Error found for file {}, iteration {}, quitting application".format(
                    file_path, iteration
                )
            )

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
