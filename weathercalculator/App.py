import os
from os import listdir
from os.path import isfile, join

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
from pyspark.sql.window import Window

from weathercalculator.FileParser import FileParser
from weathercalculator.Utils import pairwise_union
from weathercalculator.ValueTypes import ValueTypes


def compute_heat_waves(dir_path, duration=5, temperature=25):
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

    dfs = []
    abs_dir_path = os.path.abspath(dir_path)
    all_files_path = [
        join(abs_dir_path, f)
        for f in listdir(abs_dir_path)
        if isfile(join(abs_dir_path, f))
    ]

    for iteration, file_path in enumerate(all_files_path):
        try:
            file_parser = FileParser(
                file_path=file_path,
                spark_context=spark_context,
                header_charater="#",
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

    # Perform PySpark queries to reduce the dataset to a manageable size
    union_df = pairwise_union(dfs)
    # union_df = None
    # for i, df in enumerate(dfs):

    #    if i == 0:
    #        union_df = df
    #        continue

    #    union_df = union_df.union(df).distinct()

    # Add a dates column
    # count the distinct timestamps within the day
    # find the maximum temp
    # order by dates
    union_df = (
        union_df.withColumn("Dates", to_date(col("DTG")))
        .groupBy("Dates")
        .agg(countDistinct("DTG"), pyspark_max("TX_DRYB_10"), pyspark_min("TX_DRYB_10"))
        .orderBy("Dates")
    )

    # Add a row_number column to use for the windows later on
    # windowSpec = Window.orderBy(col("Dates")).partitionBy("Dates")
    # union_df_row_number = union_df.withColumn(
    #    "row_number", row_number().over(windowSpec)
    # )

    # apply a window and compute the minimum and maximum temperatures
    # windowSpec = (
    #    Window.orderBy(col("row_number"))
    #    .rangeBetween(-duration, 0)
    #    .partitionBy("row_number")
    # )
    # union_df_final = union_df_row_number.withColumn(
    #    "last_days_min_temp", pyspark_min("max(TX_DRYB_10)").over(windowSpec)
    # ).withColumn("last_days_max_temp", pyspark_max("max(TX_DRYB_10)").over(windowSpec))

    # Reduce to a small size panda dataframe to iterate over
    union_df_final_pd = union_df.repartition(10).toPandas().set_index("Dates")

    union_df_final_pd.to_csv("union_df_final_pd.csv", header=True)
