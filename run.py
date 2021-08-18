import os

from weathercalculator import App

pyspark_submit_args = "--executor-memory 10g pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args

if __name__ == "__main__":
    App.compute_heat_waves("data/uncompressed")
