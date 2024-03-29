# PySpark weather calculator

PySpark weather calculator solves the challenge of calculating the Netherlands' heat waves from a large sensor data dataset (20 Gb). A heatwave is defined as follows:

"The KNMI defines a heatwave as a period of at least 5 consecutive days in which the maximum temperature in De Bilt exceeds 25 °C.
Additionally, during these 5 days, the maximum temperature in De Bilt should exceed 30 °C for at least 3 days."

The application gives the following results:

Heatwaves

| From date   | To date (inc.) | Duration (in days) | Number of tropical days | Max temperature |
| ------------| -------------  | ------------------ | ----------------------- | --------------- |
| Jul 31 2003 | Aug 13 2003    |                 13 |                       7 |            35   |
| Jun 18 2005 | Jun 24 2005    |                  6 |                       3 |            32.8 |
| Jun 30 2006 | Jul 06 2006    |                  6 |                       4 |            32   |
| Jul 15 2006 | Jul 30 2006    |                 15 |                       9 |            35.7 |
| Jul 21 2013 | Jul 27 2013    |                  6 |                       3 |            32.6 |
| Jun 30 2015 | Jul 05 2015    |                  5 |                       4 |            33.1 |
| Jun 18 2017 | Jun 22 2017    |                  4 |                       3 |            30.9 |
| Jul 15 2018 | Jul 27 2018    |                 12 |                       4 |            35.7 |
| Jul 29 2018 | Aug 07 2018    |                  9 |                       4 |            33.9 |


As a bonus point, the cold waves were also computed. Cold waves are defined as follows:

"A cold wave is a period of freezing weather with a minimum of five consecutive days below 
freezing (max temperature below 0.0 °C) and at least three days with high frost (min temperature is lower than -10.0 °C)."

Computed cold waves

| From date   | To date (inc.) | Duration (in days) | Number of high frost days | Min temperature |
| ------------| -------------  | ------------------ | ------------------------- | --------------- |
| Jan 30 2012 | Feb 08 2012    |                  9 |                         6 |           -18.8 |

To run the application in Windows copy the KNMI data into data/raw_data and execute the following docker command commands:

    docker build -t weathercalculator .
    docker run -v %cd%:/job weathercalculator /job/run.py
    
To run the application in Linux copy the KNMI data into data/raw_data and execute the following docker command commands:

    docker build -t weathercalculator .
    docker run -v $(pwd):/job weathercalculator /job/run.py

## Application design

The application is organized as follows:
+ data, containing the following sub-directories:
    + raw_data directory to store downloaded data.
    + cache directory to store the results of the transformations. These results can be reused when a client application 
    requests to calculate heatwaves within a period that has been calculated already.
+ tests:
    + data: contains the data used by the unit tests.
    + unit: contains some tests to verify the correctness of the parsing functions (tokenizers) and heat/cold waves calculators.
+ weathercalculator: contains the Python package. Most of the code is implemented as self-standing functions, 
to facilitate future implementation of an AirFlow ETL job.
    + Extractors.py: can contain the functions for downloading KNMI data and saving them into data/raw_data.
    + Transformers.py: contains the functions used to process the files contained in data/raw_data. 
    This file implements the PySpark queries used for computing the maximum and minimum daily temperatures from 10-minute data. 
    The result of daily_min_max function is a reduced pandas data frame of 144KB. 
    + Calculators.py: contains the functions for calculating the heat and cold waves from the reduced data frame.

This directory structure facilitates extending the applications with additional Extractors, Transformers, or Calculators.

# Future work

+ Implement data extractors in weathercalculator/Extractors.py for downloading data.
+ Implement an Apache Airflow ETL job
+ Implement the Cache invalidation mechanism for the cache stored in data/cache, for example when heat waves or cold waves 
need to be computed for a period outside the already computed one. 
+ Implement a REST API for using the results of the application (e.g. using Flask).
