# Weather calculator

The Python package weathercalculator solves Go Data Driven challenge of calculating the heat waves in the Netherlands,
as defined by KNMI. An heat wave is defined as follow:

"The KNMI defines a heat wave as a period of at least 5 consecutive days in which the maximum temperature in De Bilt exceeds 25 °C.
Additionally, during this 5 day period, the maximum temperature in De Bilt should exceed 30 °C for at least 3 days."

The application gives the following results:

Heat waves

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


As bonus point, the cold waves were also caomputed.Cold waves are defined as follow:

"A cold wave is a period of excessively cold weather with a minimum of five consecutive days below 
freezing (max temperature below 0.0 °C) and at least three days with high frost (min temperature is lower than -10.0 °C)."

Cold waves

| From date   | To date (inc.) | Duration (in days) | Number of high frost days | Min temperature |
| ------------| -------------  | ------------------ | ------------------------- | --------------- |
| Jan 30 2012 | Feb 08 2012    |                  9 |                         6 |           -18.8 |

## Application design

The application is organized as follow:
dir structure with what things do

The application is implemented as a Python package. 


# Future work

