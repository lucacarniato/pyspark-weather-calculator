# Assessment: Heatwave in The Netherlands
Note that this assessment is not in the public domain, sharing this assessment is disallowed.
For this assessment you will create an application to calculate heatwaves in the Netherlands for the past 20 years.

In real life, the presence of a heatwave is calculated by meteorological institutions and widely used by municipalities and other authorities to prevent major power outages, physical damages, wildfires, and other negative consequences for people.

## Assessment
The definition of a heatwave varies per country, but the general idea stays the same. A heatwave is a period of excessively hot weather. 

For this assessment we will use the [definition](https://en.wikipedia.org/wiki/Heat_wave) of a heatwave as provided by the [KNMI](https://en.wikipedia.org/wiki/Royal_Netherlands_Meteorological_Institute).

The KNMI defines a heat wave as a period of at least 5 consecutive days in which the maximum temperature in De Bilt exceeds 25 °C.
Additionally, during this 5 day period, the maximum temperature in De Bilt should exceed 30 °C for at least 3 days.

### Required solution
#### 1. Implement an application to calculate heatwaves for The Netherlands starting from 2003 (80%)

The output should contain the following information (example):

| From date   | To date (inc.) | Duration (in days) | Number of tropical days | Max temperature |
| ------------| -------------  | ------------------ | ----------------------- | --------------- |
| 31 jul 2003 | 13 aug 2003    | 14                 | 7                       | 35.0            |

#### 2. Presentation (20%)
See Evaluation below for the expectations of the presentation.

### Bonus points
If you have some time to spare, or want to show off your skills you could extend the assignment with one of the following:

- Write your application in such way that it also calculates coldwaves. A coldwave is a period of excessively cold weather with a minimum of five consecutive days below freezing (max temperature below 0.0 °C) and at least three days with high frost (min temperature is lower than -10.0 °C).
- Make your application run in a Docker container(s).
- Implement a REST API to get the result data set.
- Create an Airflow DAG which downloads new data using the KNMI api, and run the application.

### Languages / frameworks / tools
A requirement is that the solution should be easily horizontally scalable (across more than one machine) and easy extendable for other possible processing of input data.

We don't impose a particular framework, but Spark is the preferred framework for this assessment. 
We do however limit you to use a mainstream programming language; eg Python, Java or Scala. 
Moreover, for the sake of keeping things interesting, we do *rule out* SQL based solutions, such as Hive and Spark SQL (eg the use of the `.sql(...)` method).

### Data 
Data is available [here](https://gddassesmentdata.blob.core.windows.net/knmi-data/data.tgz?sp=r&st=2020-08-24T13:54:36Z&se=2024-01-01T22:54:36Z&spr=https&sv=2019-12-12&sr=b&sig=jAc9nOsVY6PT1685MTH0P2j%2FYTK6zhbYNKHR6argLC8%3D). The data contains the following information:

| Field                | Description                                                         |
| ---------------------|---------------------------------------------------------------------|
| `DTG`                | date of measurement                                                 |
| `LOCATION`           | location of the meteorological station                              |
| `NAME`               | name of the meteorological station                                  |
| `LATITUDE`           | in degrees (WGS84)                                                  |
| `LONGITUDE`          | in degrees (WGS84)                                                  |
| `ALTITUDE`           | in 0.1 m relative to Mean Sea Level (MSL)                           |
| `U_BOOL_10`          | air humidity code boolean 10' unit                                  |
| `T_DRYB_10`          | air temperature 10' unit Celcius degrees                            |
| `TN_10CM_PAST_6H_10` | air temperature minimum 0.1m 10' unit Celcius degrees               |
| `T_DEWP_10`          | air temperature derived dewpoint - 10' unit Celcius degrees         |
| `T_DEWP_SEA_10`      | air temperature derived dewpoint- sea 10' unit Celcius degrees      |
| `T_DRYB_SEA_10`      | air temperature height oil platform 10 minutes unit Celcius degrees |
| `TN_DRYB_10`         | air temperature minimum 10' unit Celcius degrees                    |
| `T_WETB_10`          | air temperature derived wet bulb- 10' unit Celsius degrees          |
| `TX_DRYB_10`         | air temperature maximum 10' unit Celcius degrees                    |
| `U_10`               | relative air humidity 10' unit %                                    |
| `U_SEA_10`           | is relative sea air humidity 10' unit %                             |

The `De Bilt` weather station is indicated by the Location `260_T_a`.

__IMPORTANT:__ Some columns in data might be empty. Empty values are excluded from calculations. Be aware that format of data might change from year to year of meteorological observations. 

### Evaluation
The evaluation of your solution is based on a presentation given by you for two members of our team and on the solution itself. Please provide us with the working solution beforehand (the latest the day before you presentation) by sending it to [assessment@godatadriven.com](mailto:assessment@godatadriven.com).

While the given problem is seemingly simple, don't treat it as just a programming exercise for the sake of validating that you know how to write code (in that case, we'd have asked you to write Quicksort or some other very well defined problem). Instead, treat it as if there will be a real user depending on this solution for detecting heatwaves. Obviously, you'd like to start with a simple solution that works, but we value evidence that you have thought about the problem and perhaps expose ideas for improvement upon this.

The goal of this assignment and the presentation is to assess candidates' skills in the following areas:

- Computer science
- Software development
- Distributed systems
- Coding productivity
- Presentation skills

While not all of the above are fully covered by this assignment, we believe we can do a decent job of assessing these based on the solution, the presentation and subsequent Q&A after the presentation. Apart from the problem interpretation, we value evidence of solid software engineering principles, such as testability, separation of concerns, fit-for-production code, etc.

## Note
If you have any questions or want clarification on the requirements, please email [assessment@godatadriven.com](mailto:assessment@godatadriven.com).
