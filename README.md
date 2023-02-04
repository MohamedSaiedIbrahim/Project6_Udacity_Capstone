# Udacity-Data-Engineering-Capstone
### Data Engineering Capstone Project

#### Project Summary
This project will build up a data warehouse as a single-source-of-truth database by combining four data sets containing immigration data, airport codes, demographics of US cities and global temperature data. 

## Datasets:
- i94 Immigration Sample Data: Sample data of immigration records from the US National Tourism and Trade Office. This is the main data source that will serve as the Fact table in the schema. This data comes from https://travel.trade.gov/research/reports/i94/historical/2016.html.
- World Temperature Data world_temperature. This dataset contains temperature data in various cities from the 1700’s to 2013. Although the data is only recorded until 2013, we can use this as an average/gauge of temperature in 2017. This data comes from https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data.
- US City Demographic Data: Data about the demographics of US cities. This dataset includes information on the population of all US cities such as race, household size and gender. This data comes from https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/.
- Airport Codes: This table contains the airport codes for the airports in corresponding cities. This data comes from https://datahub.io/core/airport-codes#data.
​
## Data Model:
According to Kimball Modelling and as per the nature of the 4 combined Data Sources. The main target of combining those 4 Data Sources is to track the immigration actions into the USA. Accordingly:

- 1. Identify the Facts:
    - Fact tables focus on the occurrences of a singular business process, and have a one-to-one relationship with the dimention tables.
    - The fact table identified in this project is:
        - immigration_fact
        
- 2. Identify the Dimensions:
    - Dimension tables provide any quantitative or metric around the fact event or business process.
    - The dimensions identified in this project are:
        - migrant_dim
        - status_dim
        - visa_dim
        - state_dim
        - time_dim
        - airport_codes
        - temperature_dim
        - country_codes
        - country_temperature
    
The above mentioned tables will be developed in a Relational Database Management System to form a Star Schema, which can be used in Data analytics and BI insights activities.

please refer to Proposed Star Schema.PNG

#### 3.2 Mapping Out Data Pipelines
##### List the steps necessary to pipeline the data into the chosen data model:

1. Read and load into staging Dataframes
2. Apply cleansing over all those DFs
3. Perform some ETL trasformation rules using pyspark
4. Model them into Fact & Dimention tables using start schema
5. Write those DFs into S3 buckets
6. Extract again and apply Data Quality checks

#### Tools and Technologies
1. AWS S3 for data storage
2. Pandas for sample data set exploratory data analysis
3. PySpark for large data set data processing to transform staging table to dimensional table

#### Data Update Frequency
1. All tables should be update in an append-only mode.
2. Demographic readings can be updated annually, while the immigration and temperature readings can be updated on monthly basis

#### Considerations/Assumptions:
##### The data was increased by 100x:
We can use more powerful tools for massive data processing and storage like (Amazon EMR & Amazon RedShift)
or use nosql techniques like (Apache Cassandra)

##### The data populates a dashboard that must be updated on a daily basis by 7am every day.
We can use Apache Airflow to hadle the daily scheduling. Also it can be used to apply data quality checks and get some useful notifications.

##### The database needed to be accessed by 100+ people
AWS Redshift can handle a massive amount of concurrent sessions. I believe it's worthy to convert the whole solution to be on cloud based if such number of hits will be there.

