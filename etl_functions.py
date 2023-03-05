# Import Libraries
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, count, sum, mean, round, upper, lower
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id
from pyspark.sql.types import *
from pyspark.sql.types import DoubleType, StringType, IntegerType, FloatType, DateType
from pyspark.sql import functions as F

# Add functions as needed to perform necessary operations
def migrant_dimention(input_df, output_path):
    """
        Create migrant dimention table and load into S3 bucket as Parquet files.
        
        :i/p input_df: input DF.
        :i/p output_path: path to write data to.
        :o/p: dataframe representing output dimension
    """
    
    migrant_dim = input_df.withColumn("migrant_id", monotonically_increasing_id()) \
                      .select(["migrant_id", "biryear", "gender"]) \
                      .withColumnRenamed("biryear", "birth_year")\
                      .dropDuplicates(["birth_year", "gender"])
                      
    migrant_dim.write.parquet(output_path, mode="overwrite")
    print("Migrant dimention table writing has been completed!")
		
    return migrant_dim

def status_dimension(input_df, output_path):
    """
        Create status dimention table and load into S3 bucket as Parquet files.
        
        :i/p input_df: input DF.
        :i/p output_path: path to write data to.
        :o/p: dataframe representing output dimension
    """
    
    status_dim = input_df.withColumn("status_flag_id", monotonically_increasing_id()) \
                .select(["status_flag_id", "entdepa", "entdepd", "matflag"]) \
                .withColumnRenamed("entdepa", "arrival_flag")\
                .withColumnRenamed("entdepd", "departure_flag")\
                .withColumnRenamed("matflag", "match_flag")\
                .dropDuplicates(["arrival_flag", "departure_flag", "match_flag"])
                
    status_dim.write.parquet(output_path, mode="overwrite")
    print("Status dimention table writing has been completed!")
    
    return status_dim

def visa_dimension(input_df, output_path):
    """
        Create visa dimention table and load into S3 bucket as Parquet files.
        
        :i/p input_df: input DF.
        :i/p output_path: path to write data to.
        :o/p: dataframe representing output dimension
    """
    
    visa_dim = input_df.withColumn("visa_id", monotonically_increasing_id()) \
                .select(["visa_id","i94visa", "visatype", "visapost"]) \
                .dropDuplicates(["i94visa", "visatype", "visapost"])
                
    visa_dim.write.parquet(output_path, mode="overwrite")
    print("Visa dimention table writing has been completed!")
    
    return visa_dim

def state_dimension(input_df, output_path):
    """
        Create State dimention table and load into S3 bucket as Parquet files.
        
        :i/p input_df: input DF.
        :i/p output_path: path to write data to.
        :o/p: dataframe representing output dimension
    """
    
    state_dim = input_df.select(["State Code", "State", "Median Age", "Male Population", "Female Population", "Total Population", "Average Household Size",\
                          "Foreign-born", "Race", "Count"])\
                .withColumnRenamed("State Code", "state_code")\
                .withColumnRenamed("Median Age", "median_age")\
                .withColumnRenamed("Male Population", "male_population")\
                .withColumnRenamed("Female Population", "female_population")\
                .withColumnRenamed("Total Population", "total_population")\
                .withColumnRenamed("Average Household Size", "average_household_size")\
                .withColumnRenamed("Foreign-born", "foreign_born")
                
    state_dim = state_dim.groupBy(col("state_code"), col("State").alias("state")).agg(
							round(mean('median_age'), 2).alias("median_age"),\
							sum("total_population").alias("total_population"),\
							sum("male_population").alias("male_population"), \
							sum("female_population").alias("female_population"),\
							sum("foreign_born").alias("foreign_born"), \
							round(mean("average_household_size"),2).alias("average_household_size")
							).dropna()
        
    state_dim.write.parquet(output_path, mode="overwrite")
    print("State dimention table writing has been completed!")
    
    return state_dim

def time_dimension(input_df, output_path):
    """
        Create Time dimention table and load into S3 bucket as Parquet files.
        
        :i/p input_df: input DF.
        :i/p output_path: path to write data to.
        :o/p: dataframe representing output dimension
    """
    
    def convert_datetime(x):
        try:
            start = datetime(1960, 1, 1)
            return start + timedelta(days=int(x))
        except:
            return None
        
    udf_datetime_conversion = udf(lambda x: convert_datetime(x), DateType())
    
    time_dim = input_df.select(["arrdate"])\
					.withColumn("arrival_date", udf_datetime_conversion("arrdate")) \
					.withColumn('day', F.dayofmonth('arrival_date')) \
					.withColumn('month', F.month('arrival_date')) \
					.withColumn('year', F.year('arrival_date')) \
					.withColumn('week', F.weekofyear('arrival_date')) \
					.withColumn('weekday', F.dayofweek('arrival_date'))\
					.select(["arrdate", "arrival_date", "day", "month", "year", "week", "weekday"])\
					.dropDuplicates(["arrdate"])
                    
    time_dim.write.parquet(output_path, mode="overwrite")
    print("Time dimention table writing has been completed!")
    
    return time_dim

def airport_codes_dimension(input_df, output_path):
    """
        Create Airport Codes dimention table and load into S3 bucket as Parquet files.
        
        :i/p input_df: input DF.
        :i/p output_path: path to write data to.
        :o/p: dataframe representing output dimension
    """
    
    airport_codes = input_df.select(["ident", "type", "iata_code", "name", "iso_country", "iso_region", "municipality", "gps_code", "coordinates", "elevation_ft"])\
					.dropDuplicates(["ident"])
                    
    airport_codes.write.parquet(output_path, mode="overwrite")
    print("Airport_Codes dimention table writing has been completed!")
    
    return airport_codes

def temperature_dimension(input_df, output_path):
    """
        Create Temperature dimention table and load into S3 bucket as Parquet files.
        
        :i/p input_df: input DF.
        :i/p output_path: path to write data to.
        :o/p: dataframe representing output dimension
    """
    
    temperature_dim = input_df.groupBy(col("Country").alias("country")).agg(
                round(mean(F.nanvl(F.col('AverageTemperature'), F.lit(0))), 2).alias("average_temperature"),\
                round(mean(F.nanvl(F.col('AverageTemperatureUncertainty'), F.lit(0))),2).alias("average_temperature_uncertainty")
                ).dropna()\
                .withColumn("temperature_id", monotonically_increasing_id()) \
                .select(["temperature_id", "country", "average_temperature", "average_temperature_uncertainty"])
                
    temperature_dim.write.parquet(output_path, mode="overwrite")
    print("Temperature dimention table writing has been completed!")
    
    return temperature_dim

def country_dimension(input_df, output_path):
    """
        Create Country Codes dimention table and load into S3 bucket as Parquet files.
        
        :i/p input_df: input DF.
        :i/p output_path: path to write data to.
        :o/p: dataframe representing output dimension
    """
    
    input_df.write.parquet(output_path, mode="overwrite")
    print("Country dimention table writing has been completed!")
    

def immigration_fact(input_df, output_path, spark):
    """
        Create immigration_fact Fact table and load into S3 bucket as Parquet files.
        
        :i/p input_df: input DF.
        :i/p output_path: path to write data to.
		:i/p spark: spark session
        :o/p: dataframe representing output dimension
    """
    
    migrant = spark.read.parquet("s3a://udacity-datalake-msaied2/Project6_Capstone/migrant.parquet")
    status = spark.read.parquet("s3a://udacity-datalake-msaied2/Project6_Capstone/status.parquet")
    visa = spark.read.parquet("s3a://udacity-datalake-msaied2/Project6_Capstone/visa.parquet")
    state = spark.read.parquet("s3a://udacity-datalake-msaied2/Project6_Capstone/state.parquet")
    time = spark.read.parquet("s3a://udacity-datalake-msaied2/Project6_Capstone/time.parquet")
    airport = spark.read.parquet("s3a://udacity-datalake-msaied2/Project6_Capstone/airport_codes.parquet")
    country = spark.read.parquet("s3a://udacity-datalake-msaied2/Project6_Capstone/country.parquet")
    country_temperature = spark.read.parquet("s3a://udacity-datalake-msaied2/Project6_Capstone/country_temperature.parquet")
    
    immigration_fact = input_df.select(["*"])\
					.join(airport, (input_df.i94port == airport.ident), how='full')\
					.join(country_temperature, (input_df.i94res == country_temperature.code), how='full')\
					.join(migrant, (input_df.biryear == migrant.birth_year) & (input_df.gender == migrant.gender), how='full')\
					.join(status, (input_df.entdepa == status.arrival_flag) & (input_df.entdepd == status.departure_flag) &\
						  (input_df.matflag == status.match_flag), how='full')\
					.join(visa, (input_df.i94visa == visa.i94visa) & (input_df.visatype == visa.visatype)\
						  & (input_df.visapost == visa.visapost), how='full')\
					.join(state, (input_df.i94addr == state.state_code), how='full')\
					.join(time, (input_df.arrdate == time.arrdate), how='full')\
					.where(col('cicid').isNotNull())\
					.select(["cicid", "i94res", "depdate", "i94mode", "i94port", "i94cit", "i94addr", "airline", "fltno", "ident", "code",\
							 "temperature_id", "migrant_id", "status_flag_id", "visa_id", "state_code", time.arrdate.alias("arrdate")]) 
    immigration_fact.write.parquet(output_path, mode="overwrite") 
    print("Immigration fact table writing has been completed!")
    
    return immigration_fact
