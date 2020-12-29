import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import sum as Fsum
from pyspark import SparkContext as sc
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType


# read config file variables (VALIDATED & COMPLETE)

config = configparser.ConfigParser()
config.read('dl_public.cfg')


# authenticate using environment variables (VALIDATED & COMPLETE)

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']


# establish main bucket file (VALIDATED & COMPLETE)

global BUCKET
BUCKET = config.get("S3", "OUTPUT_BUCKET")


# create spark session (VALIDATED & COMPLETE)

def create_spark_session():
    
    """This function XYZ is used to instantiate the spark session and creates a session application named 'i94_Immigration_Schema'."""
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .appName("i94_Immigration_Schema") \
        .enableHiveSupport() \
        .getOrCreate()
        
    return spark



def stage_i94_data (spark, input_data):
    
    """This function:
    a) Ingests i94_data from S3 Bucket
    b) Reformats i94_data to match target analytics dimensions
    c) Publishes i94_data to staging table for future transformations
    """

    # get filepath to i94 data file
    
    i94_data_path = 'i94_data/*.json'
    
    # read i94 data file
        # note --> is option "header" option inferSchema necessary, given the files being in JSON within S3 buckets???
    
    i94_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .json('{}{}'.format(input_data,i94_data_path))    

    # Select needed i94 columns from master data set
    
    i94_fact_select = i94_df.select('cicid','i94yr','i94mon','i94cit','i94res','i94port','arrdate','i94mode','i94addr','depdate','i94bir','i94visa','count','biryear','gender','visatype')
    
    # Rename column headers to match final Fact Table Name Schema
    
    i94_pre_fact_table = i94_fact_select\
        .withColumnRenamed('cicid','i94rec')\
        .withColumnRenamed('i94yr','i94_year')\
        .withColumnRenamed('i94mon','i94_month')\
        .withColumnRenamed('i94cit','i94_citizenship')\
        .withColumnRenamed('i94res','i94_residence')\
        .withColumnRenamed('i94port','i94_port_of_entry')\
        .withColumnRenamed('arrdate','arrival_date')\
        .withColumnRenamed('i94mode','arrival_mode')\
        .withColumnRenamed('i94addr','arrival_state')\
        .withColumnRenamed('depdate','departure_date')\
        .withColumnRenamed('i94bir','i94_age')\
        .withColumnRenamed('i94visa','travel_purpose')\
        .withColumnRenamed('count','count')\
        .withColumnRenamed('biryear','birth_year')\
        .withColumnRenamed('gender','gender')\
        .withColumnRenamed('visatype','visa_type')
    
    # Store Temptable for Future Processing of Final i94 Fact Table
    
    i94_pre_fact_table.createOrReplaceTempView("i94_pre_fact_temptable")
    



    
    
    
def process_location_codes (spark, input_data):
    
    """This function:
    a) Ingests Location Data
    b) Creates temptable for pre-processing of location data 
    c) Splits Location Data into Country (not in US) & State (within US) processing files
    d) Writes State & Country Location Files to Temptables
    """
        
    # Get filepath to location data file
    
    location_file = 'location_codes.csv'

    # Read the location_data from S3 Bucket
    
    location_codes_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv('{}{}'.format(input_data,location_file))
    
    # Write original location data to temptable
    
    location_codes_df.createOrReplaceTempView("location_codes_temptable")
    
    
    # Establish Country Location_Codes Dataframe
    
    country_location_codes_df = location_codes_df.filter(location_codes_df.country != 'United States')
    
    # Create Country_Codes_Temptable
    
    country_location_codes_df.createOrReplaceTempView("country_codes_temptable")
    
    
    # Establish State Location_Codes Dataframe
    
    state_location_codes_df = location_codes_df.filter(location_codes_df.country == 'United States')
    
    # Create State_Codes Temptable
    
    state_location_codes_df.createOrReplaceTempView("state_codes_temptable")
    
    
    
    
    
    
    

    
def process_temperature_data (spark, input_data, output_data):

    """This function:
    a) Ingests Temperature Data
    b) Calculates Average Temperatures, Grouped by Country, State & Month
    c) Formats Data for Analytics Consumption
    d) Write Average Temperatures to Temptable
    e) Appends State Codes to Average Temperature Table
    f) Creates Temperature by State Temptable
    g) Appends Country Codes to Temperature Table
    h) Creates Finel temperatures_dim Temptable
    i) Writes temperatures_dim to targeted S3 Bucket
    
    """

    # Get filepath to temperature data
    
    temps_file = 'all_temps_farenheit.csv'
    
    # Read the temperature data from S3 Bucket
    
    temp_orig_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv('{}{}'.format(input_data,temps_file))
    
    # Calculate Average Temperatures, Grouped by Country + State + Month
    
    temp_grp_avg_df = temp_orig_df.groupBy("country","state","month").agg((avg("avg_temp")), count("*"))
    
    # Adjust Column Headers with Averages Dataframe
    
    temp_avg_df = temp_grp_avg_df.withColumnRenamed('avg(avg_temp)','avg_temp').withColumnRenamed('count(1)','stat_count')
    
    # Write Average Temperatures to Temptable
    
    temp_avg_df.createOrReplaceTempView("temp_avg_temptable")
    
    
    # Append State Codes to Average Temperature Temptable
    
    temperature_state_dim_df = spark.sql(
        'SELECT \
            ta.country, \
            ta.state as temp_state, \
            ta.month, \
            ta.avg_temp, \
            ta.stat_count, \
            sc.city, \
            sc.state, \
            sc.state_code as i94_state_code, \
            sc.location_code_id as state_loc_id \
            FROM temp_avg_temptable ta \
                 LEFT JOIN state_codes_temptable sc on ta.state = sc.state')
    
    # Create Temperature_by_State Temptable    
    temperature_state_dim_df.createOrReplaceTempView("temp_by_state_temptable")
    
    
    # Append Country Codes to Average Temperature Temptable    
    temperature_dim = spark.sql(
        'SELECT \
            monotonically_increasing_id() as temperature_id, \
            tst.month as temp_month, \
            tst.i94_state_code, \
            cct.country_code as i94_country_code,\
            tst.avg_temp as temp_average, \
            tst.stat_count, \
            tst.city as temp_city, \
            tst.state as temp_state, \
            tst.country as temp_country\
            FROM temp_by_state_temptable tst \
                 LEFT JOIN country_codes_temptable cct on tst.country = cct.country')
    
    # Create Final temperatures_dim Temptable
    temperature_dim.createOrReplaceTempView("temperatures_dim_table")
    
    
    # write temperatures_dim table to parquet files partitioned by month
    temperature_dim.write.partitionBy("temp_month").mode('overwrite').parquet("temperature_dim.parquet")
    
    
    # write temperatures_dim table to s3 bucket file
    temperature_dim.write.json('{}temperature_dim'.format(output_data), mode="overwrite")

    
    
    
    
    
    
    
    

    
def process_demographics_data(spark, input_data, output_data):
    
    """This function:
    a) Ingests Demographics Data
    b) Splits Demographics Data into Ethnicity & Demographics by State Temptables
    c) Adjusts Columns to Analytics-friendly Names
    d) Writes Ethnicity_dim & Demographics_dim to targeted S3 Buckets
    
    """
    
    # Get filepath to Demographics Data

    dg_file = 'us_cities_demographics.csv'
    
    # Read Demographics Data from S3 Bucket
    
    dg_orig_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv('{}{}'.format(input_data,dg_file))
    
    
    # Split Demographics Data into Ethnicity File
    
    dg_eth_st_df = dg_orig_df.groupBy("state_code","race")\
        .agg((avg("median_age")),\
             (avg("average_household_size")),\
             (Fsum("count")))
    

    # Adjust Column Names to match Analytics-friendly Schema Names
    
    dg_eth_st_lbl_df = dg_eth_st_df \
        .withColumnRenamed('state_code','state')\
        .withColumnRenamed('avg(median_age)','median_age')\
        .withColumnRenamed('avg(average_household_size)','avg_hh_size')\
        .withColumnRenamed('sum(count)','count')
    
    # Create Ethnicity_by_State Temptable
    
    dg_eth_st_lbl_df.createOrReplaceTempView("dg_eth_st_lbl_temptable")

    # Append State_Code to Ethnicity_by_State Temptable
    
    eth_st_w_i94_port_code_df = spark.sql(
        'SELECT \
            demo.state, \
            demo.race, \
            demo.median_age, \
            demo.avg_hh_size, \
            demo.count, \
            sc.state_code as i94_state_code \
            FROM dg_eth_st_lbl_temptable demo \
                 LEFT JOIN state_codes_temptable sc on demo.state = sc.state')
    
    
    # Output Ethnicity_by_State to Dimension Table
    
    # Create Final eth_st_w_i94_port_code_dim Temptable
    eth_st_w_i94_port_code_df.createOrReplaceTempView("eth_st_w_i94_port_code_dim_table")
    
    
    # write ethnicity table to parquet files partitioned by state
    eth_st_w_i94_port_code_df.write.partitionBy("state").mode('overwrite').parquet("eth_st_w_i94_port_code_dim.parquet")
    
    
    # write ethnicity table to s3 bucket file ""
    eth_st_w_i94_port_code_df.write.json('{}eth_st_w_i94_port_code_dim'.format(output_data), mode="overwrite")

    
    
    
    # Create Aggregate Demographics by State
    
    dg_st_df = dg_orig_df.groupBy("state_code") \
        .agg((Fsum("median_age")), \
             (Fsum("male_population")), \
             (Fsum("female_population")), \
             (Fsum("total_population")), \
             (Fsum("number_of_veterans")), \
             (Fsum("foreign_born")), \
             (avg("average_household_size")))
    
    # Adjust Column Names to match Analytics-friendly Schema Names
    
    dg_st_lbl_df = dg_st_df \
        .withColumnRenamed('state_code','state')\
        .withColumnRenamed('sum(median_age)','median_age')\
        .withColumnRenamed('sum(male_population)','male_population')\
        .withColumnRenamed('sum(female_population)','female_population')\
        .withColumnRenamed('sum(total_population)','total_population')\
        .withColumnRenamed('sum(number_of_veterans)','number_of_veterans')\
        .withColumnRenamed('sum(foreign_born)','foreign_born')\
        .withColumnRenamed('avg(average_household_size)','avg_hh_size')
    
    # Create Intermediate Demographics_by_State_Label Temptable
    
    dg_st_lbl_df.createOrReplaceTempView("dg_st_lbl_temptable")
    
    # Append State_Code to Demographics_by_State Temptable
    
    dg_st_w_i94_port_code_df = spark.sql(
        'SELECT \
            stdg.state, \
            stdg.median_age, \
            stdg.male_population, \
            stdg.female_population, \
            stdg.total_population, \
            stdg.number_of_veterans, \
            stdg.foreign_born, \
            stdg.avg_hh_size, \
            sc.state_code as i94_state_code \
            FROM dg_st_lbl_temptable stdg \
                 LEFT JOIN state_codes_temptable sc on stdg.state = sc.state')
    
    
    # Output Aggregate_Demographics_by_State to Dimension Table
    
    # Create Final eth_st_w_i94_port_code_dim Temptable
    dg_st_w_i94_port_code_df.createOrReplaceTempView("dg_st_w_i94_port_code_dim_table")
    
    
    # write demographics table to parquet files partitioned by state
    dg_st_w_i94_port_code_df.write.partitionBy("state").mode('overwrite').parquet("dg_st_w_i94_port_code_dim.parquet")
    
    
    # write demographics table to s3 bucket file ""
    dg_st_w_i94_port_code_df.write.json('{}dg_st_w_i94_port_code_dim'.format(output_data), mode="overwrite")

    
    
    
    
    
    
    
    
    
    

def process_airports_data (spark, input_data, output_data):

    """This function:
    a) Ingests Airports Data
    b) Filters Airports to Only US Locations
    c) Filters Airports for Only Large, Medium, and Small Airports Data Set
    d) Appends Location Codes
    e) Exports Airports Data to targeted S3 Bucket
    
    """

    # Get filepath to temperature data
    
    airports_file = 'airport-codes.csv'
    
    # Read the temperature data from S3 Bucket

    airports_orig_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv('{}{}'.format(input_data,airports_file))
    
    # Filter for only US Airports Data
    
    airports_us_only_df = airports_orig_df.filter(airports_orig_df.iso_country == 'US')
    
    # Create Airports Data "state_code" Column using substring function
    
    airport_clean_country_df = airports_us_only_df.withColumn("state_code", substring("iso_region", 4, 2))
    
    # Create Airports_by_US_State Temptable
    
    airport_clean_country_df.createOrReplaceTempView("airports_us_state")
    
    # Append i94_State_Code Using i94_Location_State_Code Temptable
    
    airports_dim = spark.sql(
    'SELECT \
        abs.ident, \
        abs.type,\
        abs.municipality as city,\
        abs.state_code, \
        sc.state_code as i94_port_code \
        FROM airports_us_state abs \
             LEFT JOIN state_codes_temptable sc on (abs.state_code = sc.state) AND (abs.municipality = sc.city)')
    
    # Filter out Extraneous Airport Type Values
    
    us_airports_size_dim = airports_dim.filter(airports_dim.i94_port_code.isNotNull()) \
        .filter(airports_dim.type != "closed") \
        .filter(airports_dim.type != "heliport") \
        .filter(airports_dim.type != "seaplane_base")
    
    
    # Output Airports Types to Dimension Table
    
    # Create Final us_airports_size_dim Temptable
    us_airports_size_dim.createOrReplaceTempView("us_airports_size_dim_table")
    
    
    # write airports table to parquet files partitioned by state
    us_airports_size_dim.write.partitionBy("state_code").mode('overwrite').parquet("us_airports_size_dim.parquet")
    
    
    # write aiports table to s3 bucket file ""
    us_airports_size_dim.write.json('{}us_airports_size_dim'.format(output_data), mode="overwrite")
    
    
    
    
    
    
    
    
    
    
    
def process_i94_fact_table (spark, output_data):

    """This function:
    a) Processes i94 Temptables to Create Final i94 Travel Details Fact Table
    b) Appends Country-specific Temperature ID as Residence Temp ID
    c) Appends Port-specific Temperature ID as Port Temp ID
    d) Write final i94_travel_details Fact Table to targeted S3 Bucket
    
    """    
    
    # ???Does this function require that the outside variable of "tempertures_dim_table" be a variable???
    
    # Join i94_data with Temperature Codes for Residence
    
    i94_w_res_temp_id_df = spark.sql(
        'SELECT \
            pft.i94rec, \
            pft.i94_year, \
            pft.i94_month, \
            pft.i94_citizenship, \
            pft.i94_residence, \
            pft.i94_port_of_entry, \
            pft.arrival_date, \
            pft.arrival_mode, \
            pft.arrival_state, \
            pft.departure_date, \
            pft.i94_age, \
            pft.travel_purpose, \
            pft.count, \
            pft.birth_year, \
            pft.gender, \
            pft.visa_type, \
            tdt.temperature_id as i94_residence_temp_id \
            FROM i94_pre_fact_temptable as pft \
                LEFT JOIN temperatures_dim_table as tdt \
                    ON (pft.i94_residence = tdt.i94_country_code) \
                        AND (pft.i94_month = tdt.temp_month)')
    
    # Write i94_data_with_residence_temp_codes to Temptable
    
    i94_w_res_temp_id_df.createOrReplaceTempView("i94_w_res_temp_id_temptable")
    
    # Join i94_with_residence_temp_codes with Temperature Codes for i94_Port
    
    i94_w_res_temp_id_df = spark.sql(
    'SELECT \
        i94wt.i94rec, \
        i94wt.i94_year, \
        i94wt.i94_month, \
        i94wt.i94_citizenship, \
        i94wt.i94_residence, \
        i94wt.i94_port_of_entry, \
        i94wt.arrival_date, \
        i94wt.arrival_mode, \
        i94wt.arrival_state, \
        i94wt.departure_date, \
        i94wt.i94_age, \
        i94wt.travel_purpose, \
        i94wt.count, \
        i94wt.birth_year, \
        i94wt.gender, \
        i94wt.visa_type, \
        i94wt.i94_residence_temp_id, \
        tdt.temperature_id as i94_port_temp_id\
        FROM i94_w_res_temp_id_temptable as i94wt \
            LEFT JOIN temperatures_dim_table as tdt \
                ON (i94wt.i94_port_of_entry = tdt.i94_state_code) \
                    AND (i94wt.i94_month = tdt.temp_month)')
    
    
    # Output Final i94_visits_fact Table
    
    # Create Final us_airports_size_dim Temptable
    i94_w_res_temp_id_df.createOrReplaceTempView("i94_visits_fact_table")
    
    
    # write songs table to parquet files partitioned by state
    i94_w_res_temp_id_df.write.partitionBy("i94_port_of_entry").mode('overwrite').parquet("i94_visits_fact.parquet")
    
    
    # write songs table to s3 bucket file ""
    i94_w_res_temp_id_df.write.json('{}i94_visits_fact'.format(output_data), mode="overwrite")
    

    
    
    
    

def main():
    
    """This master function executes all functions within the 'etl.py' file."""
    
    spark = create_spark_session()
    input_data = 's3://pg-001-1st-test-bucket/capstone/raw_data/'
    output_data = BUCKET
    
    stage_i94_data (spark, input_data)
    process_location_codes (spark, input_data)
    process_temperature_data (spark, input_data, output_data)
    process_demographics_data(spark, input_data, output_data)
    process_airports_data (spark, input_data, output_data)
    process_i94_fact_table (spark, output_data)
    


if __name__ == "__main__":
    main()
