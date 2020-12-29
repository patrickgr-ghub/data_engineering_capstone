# Data Engineering Capstone Project
### Authored by: Patrick Groover
##### Authored on: December 2020

##### Sources Include: Personal Project Work, Answers Provided within the "Mentor Help" Responses for this project within the Udacity Learning Portal, and Code Mentoring Sessions.


# Project Summary

This project was designed to demonstrate the skills and approaches learned within the Data Engineering Class that allow for the planning, analysis, visualization, and wrangling of data to deliver an automated pipeline of information for analytics purposes. To demonstrate these learnings, students were asked to develop a process that would incorporate 4 distinct sets of data, with at least one being Big Data (defined as a source over 1 Million Rows), to support a targeted analysis that mirrors a real world purpose. To demonstrate knowledge acquired within the course, this project leveraged the following approach to produce the working deliverables:

    Step 1: Scope the Project and Gather Data
    Step 2: Explore and Assess the Data
    Step 3: Define the Data Model
    Step 4: Run ETL to Model the Data
    Step 5: Complete Project Write Up

This project leveraged data transformations and handling for multiple data types, including csv, json, and parquet files.


### File Setup & Process for Launching Project

#### Run ETL using EMR Cluster

    A) Set Target Directory for ETL Files
    B) Access "dl.cfg" file and add AWS Credentials, then update Target Directory as "OUTPUT_BUCKET=<Target Directory>"
    C) Launch EMR Cluster
    D) Load "capstone_etl.py" and "dl.cfg" files to EMR Cluster
    E) Run "capstone_etl.py"
    
#### Run Airflow to Create & Validate Analytics Fact & Dimension Tables within Redshift

    A) Load files from "Capstone Airflow Files" Directory into Airflow matching the Directory Structure (see Step 4.1.2)
    B) Open the file "capstone_dag.py" and update the "data_path" to match the Target Directory within the ETL Process for the following dag operator statements:
        - import_i94_visit_details_fact
        - import_state_demographics_dim
        - import_ethnicity_by_state
        - import_temperatures_dim
        - import_us_airports_size_dim
    C) Launch Airflow & Add AWS Credentials and Redshift Credentials
    D) Run "capstone_dag" in Airflow





## Step 1: Scope the Project and Gather Data

#### Sample Scenario that directed project outcomes:

Travel & Immigration Data to the United States can help city and state officials plan for tourism, naturalization, business, and educational needs. The seasonal variance of such activites can lead to staffing, security, and support issues that make it challenging to address the volume and needs of visitors. To help with planning, the U.S. Travel & Immigration Department has requested that a database of visitor information be created that helps identify patterns of ingress including a look at orgins, destinations, visa type, demographics of visitor, demographics of destination state, access to a major airport, as well as temperatures of origin and destination city. The datalake will serve as the foundation for adding additional data sources to analytics research in the future.


#### 1.1 Main Data Sources for the Project

- **I94 Immigration Data:** This data comes from the US National Tourism and Trade Office and is a listing of visits to the United States.

> Link to Immigration Data Source
> https://travel.trade.gov/research/reports/i94/historical/2016.html

- **World Temperature Data:** This dataset came from Kaggle that will correlate temperatures of US Destination and Country of Origen by Month.

> Link to World Temperature Data Source      
> https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data

- **U.S. City Demographic Data:** This data comes from OpenSoft and will be used to correlate US Destination Demographics by State.

> Link to U.S. City Demographic Data      
> https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/

- **Airport Code Table:** This is a simple table of airport codes and corresponding cities the will be used to correlate Airport Size to US Destination.

> Link to Airport Code Table      
> https://datahub.io/core/airport-codes#data


#### 1.2 Secondary Data Sources for Creating Analytics Tables

These files are found in the publicly accessible S3 Bucket: "pg-001-1st-test-bucket"

And Located within the following directory path: "capstone/analytics_keys/

- **Location Codes:** A table of aggregate Country & City Codes that related to the Location Fields within the i94 Immigration Data.

> File Name for Location Codes: "location_codes.csv"

- **Travel Mode Codes:** A table of travel codes that denote the transportation type used to reach the United States.

> File Name for Travel Mode Codes: "travel_mode_code.csv"

- **Travel Purpose Codes:** A table of travel purpose codes that denotes the main reason for visiting the United States.

> File Name for Travel Purpose Codes: "travel_purpose.csv"

- **Visa Type Codes:** A table of Visa Type Codes that denotes the classification of the visit to the United States.

> File Name for Visa Type Codes: "visa_type_codes.csv"


#### 1.3 Tools Used to Develop Framework and Pipeline

    A) Analysis of file sources & structures through Pyspark within Jupyter Notebooks
    B) Creation of a Star Schema and Data Processing Flow Diagram using Lucid Chart
    C) Development of ETL Process using Jupyter Notebooks, tested on a subset of Data within a Pyspark Dataframe
    D) Execution of ETL Process through an EMR Cluster to produce Dimension & Fact Tables that were loaded to the targeted Amazon S3 Bucket
    E) Development of a Pipeline Process for ingesting & updating the Redshift Analytics Database through Airflow
    F) Execution of the Airflow Process with Data Validation to produce a working analytics data set
    G) Review of delivered information within Redshift for a final validation of delivered results
    
    
#### 1.4 Targeted Solution

The ultimate outcome of the project will be a star schema analytics database that is easy for analytics users to query. The database will relate through a central Fact Table of i94 Visit Details and will need to link to Dimension Tables using record keys to make analytics queries simple to non-engineering resources and ultimately save any future need for transforming information in order to link disparate data sets.

The following steps detail the data discovery that was used to produce the final Star Schema Analytics Database that is seen here:
![i94 Visit Analytics Star Schema](http://na-sjdemo1.marketo.com/rs/786-GZR-035/images/Capstone_i94_Analytics_Star_Schema.png "i94 Visit Analytics Star Schema")

> External Link to View Star Schema       
> http://na-sjdemo1.marketo.com/rs/786-GZR-035/images/Capstone_i94_Analytics_Star_Schema.png


#### 1.5 Gather Data 

To support the development of the Analytics Star Schema initial Data Sources & Analytics Data Tables were loaded to S3 Buckets within "pg-001-1st-test-bucket".




## Step 2: Explore and Assess the Data

To develop the Fact & Dimension Tables that would support the final Analytics Data Set and Pipeline, it was necessary to use an itterative approach to analyzing existing dataset, then adapting the scope of analytics that would be available due to data formats and gaps within the data sets.

The process leveraged the following Pyspark Imports to analyze and adjust the flow for integrating data sets into a final, workable, and connected pipeline.

#### Explore the Data 
Identify data quality issues, like missing values, duplicate data, etc.

#### Cleaning Steps
Document steps necessary to clean the data


##### 2.0.1 Import Code Libraries

    # Imports and installs
    from pyspark.sql import SparkSession
    from pyspark import SparkContext as sc
    import pandas as pd
    import numpy as np
    from IPython.display import display
    from zipfile import ZipFile
    import os
    import io
    from pyspark.sql.functions import isnan, count, when, col, desc, udf, col, sort_array, asc, avg, substring
    from pyspark.sql.functions import sum as Fsum
    from pyspark.sql.window import Window
    from pyspark.sql.types import IntegerType
    
    
#### 2.1.1 i94 Data Analysis

    # Read in the i94 data
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11").enableHiveSupport().getOrCreate()
    fpath = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    i94_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .format('com.github.saurfang.sas.spark').load(fpath)
        

    # Visualize the i94 data
    i94_df.head()
    i94_df.printSchema()
    
    # Display in Pandas to View Column Headers & Data Types
    pd.set_option('display.max_columns', 200)
    i94_df.limit(5).toPandas()


##### 2.1.2 Conclusions of i94 Visits Data Analysis
    A) Column headers will need to be transformed to match more user-friendly naming schemas
    B) Addition of temperature keys for i94 Port & Visitor Residence will need to be appended


##### 2.1.3 i94 Visits Data Cleaning

    # Create i94_fact_select Dataframe
    i94_fact_select = i94_df.select('cicid','i94yr','i94mon','i94cit','i94res','i94port','arrdate','i94mode','i94addr','depdate','i94bir','i94visa','count','biryear','gender','visatype')
    

    # Prepare Fact Select Dataframe for final additions by adding column headers

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
        
        
    # Visualize the Fact Select Dataframe and Validate Count of Rows
    i94_pre_fact_table.limit(5).toPandas()
    i94_pre_fact_table.count()
    

    # Create Temptable for Creation of final Fact Table
    i94_pre_fact_table.createOrReplaceTempView("i94_pre_fact_temptable")
    
    
#### 2.2.1 Location Codes Data Analysis

    # Read in Location Codes Data
    location_file = 'raw_data/location_codes.csv'
    location_codes_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv(location_file)
        
    # Visualize Location Data & Count Rows
    location_codes_df.head()
    location_codes_df.printSchema()
    location_codes_df.count()
    
    # Display Location Codes Data in Pandas to View Column Headers & Data Types
    location_codes_df.limit(5).toPandas()


##### 2.2.2 Conclusions of Location Codes Data Analysis
    A) Main table will need to be split into Country & State Temptables within the EMR Process
    B) Country Codes can be isolated using ".filter(location_codes_df.country != 'United States')"
    C) City Codes will need to be mapped to State Codes for aggregate analytics
    D) State Codes can be isolated using ".filter(location_codes_df.country == 'United States')"
    
    
##### 2.2.3 Location Codes Data Cleaning

    # Write Dataframe to Temptable for use in creating the split tables
    location_codes_df.createOrReplaceTempView("location_codes_temptable")
    

    # Filter & Visualize, then Create Country Codes Temptable
    country_location_codes_df = location_codes_df.filter(location_codes_df.country != 'United States')
    country_location_codes_df.show(5)
    country_location_codes_df.createOrReplaceTempView("country_codes_temptable")
    
    # Filter & Visualize, then Create State Codes Temptable
    state_location_codes_df = location_codes_df.filter(location_codes_df.country == 'United States')
    state_location_codes_df.show(5)
    state_location_codes_df.createOrReplaceTempView("state_codes_temptable")
    

#### 2.3.1 Temperature Data Analysis

    # Read in Temperature Data
    temps_file = 'raw_data/all_temps_farenheit.csv'
    temp_orig_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv(temps_file)
        
    # Visualize Temperature Data
    temp_orig_df.head()
    temp_orig_df.printSchema()
    
    # Display Temperature Data in Pandas to View Column Headers & Data Types
    temp_orig_df.filter(col("country")=="United States").limit(5).toPandas()
    

##### 2.3.2 Conclusions of Location Codes Data Analysis
    A) Temperature information can only be aggregated to match the State & Country of i94 Visitor Records
    B) Temperature information is useful in aggregate at the State and will need to be mapped to locations
    

##### 2.3.3 Temperatures Data Cleansing

    # Leverage GroupBy Statements & Aggregate Functions to Identify Usable Data Sets for Analytics
    temp_grp_avg_df = temp_orig_df.groupBy("country","state","month").agg((avg("avg_temp")), count("*"))
    
    # Format Data Set to Match Intended Analytics Star Schema
    temp_avg_df = temp_grp_avg_df.withColumnRenamed('avg(avg_temp)','avg_temp').withColumnRenamed('count(1)','stat_count')
    
    # Output Count of Data to Validate Transformations
    temp_avg_df.count()
    
    # Create Temperatures Temptable for Future Transformations
    temp_avg_df.createOrReplaceTempView("temp_avg_temptable")
    
    # Join Locations & Temperatures Tables using State Codes 
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
                 
    # Create Temperatures_by_State Temptable for Country Codes Append
    temperature_state_dim_df.createOrReplaceTempView("temp_by_state_temptable")
    
    # Join Locations Temptable & Temperatures_by_State Temptable to Create Final Temperatures Dimension Table
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
                 
    # Visualize Transformed Data, Validate Appends, and Create Final Temptable for use in Future Appends
    temperature_dim.count()
    temperature_dim.printSchema()
    temperature_dim.createOrReplaceTempView("temperatures_dim_table")
    

#### 2.4.1 Demographics Data Analysis

    # Read in Demographics Data
    demo_file = 'raw_data/us_cities_demographics.csv'
    demo_orig_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv(demo_file)
        
    # Visualize Demographics Data
    demo_orig_df.head()
    demo_orig_df.printSchema()
    demo_orig_df.count()
    
    # Display Demographics Data in Pandas to View Column Headers & Data Types
    demo_orig_df.show(25)
    

##### 2.4.2 Conclusions of Demographics Data Analysis
    A) Demographics Tables will need to be split into Demographics by State & Ethnicity by State Dimensions
    B) Data will need to be Summed or Averaged by State to deliver meaningful insights
    C) Location Codes to match the Summaries by State will need to be appended to produce the final Dimension Table Links to the i94 Fact Table
    

##### 2.4.3 Demographics Data Cleansing

##### 2.4.3.1 Produce Ethnicity by State Dimension Table

    # GroupBy & Aggregate Functions for Ethnicity 
    dg_eth_st_df = demo_orig_df.groupBy("state_code","race").agg((avg("median_age")),(avg("average_household_size")),(Fsum("count")))
    
    # Adjust Columns to Match Usable Analytics Names
    dg_eth_st_lbl_df = dg_eth_st_df \
        .withColumnRenamed('state_code','state')\
        .withColumnRenamed('avg(median_age)','median_age')\
        .withColumnRenamed('avg(average_household_size)','avg_hh_size')\
        .withColumnRenamed('sum(count)','count')
        
    # Visualize & Validate, then Create Temptable for developing final Ethnicity Dimension Table
    dg_eth_st_lbl_df.show(5)
    dg_eth_st_lbl_df.count()
    dg_eth_st_lbl_df.createOrReplaceTempView("dg_eth_st_lbl_temptable")
    
    # Create Final Ethnicity_by_State Dimension Table
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
                 
##### 2.4.3.2 Product Demographics by State Dimension Table

    # GroupBy & Aggregate Functions for Demographics by State
    dg_st_df = demo_orig_df.groupBy("state_code").agg((Fsum("median_age")),(Fsum("male_population")),(Fsum("female_population")),(Fsum("total_population")),(Fsum("number_of_veterans")),(Fsum("foreign_born")),(avg("average_household_size")))
    
    # Adjust Columns to Match Usable Analytics Names
    dg_st_lbl_df = dg_st_df \
        .withColumnRenamed('state_code','state')\
        .withColumnRenamed('sum(median_age)','median_age')\
        .withColumnRenamed('sum(male_population)','male_population')\
        .withColumnRenamed('sum(female_population)','female_population')\
        .withColumnRenamed('sum(total_population)','total_population')\
        .withColumnRenamed('sum(number_of_veterans)','number_of_veterans')\
        .withColumnRenamed('sum(foreign_born)','foreign_born')\
        .withColumnRenamed('avg(average_household_size)','avg_hh_size')
        
    # Visualize & Validate, then Create Temptable for developing final Demographics by State Dimension Table
    dg_st_lbl_df.show(5)
    dg_st_lbl_df.count()
    dg_st_lbl_df.createOrReplaceTempView("dg_st_lbl_temptable")
    
    # Create Final Ethnicity_by_State Dimension Table
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
                 

#### 2.5.1 Airports Data Analysis

    # Read in the Airports Data
    airports_file = 'raw_data/airport-codes.csv'
    airports_orig_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv(airports_file)
        
    # Visualize the Airports Data
    airports_orig_df.show(5)
    
##### 2.5.2 Conclusions of Airports Data Analysis
    A) Airports Data will only be relevant to the U.S. City and will need a City & State Code Appended to match the i94 Visits Analytics Table
    B) Airports Data will need to be filtered to focus on large, medium, and small airports only
    
##### 2.5.3 Airports Data Cleansing

    # Isolate the US Airports by City & State, then Visualize the Dataframe
    airports_us_only_df = airports_orig_df.filter(airports_orig_df.iso_country == 'US')
    airport_clean_country_df = airports_us_only_df.withColumn("state_code", substring("iso_region", 4, 2))
    airport_clean_country_df.show(5)
    
    # Create Airports Temptable using Isolated Data by US State
    airport_clean_country_df.createOrReplaceTempView("airports_us_state")
    
    # Link together Location Codes & Airport Data
    airports_dim = spark.sql(
        'SELECT \
            abs.ident, \
            abs.type,\
            abs.municipality as city,\
            abs.state_code, \
            sc.state_code as i94_port_code \
            FROM airports_us_state abs \
                 LEFT JOIN state_codes_temptable sc on (abs.state_code = sc.state) AND (abs.municipality = sc.city)')
                 
    # Filter Dataframe to only include Small, Medium and Large Airports
    us_airports_size_dim = airports_dim.filter(airports_dim.i94_port_code.isNotNull()).filter(airports_dim.type != "closed").filter(airports_dim.type != "heliport").filter(airports_dim.type != "seaplane_base")
    
    # Validate Final Dataframe
    us_airports_size_dim.show(5)
    us_airports_size_dim.count()
    

### (Continuted) 2.1.4 Final Processing of i94 Visit Details Fact Table

    # Append Residence Temperature ID to i94 Visit Details
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
                        
    # Validate Append of Residence Temperature ID, then create i94_with_Residence_Temperature_Id Temptable
    i94_w_res_temp_id_df.show(5)
    i94_w_res_temp_id_df.count()
    i94_w_res_temp_id_df.printSchema()
    i94_w_res_temp_id_df.filter(i94_w_res_temp_id_df.i94_residence_temp_id.isNotNull()).show(5)
    i94_w_res_temp_id_df.createOrReplaceTempView("i94_w_res_temp_id_temptable")
    
    # Append Port Temperature ID to i94 Visit Details
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
                        
    # Validate Append of Port Temperature ID
    i94_w_res_temp_id_df.show(5)
    i94_w_res_temp_id_df.count()
    


## Step 3: Define the Data Model

Having worked through analysis and cleaning of the data, it was possible to develop the final ETL Process that would be used to consistently pull data in from sources, then stage information for redshift analysis. As mentioned before, this was an itterative process, with the final Star Schema shown above being the outcome of the analysis and data cleansing. To support the definition of the data model and processing, the following data flow diagram was developed:

![ETL Pipeline Dataflow for Pyspark Build through EMR](http://na-sjdemo1.marketo.com/rs/786-GZR-035/images/ETL_Pipeline_Dataflow_for_Pyspark_Build.png "ETL Pipeline Dataflow for Pyspark Build through EMR")

> External Link to Visual Flow for Building out the data model:      
> http://na-sjdemo1.marketo.com/rs/786-GZR-035/images/ETL_Pipeline_Dataflow_for_Pyspark_Build.png


#### 3.1 Explanation of the Conceptual Data Model
With multiple data sources, a need to standardize primary keys, foreign keys, and make it simple to link together information for redshift analytics the flow stages data sources while preparing data processing to develop the listed dimension tables and central fact table. A few of the key steps included:

    Appending the Location ID to the Temperature Table.
    Appending the Location ID to the Demographics and Ethnicity Tables.
    Appending the Location ID to the Airport.
    Appending the Temperature Record ID using the Location ID and Month of Visit.

**Key observations about the Current Data Set (Summary of Gaps)**

From a statistical and analytical perspective, making conclusions from average or related data rather than the exactly correlated data requires careful consideration. This particular data model had numerous gaps that needed to be considered when building out the current model.

1. Information on Location Temperatures needed to be aggregated to the State Level because matching City-specific details did not present a large enough match rate.
2. Information on Countries could not be matched back to Airport Details with enough consistency, so international analysis of Airport Sizing could not be developed.
3. Overall, the Data Sets between Temperature, Airport Size, Demographics and i94 Travel Details are all from different time periods.

With the current data framework in place data engineers can work to locate increasingly aligned datasets in the future to improve accuracy of the conclusions made by analysts.


#### 3.2 Mapping Out Data Pipelines
As described within the document, the following approach was used to develop the data flow:

1. Gather Data Sources
2. Analyze Potential Gaps
3. Build a First-pass Conceptual Framework
4. Test Data Sets & Outline Initial Star Schema
5. Develop Initial Data Handling Flow targeting an EMR ETL.
6. Iterrate through Pyspark Configurations
7. Adjust Star Schema and Data Flow to Match Findings
8. Test the EMR ETL within the Local Directory
9. Execute the Final EMR ETL to deliver Dimension & Fact Tables



## Step 4: Run Pipelines to Model the Data 

### Overview of Data Model

#### 4.1.1 Create the data model - EMR ETL
Using the Itterative Process above, the EMR ETL was locally developed within the Udacity Pyspark Workspace, then the following steps were used to execute the full EMR ETL.

1. Export the "capstone_etl.py" & "dl.cfg" file to a desktop directory.
2. Use AWS IAC to initialize an EMR Cluster.
3. Authenticate to the EMR Cluster through Terminal via SSH to import the "capstone_etl.py" and "dl.cfg" files.
4. Launch the "final_elt.py" process within the EMR Cluster Terminal.
5. Validate the Dimension & Fact Table Data has been processed to the targeted S3 Bucket and Directories.

#### 4.1.2 - Details of "capstone_etl.py"

#### Import Libraries

    import configparser
    from datetime import datetime
    import os
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.functions import sum as Fsum
    from pyspark import SparkContext as sc
    from pyspark.sql.window import Window
    from pyspark.sql.types import IntegerType
    
#### Read config file variables (VALIDATED & COMPLETE)

    config = configparser.ConfigParser()
    config.read('dl.cfg')
    
#### Authenticate using environment variables (VALIDATED & COMPLETE)

    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']

#### Establish main bucket file (VALIDATED & COMPLETE)

    global BUCKET
    BUCKET = config.get("S3", "OUTPUT_BUCKET")

#### Create spark session (VALIDATED & COMPLETE)

    def create_spark_session():

        """This function XYZ is used to instantiate the spark session and creates a session application named 'i94_Immigration_Schema'."""

        spark = SparkSession \
            .builder \
            .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11") \
            .appName("i94_Immigration_Schema") \
            .enableHiveSupport() \
            .getOrCreate()

        return spark

#### "Stage i94 Data" Function

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

#### "Process Location Codes" Function

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

#### "Process Temperature Data" Function

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

#### "Process Demographics Data" Function

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

#### "Process Airports Data" Function

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

#### "Process i94_fact_table" Function

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

#### Launch functions within "capstone_etl.py"

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

#### 4.1.2 Create the Data Pipeline - Apache Airflow

To support the ongoing use and update of information within the analytics framework, the finalized data model was translated to a data pipeline through Apache Airflow to upload analytics tables to a redshift cluster and validate that data is loading without errors. A DAG (Directed Acyclic Graph) was developed to support ingestion, processing, and validation of the i94 Travel Details Analytics Star Schema.

1. Create tables within the Redshift Instance that mirror the Dimension & Fact Tables within the Star Schema - See "capstone_create_tables.sql" file (also included below)
2. Log into and Launch Apache Airflow Instance.
3. Load AWS Credentials to Airflow.
4. Launch Capstone DAG, including Loading and Validation Processes.

Accessing & Using the Airflow DAG Files:
To Launch the Airflow Files, see the "Capstone Airflow Files" folder within this workspace and mirror the folder structure within an Airflow Environment:

![Capstone Airflow - Directory Structure](http://na-sjdemo1.marketo.com/rs/786-GZR-035/images/Airflow_Directory_Structure.png)


The Airflow Data Pipeline produced the following graph:

![Capstone DAG - Graph View](http://na-sjdemo1.marketo.com/rs/786-GZR-035/images/capstone_dag_completed_graph_view.png)

> External Link to Airflow Data Pipeline:     
> http://na-sjdemo1.marketo.com/rs/786-GZR-035/images/capstone_dag_completed_graph_view.png


#### Capstone Create Tables SQL

    CREATE TABLE IF NOT EXISTS i94_visit_details_fact (
        "i94rec"              int     NOT NULL     PRIMARY KEY,
        "i94_year"            int,
        "i94_month"           int,
        "i94_citizenship"     varchar,
        "i94_residence"       varchar,
        "i94_port_of_entry"   varchar sortkey,
        "arrival_date"        numeric,
        "arrival_mode"        int,
        "arrival_state"       varchar,
        "departure_date"      numeric,
        "i94_age"             int,
        "travel_purpose"      int,
        "count"               int,
        "birth_year"          int,
        "gender"              varchar,
        "visa_type"           varchar,
        "residence_temp_id"   int,
        "port_temp_id"        int
    );


    CREATE TABLE IF NOT EXISTS location_codes_dim (
        "location_code_id"      varchar     NOT NULL,
        "country_code"          varchar,
        "country"               varchar,
        "state_code"            varchar,
        "city"                  varchar,
        "state"                 varchar
    );


    CREATE TABLE IF NOT EXISTS travel_mode_dim (
        "travel_mode_code"      varchar     NOT NULL      PRIMARY KEY,
        "mode"                  varchar
    );


    CREATE TABLE IF NOT EXISTS travel_purpose_dim (
        "travel_purpose_code"   varchar     NOT NULL      PRIMARY KEY,
        "travel_purpose"        varchar
    );


    CREATE TABLE IF NOT EXISTS visa_type_codes_dim (
        "visa_code"             varchar     NOT NULL,
        "visa_category"         varchar,
        "visa_travel_purpose"   varchar
    );


    CREATE TABLE IF NOT EXISTS temperatures_dim (
        "temperature_id"          bigint     NOT NULL   PRIMARY KEY,
        "temp_month"              int,
        "i94_state_code"          varchar,
        "i94_country_code"        varchar,
        "temp_average"            numeric,
        "stat_count"              int,
        "temp_city"               varchar,
        "temp_state"              varchar,
        "temp_country"            varchar
    );


    CREATE TABLE IF NOT EXISTS state_demographics_dim (
        "i94_state_code"         varchar     NOT NULL     PRIMARY KEY,
        "median_age"             numeric,
        "male_population"        int,
        "female_population"      int,
        "total_population"       int,
        "number_of_veterans"     int,
        "foreign_born"           int,
        "avg_hh_size"            numeric
    );


    CREATE TABLE IF NOT EXISTS ethnicity_by_state_dim (
        "i94_state_code"          varchar,
        "race"                    varchar,
        "state"                   varchar,
        "count"                   int
    );


    CREATE TABLE IF NOT EXISTS us_airports_size_dim (
        "i94_port_code"           varchar     NOT NULL     PRIMARY KEY,
        "state_code"              varchar,
        "type"                    varchar,
        "city"                    varchar,
        "ident"                   varchar
    );

#### Content of "capstone_dag.py"

    from datetime import datetime, timedelta
    import os
    from airflow import DAG
    from airflow.operators.dummy_operator import DummyOperator
    from operators import (JsonToRedshiftOperator, CsvToRedshiftOperator, CapstoneDataQualityOperator)
    from helpers import SqlQueries

    from airflow.contrib.hooks.aws_hook import AwsHook
    from airflow.hooks.postgres_hook import PostgresHook
    from airflow.operators.postgres_operator import PostgresOperator
    from airflow.operators.python_operator import PythonOperator

    # Instantiates the AWS Credentials

    AWS_KEY = os.environ.get('AWS_KEY')
    AWS_SECRET = os.environ.get('AWS_SECRET')


    # Creates the Default Args for the DAG

    default_args = {
        'owner': 'udacity',
        'start_date': datetime(2020, 1, 12),
        'depends_on_past': False,
        'retries': 3,
        'retry_delay':timedelta(seconds=300),
        'catchup': False,
        'email_on_retry': False,
        'query_checks': [
            {'check_sql': "SELECT COUNT(*) FROM i94_visit_details_fact WHERE i94rec is null", 'expected_result':0},
            {'check_sql': "SELECT COUNT(*) FROM location_codes_dim WHERE location_code_id is null", 'expected_result':0},
            {'check_sql': "SELECT COUNT(*) FROM travel_mode_dim WHERE travel_mode_code is null", 'expected_result':0},
            {'check_sql': "SELECT COUNT(*) FROM travel_purpose_dim WHERE travel_purpose_code is null", 'expected_result':0},
            {'check_sql': "SELECT COUNT(*) FROM visa_type_codes_dim WHERE visa_code is null", 'expected_result':0},
            {'check_sql': "SELECT COUNT(*) FROM temperatures_dim WHERE temperature_id is null", 'expected_result':0},
            {'check_sql': "SELECT COUNT(*) FROM state_demographics_dim WHERE i94_state_code is null", 'expected_result':0},
            {'check_sql': "SELECT COUNT(*) FROM ethnicity_by_state_dim WHERE race is null", 'expected_result':0},
            {'check_sql': "SELECT COUNT(*) FROM us_airports_size_dim WHERE i94_port_code is null", 'expected_result':0}
            ]
    }


    # Establishes the DAG

    dag = DAG(
        'capstone_dag',
        default_args = default_args,
        max_active_runs=1,
        description='Load and transform i94_travel_visits in Redshift with Airflow',
        schedule_interval='@monthly'
    )


    # Sets the Start Operator

    start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


    # Uses the ImportJsonToRedshift Operator to Load JSON Project Tables

    import_i94_visit_details_fact = JsonToRedshiftOperator(
        task_id='Import_i94_Visit_Details',
        dag=dag,
        table="i94_visit_details_fact",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="pg-001-1st-test-bucket",
        data_path="capstone/analytics_data/i94_visits_fact"
    )

    import_state_demographics_dim = JsonToRedshiftOperator(
        task_id='Import_State_Demographics',
        dag=dag,
        table="state_demographics_dim",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="pg-001-1st-test-bucket",
        data_path="capstone/analytics_data/dg_st_w_i94_port_code_dim"
    )

    import_ethnicity_by_state = JsonToRedshiftOperator(
        task_id='Import_Ethnicity_by_State',
        dag=dag,
        table="ethnicity_by_state_dim",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="pg-001-1st-test-bucket",
        data_path="capstone/analytics_data/eth_st_w_i94_port_code_dim"
    )

    import_temperatures_dim = JsonToRedshiftOperator(
        task_id='Import_Temperatures',
        dag=dag,
        table="temperatures_dim",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="pg-001-1st-test-bucket",
        data_path="capstone/analytics_data/temperature_dim"
    )

    import_us_airports_size_dim = JsonToRedshiftOperator(
        task_id='Import_Airports_Sizing',
        dag=dag,
        table="us_airports_size_dim",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="pg-001-1st-test-bucket",
        data_path="capstone/analytics_data/us_airports_size_dim"
    )

    # Uses the ImportCSVToRedshift Operator to Load JSON Project Tables

    import_location_codes_dim = CsvToRedshiftOperator(
        task_id='Import_Location_Codes',
        dag=dag,
        table="location_codes_dim",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="pg-001-1st-test-bucket",
        data_path="capstone/analytics_keys/location_codes.csv"
    )

    import_travel_mode_dim = CsvToRedshiftOperator(
        task_id='Import_Travel_Mode_Codes',
        dag=dag,
        table="travel_mode_dim",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="pg-001-1st-test-bucket",
        data_path="capstone/analytics_keys/travel_mode_code.csv"
    )

    import_travel_purpose_dim = CsvToRedshiftOperator(
        task_id='Import_Travel_Purpose',
        dag=dag,
        table="travel_purpose_dim",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="pg-001-1st-test-bucket",
        data_path="capstone/analytics_keys/travel_purpose.csv"
    )

    import_visa_type_codes_dim = CsvToRedshiftOperator(
        task_id='Import_Visa_Type_Codes',
        dag=dag,
        table="visa_type_codes_dim",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="pg-001-1st-test-bucket",
        data_path="capstone/analytics_keys/visa_type_codes.csv"
    )


    # Uses the DataQuality Operator to Run the DataQuality Checks across the listed tables

    data_quality_checks = CapstoneDataQualityOperator(
        task_id='i94_quality_checks',
        dag=dag,
        tables=('i94_visit_details_fact', 'state_demographics_dim', 'ethnicity_by_state_dim', 'temperatures_dim', 'us_airports_size_dim', 'location_codes_dim', 'travel_mode_dim', 'travel_purpose_dim', 'visa_type_codes_dim'),
        dq_checks=default_args['query_checks'],
        redshift_conn_id="redshift"
    )


    # Ends the DAG Run

    end_operator = DummyOperator(task_id='End_execution',  dag=dag)


    # Sets the Order of Parallelization for DAG execution

    start_operator >> import_i94_visit_details_fact
    start_operator >> import_state_demographics_dim
    start_operator >> import_ethnicity_by_state
    start_operator >> import_temperatures_dim
    start_operator >> import_us_airports_size_dim
    start_operator >> import_location_codes_dim
    start_operator >> import_travel_mode_dim
    start_operator >> import_travel_purpose_dim
    start_operator >> import_visa_type_codes_dim

    import_i94_visit_details_fact >> data_quality_checks
    import_state_demographics_dim >> data_quality_checks
    import_ethnicity_by_state >> data_quality_checks
    import_temperatures_dim >> data_quality_checks
    import_us_airports_size_dim >> data_quality_checks
    import_location_codes_dim >> data_quality_checks
    import_travel_mode_dim >> data_quality_checks
    import_travel_purpose_dim >> data_quality_checks
    import_visa_type_codes_dim >> data_quality_checks

    data_quality_checks >> end_operator

#### Content of "capstone_csv_redshift.py"

    from airflow.contrib.hooks.aws_hook import AwsHook
    from airflow.hooks.postgres_hook import PostgresHook
    from airflow.models import BaseOperator
    from airflow.utils.decorators import apply_defaults

    # Establishes the Class "StageToRedshiftOperator" within the DAG
    # Designed to Copy Files from S3 Bucket to Staging Tables

    class CsvToRedshiftOperator(BaseOperator):

        ui_color = '#358140'

        # Establishes the template SQL Copy Statements

        copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            csv
            IGNOREHEADER 1;
        """

        # Defines default values from the Operator Call within the DAG, including context

        @apply_defaults
        def __init__(self,
                     redshift_conn_id="",
                     aws_credentials_id="",
                     table="",
                     s3_bucket="",
                     # s3_key="",
                     json_path="auto",
                     data_path="",
                     delimiter=",",
                     ignore_headers=1,
                     *args, **kwargs):

            super(CsvToRedshiftOperator, self).__init__(*args, **kwargs)
            self.table = table
            self.redshift_conn_id = redshift_conn_id
            self.s3_bucket = s3_bucket
            self.data_path = data_path
            # self.s3_key = s3_key
            self.aws_credentials_id = aws_credentials_id
            self.json_path = json_path


        # Function to Run Staging Tables Load 

        def execute(self, context):
            aws_hook = AwsHook(self.aws_credentials_id)
            credentials = aws_hook.get_credentials()
            redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

            self.log.info("Copying data from S3 to Redshift")
            # rendered_key = self.s3_key.format(**context)
            s3_path = "s3://{}/{}".format(self.s3_bucket, self.data_path)
            formatted_sql = CsvToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key
            )

            redshift.run(formatted_sql)
            self.log.info('Redshift Staging Tables Complete')

#### Content of "capstone_json_redshift.py"

    from airflow.contrib.hooks.aws_hook import AwsHook
    from airflow.hooks.postgres_hook import PostgresHook
    from airflow.models import BaseOperator
    from airflow.utils.decorators import apply_defaults

    # Establishes the Class "StageToRedshiftOperator" within the DAG
    # Designed to Copy Files from S3 Bucket to Staging Tables

    class JsonToRedshiftOperator(BaseOperator):

        ui_color = '#358140'

        # Establishes the template SQL Copy Statements

        copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            FORMAT AS JSON '{}'
            IGNOREHEADER 1
            DATEFORMAT 'auto';
        """

        # Defines default values from the Operator Call within the DAG, including context

        @apply_defaults
        def __init__(self,
                     redshift_conn_id="",
                     aws_credentials_id="",
                     table="",
                     s3_bucket="",
                     # s3_key="",
                     json_path="auto",
                     data_path="",
                     delimiter=",",
                     ignore_headers=1,
                     *args, **kwargs):

            super(JsonToRedshiftOperator, self).__init__(*args, **kwargs)
            self.table = table
            self.redshift_conn_id = redshift_conn_id
            self.s3_bucket = s3_bucket
            self.data_path = data_path
            # self.s3_key = s3_key
            self.aws_credentials_id = aws_credentials_id
            self.json_path = json_path


        # Function to Run Staging Tables Load 

        def execute(self, context):
            aws_hook = AwsHook(self.aws_credentials_id)
            credentials = aws_hook.get_credentials()
            redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

            self.log.info("Copying data from S3 to Redshift")
            # rendered_key = self.s3_key.format(**context)
            s3_path = "s3://{}/{}".format(self.s3_bucket, self.data_path)
            formatted_sql = JsonToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.json_path
            )

            redshift.run(formatted_sql)
            self.log.info('Redshift Staging Tables Complete')


### 4.2 Data Quality Checks
To validate the processing of the Airflow DAG, the following validations were developed within the "CapstoneDataQualityOperator"
- NULL Check to make sure that all records loaded properly
- Record Length check to validate that all Redhsift Dimension Tables and Fact Table loaded properly
 
The following is a screenshot of 4 completed runs for the DAG, including Data Quality Checks:
![Completed Airflow Data Pipeline - 4 Runs](http://na-sjdemo1.marketo.com/rs/786-GZR-035/images/capstone_dag_completed_tree_view.png)

> Link to Image of Complete Airflow Data Pipeline        
> http://na-sjdemo1.marketo.com/rs/786-GZR-035/images/capstone_dag_completed_tree_view.png

#### Content of "capstone_data_quality.py"

    import logging

    from airflow.hooks.postgres_hook import PostgresHook
    from airflow.models import BaseOperator
    from airflow.utils.decorators import apply_defaults


    # Establishes the Class "DataQualityOperator" within the DAG
    # Designed to Run Data Quality Verifications on Capstone Analytics Tables

    class CapstoneDataQualityOperator(BaseOperator):

        ui_color = '#89DA59'


        # Defines default values from the Operator Call within the DAG, including context

        @apply_defaults
        def __init__(self,
                     redshift_conn_id="",
                     tables=[],
                     dq_checks=[],
                     *args, **kwargs):

            super(CapstoneDataQualityOperator, self).__init__(*args, **kwargs)
            self.tables = tables
            self.dq_checks = dq_checks
            self.redshift_conn_id = redshift_conn_id


        # Function to Run Data Quality Checks - including NULL check and Record Count

        def execute(self, context):
            redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

            # NULL Check Evaluation Logic

            for check in self.dq_checks:
                sql = check.get('check_sql')
                exp_result = check.get('expected_result')

                records = redshift.get_records(sql)[0]

                error_count = 0

                if exp_result != records[0]:
                    error_count += 1
                    failing_tests.append(sql)

                if error_count > 0:
                    self.log.info('SQL Tests failed')
                    self.log.info(failing_tests)
                    raise ValueError('Data quality check failed')

                if error_count == 0:
                    self.log.info('SQL Tests Passed')

            # Record Count Evaluation Logic

            for table in self.tables:

                records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")

                if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:

                    self.log.error(f"Data quality check failed. {table} returned no results")

                    raise ValueError(f"Data quality check failed. {table} returned no results")

                self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")

            self.log.info('Data Quality Check Complete')



### 4.3 Data dictionary 

The following is a summary of the data that is contained within the i94 Analytics Tables, including a description of the data and where it came from.


#### 4.3.1 **"i94_visit_details_fact"**

- This data comes from the US National Tourism and Trade Office and is a listing of visits to the United States and includes an appended Temp Id for Residence & Port.

> Link to Immigration Data Source
> https://travel.trade.gov/research/reports/i94/historical/2016.html


| Field Name        |   Data Type  | Field Length |   Constraint   | Accepts Null? |                  Description                 |
| :---              |     :----:   |    :----:    |     :----:     |    :----:     |                                         ---: |
| i94rec            |    Integer   |      10      |   Primary Key  |      No       |  Visit Details Record ID                     |
| i94_year          |    Integer   |      10      |       n/a      |      Yes      |  Visit Year                                  |
| i94_month         |    Integer   |      10      |       n/a      |      Yes      |  Visit Month                                 |
| i94_port_of_entry |    Varchar   |      30      |     Sort Key   |      Yes      |  Visit Entry Location                        |
| i94_arrival_state |    Varchar   |      30      |       n/a      |      Yes      |  Visit Entry State                           |
| i94_citizenship   |    Varchar   |      30      |       n/a      |      Yes      |  Visitor's Country of Citizenship            |
| i94_residence     |    Varchar   |      30      |       n/a      |      Yes      |  Visitor's Country of Residence              |
| arrival_mode      |    Integer   |      10      |       n/a      |      Yes      |  Visit Arrival Mode Code                     |
| arrival_date      |    Numeric   |      10      |       n/a      |      Yes      |  Visit Arrival Date                          |
| departure_date    |    Numeric   |      10      |       n/a      |      Yes      |  Visit Departure Date                        |
| i94_age           |    Integer   |      10      |       n/a      |      Yes      |  Visitor's Age                               |
| travel_purpose    |    Integer   |      10      |       n/a      |      Yes      |  Travel Purpose Code                         |
| birth_year        |    Integer   |      10      |       n/a      |      Yes      |  Visitor's Birth Year                        |
| gender            |    Varchar   |       2      |       n/a      |      Yes      |  Visitor's Gender                            |
| visa_type         |    Varchar   |      10      |       n/a      |      Yes      |  Visitor's Visa Type                         |
| residence_temp_id |    Integer   |      30      |       n/a      |      Yes      |  Id for Temperature at Country of Residence  |
| port_temp_id      |    Integer   |      30      |       n/a      |      Yes      |  Id for Temperature at Port of Arrival       |


#### **4.3.2 "temperatures_dim"**

This dataset came from Kaggle and correlates temperatures of US Destination and Country of Origen by Month.

> Link to World Temperature Data Source      
> https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data


| Field Name                |  Data Type   | Field Length |   Constraint   | Accepts Null? |                      Description                      |
| :---                      |    :----:    |    :----:    |     :----:     |    :----:     |                                                  ---: |
| temperature_id            |    Bigint    |      30      |   Primary Key  |      No       |  Temperatures Record ID                               |
| temp_month                |    Integer   |      10      |      n/a       |      Yes      |  Temperature Record Month                             |
| i94_state_code            |    Varchar   |      30      |      n/a       |      Yes      |  Location Code with Two Letters i.e. "GA"             |
| i94_country_code          |    Varchar   |      30      |      n/a       |      Yes      |  Location Code for Country by Full Name i.e. "Spain"  |
| temp_average              |    Integer   |      10      |      n/a       |      Yes      |  Average Temperature in Ferenheit                     |
| stat_count                |    Integer   |      10      |      n/a       |      Yes      |  Count of Datapoints that create average value        |
| temp_city                 |    Varchar   |      30      |      n/a       |      Yes      |  Temperature City Full Name i.e. "Atlanta"            |
| temp_state                |    Varchar   |      30      |      n/a       |      Yes      |  Temperature State Abbreviation Used for Matching     |
| temp_country              |    Varchar   |      30      |      n/a       |      Yes      |  Temperature Country Name Used for Matching           |


#### **4.3.3 "state_demographics_dim" and "ethnicity_by_state_dim"**

This data comes from OpenSoft and will be used to correlate US Destination Demographics by State.

> Link to U.S. City Demographic Data      
> https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/

**"state_demographics_dim"

| Field Name                |  Data Type   | Field Length |   Constraint   | Accepts Null? |                      Description                      |
| :---                      |    :----:    |    :----:    |     :----:     |    :----:     |                                                  ---: |
| i94_state_code            |    Varchar   |      10      |   Primary Key  |      No       |  Location Code ID for State, i.e. "GA"                |
| median_age                |    Integer   |      10      |       n/a      |      Yes      |  Average Age for State                                |
| male_population           |    Integer   |      20      |       n/a      |      Yes      |  Total of Male Population for State                   |
| female_population         |    Integer   |      20      |       n/a      |      Yes      |  Total of Female Population for State                 |
| total_population          |    Integer   |      20      |       n/a      |      Yes      |  Total Population for State                           |
| number_of_veterans        |    Integer   |      20      |       n/a      |      Yes      |  Total Veterans within State                          |
| foreign_born              |    Integer   |      20      |       n/a      |      Yes      |  Total of foreign born citizens within the State      |
| avg_hh_size               |    Integer   |       4      |       n/a      |      Yes      |  Average Number of Members within a Household         |


**"ethnicity_by_state_dim"**

| Field Name                |  Data Type   | Field Length |   Constraint   | Accepts Null? |                      Description                      |
| :---                      |    :----:    |    :----:    |     :----:     |    :----:     |                                                  ---: |
| i94_state_code            |    Varchar   |      10      |   Primary Key  |      No       |  Location Code ID for State, i.e. "GA"                |
| race                      |    Varchar   |      30      |       n/a      |      Yes      |  Ethnicity Category, i.e. "Hispanic or Latino         |
| state                     |    Varchar   |      10      |       n/a      |      Yes      |  State Id Used for Matching                           |
| count                     |    Integer   |      30      |       n/a      |      Yes      |  Total of Population by Ethnicity                     |


#### **4.3.4 "us_airport_size_dim"**

This is a simple table of airport codes and corresponding cities the will be used to correlate Airport Size to US Destination.

> Link to Airport Code Table      
> https://datahub.io/core/airport-codes#data

| Field Name                |  Data Type   | Field Length |   Constraint   | Accepts Null? |                      Description                      |
| :---                      |    :----:    |    :----:    |     :----:     |    :----:     |                                                  ---: |
| i94_port_code             |    Varchar   |      10      |   Primary Key  |      No       |  Location Code ID for City, i.e. "ORL"                |
| state_code                |    Varchar   |      10      |       n/a      |      Yes      |  State Code Used for Matching                         |
| type                      |    Varchar   |      10      |       n/a      |      Yes      |  Type of Ariport, i.e. "Large, Medium or Small"       |
| city                      |    Varchar   |      30      |       n/a      |      Yes      |  Full City Name                                       |
| ident                     |    Varchar   |      10      |       n/a      |      Yes      |  Airport Identifier for Future State Models           |


#### **4.3.5 "location_codes_dim"**

A table of aggregate Country & City Codes that related to the Location Fields within the i94 Immigration Data.

> File Name for Location Codes: "location_codes.csv"

| Field Name                |  Data Type   | Field Length |   Constraint   | Accepts Null? |                      Description                      |
| :---                      |    :----:    |    :----:    |     :----:     |    :----:     |                                                  ---: |
| location_code_id          |    Integer   |      30      |   Primary Key  |      No       |  Master Location Code                                 |
| country_code              |    Varchar   |      30      |       n/a      |      Yes      |  Country Location Code                                |
| country                   |    Varchar   |      30      |       n/a      |      Yes      |  Full Name of Country                                 |
| state_code                |    Varchar   |      10      |       n/a      |      Yes      |  State Location Code                                  |
| city                      |    Varchar   |      30      |       n/a      |      Yes      |  City Location Name                                   |
| state                     |    Varchar   |      30      |       n/a      |      Yes      |  Full State Name                                      |


#### **4.3.6 "travel_mode_dim"

A table of travel codes that denote the transportation type used to reach the United States.

> File Name for Travel Mode Codes: "travel_mode_code.csv"

| Field Name                |  Data Type   | Field Length |   Constraint   | Accepts Null? |                      Description                      |
| :---                      |    :----:    |    :----:    |     :----:     |    :----:     |                                                  ---: |
| travel_mode_code          |    Varchar   |       2      |   Primary Key  |      No       |  Master Travel Mode Code                              |
| mode                      |    Varchar   |      10      |       n/a      |      Yes      |  Travel Mode Detailed Description, i.e. "Air"         |


#### **4.3.7 "travel_purpose_dim"

A table of travel purpose codes that denotes the main reason for visiting the United States.

> File Name for Travel Purpose Codes: "travel_purpose.csv"

| Field Name                |  Data Type   | Field Length |   Constraint   | Accepts Null? |                      Description                      |
| :---                      |    :----:    |    :----:    |     :----:     |    :----:     |                                                  ---: |
| travel_purpose_code       |    Varchar   |       2      |   Primary Key  |      No       |  Master Travel Purpose Code                           |
| travel_purpose            |    Varchar   |      10      |       n/a      |      Yes      |  Travel Purpose Detailed Description, i.e. "Business" |


#### "4.3.8 "visa_type_codes_dim"

A table of Visa Type Codes that denotes the classification of the visit to the United States.

> File Name for Visa Type Codes: "visa_type_codes.csv"

| Field Name                |  Data Type   | Field Length |   Constraint   | Accepts Null? |                      Description                      |
| :---                      |    :----:    |    :----:    |     :----:     |    :----:     |                                                  ---: |
| visa_code                 |    Varchar   |       4      |   Primary Key  |      No       |  Master Visa Type Code                                |
| visa_category             |    Varchar   |      10      |       n/a      |      Yes      |  Main Category for Visa Type, i.e. "Travel Visa"      |
| visa_travel_purpose       |    Varchar   |      30      |       n/a      |      Yes      |  Detailed Description of Visa Type                    |



## Step 5: Final Project Write Up

### 5.1 Rationale for Tools & Technologies within the Project

The purpose of this project was to setup a repeatable, scalable process for analyzing i94 Travel Details to understand potential correlations to other data sets such as temperature, time of year, demographics of destination, and airport size. To support the simple, yet scalable use of data for analytics, a Fact & Dimensions Star Schema was developed to make it easy for teams using Redshift to access information that orginally was in multiple different formats.

To make analytics simple, a central i94_travel_details Fact Table was produced that directly correlates to associated dimension tables through the use of primary and foreign keys. Additionally, pre-computed information such as a the sum of demographics and ethnicity were incorporated into the data model and ETL process to reduce the time required by analytics teams to wrangle data.

Pyspark was chosen as for the technology foundation of the ETL based on the ability to handle large data sets and to create "schema-on-read", which helped with both the setup, configuration, and processing of the ETL. Amazon S3 was chosen as the storage location based on accessibility. Apache Airflow was selected for ingestion and validation of data into Redshift for use by the analytics team.

The Pyspark ETL process was configured in a scalable way so that continued research into data quality and improved data sources could easily mirror the current setup with future additions or changes.

### 5.2 Recommendations for Future Data Updates

To build upon the current ETL Process, it is recommended that a monthly update of new i94 Travel Information be added to the current data set. Given the gaps within Location Tables information within the i94 Travel Details, it is recommended that an improved key be developed by US Immigration Services for Location information within the i94 core data set and potentially standardized to international encoding. Temperature Data would benefit from paid access to Temperature information from the listed sources to fill in gaps. Provided the considerations here, Temperature, Demographics, and Airport Data should be updated every 6 months to 1 year to aid in analytics accuracy.

### 5.3 Scaling Considerations for the Project
Write a description of how you would approach the problem differently under the following scenarios:

#### The data was increased by 100x.
Provided a drastic increase in the volume of data, i.e. if the data were to be increased by 100x, the current process would still hold up, though it might be wise to move the ETL process to be controlled within Airflow as well. The current process for loading information from sources to Staging Buckets within Amazon S3 works from the full data set and Airflow would help to break up the ETL calls into batches by date, i.e. month or week. Additionally, the current processing for ETL through EMR uses an Cluster with 1 Core and 3 Supporting Clusters, depending on configuration of Airflow or the ETL, it is recommended that 1 Additional Supporting Cluster be added for every 2 Million Rows of i94 Data that are moved through the ETL.

#### The data populates a dashboard that must be updated on a daily basis by 7am every day.
If data were to be used in a realtime dashboard that updates on a daily basis by 7am every day, it is recommended that Airflow be used for both ETL Processing of new information and Setup of Redshift Analytics. Airflow processing would be moved from a monthly setting to a daily setting within the DAG. Given that the volume of information to be appended daily is relatively low, these systems could run nightly around 10pm to process data on non-peak hours through AWS and to ensure that any potential issues that arise could be addressed in the evening so that information is reliably available at 7am. Additionally, email alerts could be configured through Airflow to ensure that issues are known and addressable by the support team. 

#### The database needed to be accessed by 100+ people.
If the database needs to be accessed by 100+ people, it would be important to setup preconfigured Redshift queries for users to help with less technical users and preventing non-power users from directly interfacing with the Redshift dataset. A structure would need to be put in place to handle questions, data quality issues, and escalations to ensure that users are consistently able access the information. It would be essential to have a dashboard that monitors data ingestion status, as well as a set of alerts that go out to Power Users of the Analytics Database should any errors or issues arrive.

### X. Final Notes from the Project Author

It has been a real pleasure taking this class through Udacity, along with the assistance from the mentors. The knowledge learned within the class has delivered real-world skills and challenged students so that learnings can be applied with confidence and accuracy. Thank you to all of staff of Udacity for timely answers and helpful direction throughout the course!
