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