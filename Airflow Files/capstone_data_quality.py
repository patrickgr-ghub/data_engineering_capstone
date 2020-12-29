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