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