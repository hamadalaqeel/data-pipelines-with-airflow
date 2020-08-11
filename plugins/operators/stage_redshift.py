from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    template_fields = ('s3_key',)
    
    copy_sql = """
               COPY {table_name}
               FROM '{s3_path}'
               ACCESS_KEY_ID '{access_key}'
               SECERT_ACCESS_KEY '{secert_key}'
               REGION 'us-west-2'
               {file_type}
               fileformat as 'epochmillisecs'
               """
    
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 table = "",
                 s3_bucket = "",
                 s3_key = "",
                 file_type = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_type = file_type

    def execute(self, context):
        #Step 1: connection
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        #Step 2: clear execting raw data
        self.log.info('clearning data from destination Redshift table')
        redshift.run("DELETE FROM {}".format(self.table))
        
        #Step 3: copy the new raw data
        self.log.info(f"In progress: Copying {self.table} from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket,rendered_key)
        
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
                        table_name = self.table,
                        s3_path = s3_path,
                        access_key = credentials.access_key,
                        secert_key = credentials.secert_key,
                        file_type = self.file_type
                        )
        
        redshift.run(formatted_sql)
        self.log_info(f"Done: Copying {self.table} from S3 to Redshift")
        





