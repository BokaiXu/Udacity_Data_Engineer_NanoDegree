from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_check=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_check = table_check

    def execute(self, context):
        self.log.info('DataQualityOperator')
        redshift_hook = PostgresHook(self.redshift_conn_id)

        error_count = 0
        failing_tests = []

        for table_ in self.table_check:
            sql = table_.get('check_sql')
            exp_result = table_.get('expected_result')
            records = redshift_hook.get_records(sql)[0]

            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(sql)
        if error_count > 0:
            self.log.info('Tests failed')
            self.log.info(failing_tests)
            raise ValueError('Data quality check failed')
        self.log.info('Data Quality checks passed!')     
            