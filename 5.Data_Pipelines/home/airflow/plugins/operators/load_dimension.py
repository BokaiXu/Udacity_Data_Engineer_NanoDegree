from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 append=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append=append

    def execute(self, context):
        self.log.info('LoadDimensionOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if not self.append:
            self.log.info('Remove data from table {}.'.format(self.table))
            redshift.run('DELETE FROM {}'.format(self.table))
        else:
            self.log.info('Appending data from table {}.'.format(self.table))
            redshift.run("INSERT INTO {} {}".format(self.table, self.sql))
