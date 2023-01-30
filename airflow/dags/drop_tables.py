from datetime import datetime, timedelta
import datetime
import os

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

from helpers import SqlQueries

from airflow import conf

# Default args 
default_args = {
    'owner': 'Ajay Jain',
    'depends_on_past': False,
    'start_date': datetime.datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'catchup': False,
    'retry_delay': timedelta(minutes=3)
}

dag_deletetables = DAG(
    'DeleteTablesDag',
    description='Delete the tables',
    default_args = default_args,
    start_date = datetime.datetime.now(),
    max_active_runs=3,
    schedule_interval='0 * * * *'
)


#dags_folder = /home/workspace/airflow/dags
#core is Section in configration file placed under/opt/airflow/airflow.cfg

# To Drop tables - only ran once 
f_drop_table= open(os.path.join(conf.get('core','dags_folder'),'drop_tables.sql'))
drop_tables_sql = f_drop_table.read()

drop_trips_table = PostgresOperator(
    task_id="drop_trips_table",
    dag=dag_deletetables,
    postgres_conn_id="redshift",
    sql=drop_tables_sql
)


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag_deletetables)
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag_deletetables)

start_operator>>drop_trips_table>>end_operator