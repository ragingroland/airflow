from airflow import DAG
from airflow.providers.vertica.operators.vertica import VerticaOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'vertica_connection_test',
    default_args=default_args,
    description='A simple DAG to test Vertica connection',
    schedule_interval=None,  # This DAG will not run on a schedule, only when triggered
    start_date=days_ago(1),
    catchup=False,
)

# SQL query to test connection
test_query = 'SELECT 1;'

test_vertica_connection = VerticaOperator(
    task_id='test_vertica_connection',
    sql=test_query,
    vertica_conn_id='vertica_default',  # Connection ID configured in Airflow UI
    dag=dag,
)

test_vertica_connection