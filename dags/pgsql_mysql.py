import datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.email_operator import EmailOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024, 5, 6),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2
}

DAG_ID = "postgres_operator_dag"

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule="@once",
    catchup=False,
) as dag:
    create_pet_table_pg = PostgresOperator(
        task_id="create_pet_table_pgsql",
        sql="""
            CREATE TABLE IF NOT EXISTS pet (
            pet_id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            pet_type VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            OWNER VARCHAR NOT NULL);
          """,
        postgres_conn_id = 'postgres'
    )
    create_pet_table_mysql = MySqlOperator(
        task_id="create_pet_table_mysql",
        sql="""
            CREATE TABLE IF NOT EXISTS pet (
                pet_id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                pet_type VARCHAR(255) NOT NULL,
                birth_date DATE NOT NULL,
                OWNER VARCHAR(255) NOT NULL
            );
            """,
        mysql_conn_id = 'mysql'
    )
    populate_pet_table_pgsql = PostgresOperator(
        task_id="populate_pet_table_pgsql",
        sql="""
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Max', 'Dog', '2018-07-05', 'Jane');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Susie', 'Cat', '2019-05-01', 'Phil');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Lester', 'Hamster', '2020-06-23', 'Lily');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Quincy', 'Parrot', '2013-08-11', 'Anne');
            """,
        postgres_conn_id = 'postgres'
    )
    populate_pet_table_mysql = MySqlOperator(
        task_id="populate_pet_table_mysql",
        sql="""
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ('Max', 'Dog', '2018-07-05', 'Jane');

            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ('Susie', 'Cat', '2019-05-01', 'Phil');

            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ('Lester', 'Hamster', '2020-06-23', 'Lily');

            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ('Quincy', 'Parrot', '2013-08-11', 'Anne');
            
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ('Samurai', 'Dog', '2010-08-11', 'Lyudmila');
            """,
        mysql_conn_id = 'mysql'
    )
    
    get_all_pets_pgsql = PostgresOperator(task_id="get_all_pets_pgsql", sql="SELECT * FROM pet;",
    postgres_conn_id = 'postgres')
    get_all_pets_mysql = MySqlOperator(task_id="get_all_pets_mysql", sql="SELECT * FROM pet;",
    mysql_conn_id = 'mysql')
    
    get_birth_date_pgsql = PostgresOperator(
        task_id="get_birth_date_pgsql",
        sql="SELECT * FROM pet WHERE birth_date BETWEEN SYMMETRIC %(begin_date)s AND %(end_date)s",
        parameters={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
        hook_params={"options": "-c statement_timeout=3000ms"},
        postgres_conn_id = 'postgres'
    )
    get_birth_date_mysql = MySqlOperator(
    task_id="get_birth_date_mysql",
    sql="SELECT birth_date FROM pet",
    mysql_conn_id = 'mysql'
    )
    migrate_pg_my = MySqlOperator(
        task_id = "getfrommysql",
        sql = 'SELECT * FROM pet WHERE name = "Samurai"',
        mysql_conn_id = 'mysql'
    )
    
    def insert_to_postgres(**kwargs):
        ti = kwargs['ti']
        mysql_result = ti.xcom_pull(task_ids='getfrommysql')
        postgres_hook = PostgresHook(postgres_conn_id='postgres')
        postgres_conn = postgres_hook.get_conn()
        cursor = postgres_conn.cursor()
        cursor.execute("""
        INSERT INTO pet (pet_id, name, pet_type, birth_date, owner) VALUES (%s, %s, %s, %s, %s) 
        WHERE NOT EXISTS (SELECT * FROM pet WHERE name = 'Samurai')""", mysql_result[0])
        postgres_conn.commit()
        cursor.close()
        postgres_conn.close()

    mysqlto_postgres = PythonOperator(
        task_id='insert_to_postgres',
        python_callable=insert_to_postgres,
        provide_context=True,
    )
    email_operator = EmailOperator(
    task_id='send_email_on_success',
    to='reasonabledecision@gmail.com',
    subject='Airflow is doing good',
    html_content='No bother, all is OK.',
    retries=0,
    trigger_rule='all_success'
    )

    create_pet_table_pg >> create_pet_table_mysql >> populate_pet_table_pgsql >> populate_pet_table_mysql >> get_all_pets_pgsql >> get_all_pets_mysql >> get_birth_date_pgsql >> get_birth_date_mysql >> migrate_pg_my >> mysqlto_postgres >> email_operator