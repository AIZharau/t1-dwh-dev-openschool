"""
TASK 3
PIPELINE CREATION, PART 1. PRACTICE
Generate test data and put it into a postgress table
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
import logging
import csv
import hashlib
import random as rn

TMP_DATA_FILE = "/tmp/dwh_4_t1_data_zharau.csv"
PROCESSED_DATA_DIR = "/tmp/processed_data"
POSTGRES_CONN_ID = "dwh_4_t1_zharau_conn_pg"
TABLE_NAME = 'homework.test_table_zharau'

SQL_QUERY = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id INTEGER,
        value TEXT
    );
"""

def generate_data(**kwargs):
    '''Creation test-data'''
    data = []
    for i in range(100):
        data.append(
            {
                "id": i + 1,
                "value": hashlib.md5(
                    f"dwh4_a_zharau_{rn.randrange(1000)}".encode()
                ).hexdigest()
            }
        )

    with open(TMP_DATA_FILE, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["id", "value"])
        writer.writeheader()
        writer.writerows(data)

    logging.info(f"Data successfully generated and saved to: {TMP_DATA_FILE}")
    kwargs["ti"].xcom_push(key="data_file_path", value=TMP_DATA_FILE)


def load_data_to_postgres(**kwargs):
    data_file_path = f"{PROCESSED_DATA_DIR}/dwh_4_t1_data_zharau.csv"

    with open(data_file_path, mode='r') as file:
        reader = csv.reader(file)
        next(reader)
        
        with PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn() as conn:
            with conn.cursor() as cursor:
                records = 0
                for row in reader:
                    cursor.execute(
                        f"""INSERT INTO {TABLE_NAME} (id, value) 
                        VALUES (%s, %s)""",
                        (int(row[0]), row[1]),
                    )
                    records += 1
                conn.commit()
                logging.info(f"Data successfully loaded into table {TABLE_NAME}")
                kwargs["ti"].xcom_push(key="num_records", value=records)


def log_num_records(**kwargs):
    num_records = kwargs["ti"].xcom_pull(key="num_records", task_ids="load_data_task")
    logging.info(f"Number of records loaded: {num_records}")

DEFAULT_ARGS = {
    'owner': 'a.zharau',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG("dwh_4_t1_zharau",
         default_args=DEFAULT_ARGS,
         description='Generate test data and put it into a postgress table',
         schedule_interval="@daily",
         max_active_runs=1,
         catchup=False,
         tags=['rama'],
         ) as dag:

    generate_data_task = PythonOperator(
        task_id="generate_data_task",
        python_callable=generate_data,
    )

    move_file_task = BashOperator(
        task_id="movefile_task",
        bash_command=f'mv {{{{ ti.xcom_pull(key="data_file_path", task_ids="generate_data_task") }}}} {PROCESSED_DATA_DIR}/dwh_4_t1_data_zharau.csv',
    )

    create_table_task = PostgresOperator(
        task_id="create_table_task",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=SQL_QUERY,
    )

    load_data_task = PythonOperator(
        task_id="load_data_task",
        python_callable=load_data_to_postgres,
    )

    log_records_task = PythonOperator(
        task_id="log_records_task",
        python_callable=log_num_records,
    )

    generate_data_task >> move_file_task >> create_table_task >> load_data_task >> log_records_task