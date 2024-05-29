import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Импорт функции трансформации
from dags.transform_script import transform

default_args = {
    "owner": "Gennadii Muzykantov",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "customer_activity_etl_gennadii_muzykantov",
    default_args=default_args,
    description="ETL for customer activity",
    schedule_interval="0 0 5 * *",  # Запуск 5-го числа каждого месяца
    start_date=days_ago(1),
    catchup=False,
)


def extract():
    # Чтение данных из CSV-файла
    df = pd.read_csv("/opt/airflow/dags/profit_table.csv")
    return df


def transform_data(**kwargs):
    # Применение функции трансформации
    df = kwargs["ti"].xcom_pull(task_ids="extract")
    # Использование текущей даты
    date = datetime.now().strftime("%Y-%m-%d")
    transformed_df = transform(df, date)
    return transformed_df


def load(**kwargs):
    # Сохранение данных в CSV-файл с добавлением новых данных без перезаписи
    transformed_df = kwargs["ti"].xcom_pull(task_ids="transform")
    output_path = "/opt/airflow/dags/flags_activity.csv"
    if os.path.exists(output_path):
        transformed_df.to_csv(output_path, mode="a", header=False, index=False)
    else:
        transformed_df.to_csv(output_path, index=False)


extract_task = PythonOperator(
    task_id="extract",
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform",
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load",
    python_callable=load,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_task
