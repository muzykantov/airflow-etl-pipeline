import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Импорт функции трансформации
from dags.transform_script import transform_product

default_args = {
    "owner": "Gennadii Muzykantov",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "customer_activity_etl_parallel_gennadii_muzykantov",
    default_args=default_args,
    description="Parallel ETL for customer activity",
    schedule_interval="0 0 5 * *",  # Запуск 5-го числа каждого месяца
    start_date=days_ago(1),
    catchup=False,
)


def extract():
    # Чтение данных из CSV-файла
    df = pd.read_csv("/opt/airflow/dags/profit_table.csv")
    return df


def transform_data(product, **kwargs):
    # Применение функции трансформации для конкретного продукта
    df = kwargs["ti"].xcom_pull(task_ids="extract")
    date = datetime.now().strftime("%Y-%m-%d")
    transformed_df = transform_product(df, date, product)
    return transformed_df


def load(product, **kwargs):
    # Сохранение данных в CSV-файл с добавлением новых данных без перезаписи
    transformed_df = kwargs["ti"].xcom_pull(task_ids=f"transform_{product}")
    output_path = f"/opt/airflow/dags/flags_activity_{product}.csv"
    if os.path.exists(output_path):
        transformed_df.to_csv(output_path, mode="a", header=False, index=False)
    else:
        transformed_df.to_csv(output_path, index=False)


products = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]

extract_task = PythonOperator(
    task_id="extract",
    python_callable=extract,
    dag=dag,
)

for product in products:
    transform_task = PythonOperator(
        task_id=f"transform_{product}",
        python_callable=transform_data,
        provide_context=True,
        op_kwargs={"product": product},
        dag=dag,
    )

    load_task = PythonOperator(
        task_id=f"load_{product}",
        python_callable=load,
        provide_context=True,
        op_kwargs={"product": product},
        dag=dag,
    )

    extract_task >> transform_task >> load_task
