    from datetime import datetime, timedelta

    import pandas as pd
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from dags.transform_script import transfrom

    default_args = {
        "owner": "Gennadii Muzykantov",
        "depends_on_past": False,
        "start_date": datetime(2023, 10, 1),
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }

    dag = DAG(
        "client_activity_etl_GennadiiMuzykantov",
        default_args=default_args,
        description="ETL process for client activity",
        schedule_interval="0 0 5 * *",
        catchup=False,
    )


    def extract_data(**kwargs):
        df = pd.read_csv("/opt/airflow/dags/profit_table.csv")
        kwargs["ti"].xcom_push(key="dataframe", value=df.to_dict())
        return df


    def transform_data(**kwargs):
        df_dict = kwargs["ti"].xcom_pull(key="dataframe", task_ids="extract_data")
        df = pd.DataFrame.from_dict(df_dict)
        result = transform(df)
        kwargs["ti"].xcom_push(key="transformed_data", value=result.to_dict())
        return result


    def load_data(**kwargs):
        result_dict = kwargs["ti"].xcom_pull(
            key="transformed_data", task_ids="transform_data"
        )
        result = pd.DataFrame.from_dict(result_dict)
        result.to_csv(
            "/opt/airflow/dags/flags_activity.csv", mode="a", header=False, index=False
        )


    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        provide_context=True,
        dag=dag,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        provide_context=True,
        dag=dag,
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        provide_context=True,
        dag=dag,
    )

    extract_task >> transform_task >> load_task
