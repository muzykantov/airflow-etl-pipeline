FROM apache/airflow:latest

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential

RUN mkdir -p /opt/airflow/dags && chmod -R a+w /opt/airflow/dags

USER airflow

RUN pip install tqdm pandas pendulum