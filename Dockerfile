FROM apache/airflow:latest

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential

USER airflow

RUN pip install tqdm pandas pendulum
