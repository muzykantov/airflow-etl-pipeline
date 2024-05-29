#!/bin/bash

docker compose run webserver airflow users create \
    --username muzykantov \
    --password passw0rd \
    --firstname Gennadii \
    --lastname Muzykantov \
    --role Admin \
    --email gennadii@muzykantov.me
