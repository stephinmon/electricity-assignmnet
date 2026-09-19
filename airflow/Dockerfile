FROM apache/airflow:3.2.2-python3.11

COPY requirements.txt /requirements.txt

USER airflow
RUN pip install --no-cache-dir \
    -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.2.2/constraints-3.11.txt"

USER root
COPY --chown=airflow:root dags /opt/airflow/dags
USER airflow

ARG ELECTRICITY_ETL_JOB_ID=
ARG ELECTRICITY_ETL_JOB_NAME=electricity-etl
ENV ELECTRICITY_ETL_JOB_ID=${ELECTRICITY_ETL_JOB_ID} \
    ELECTRICITY_ETL_JOB_NAME=${ELECTRICITY_ETL_JOB_NAME}
