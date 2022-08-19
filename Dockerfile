FROM quay.io/astronomer/astro-runtime:5.0.4
# TODO Add connection uri
ENV AIRFLOW_CONN_S3_DATALAKE=<connection-uri>

USER root

RUN apt-get update \
    && apt-get -y install wget