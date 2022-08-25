FROM quay.io/astronomer/astro-runtime:5.0.4
# TODO Think this is working.... test?
ENV AIRFLOW_VAR_CONN_S3_DATALAKE=s3://?aws_access_key_id=AKIAV67XDIJQYFHWSJZB&aws_secret_access_key=qiR73Y%2B1dPWxGQmiZGs3n9SJJrpJ3JREhJlb4NvQ@

USER root
RUN apt-get update \
    && apt-get -y install wget