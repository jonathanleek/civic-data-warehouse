Overview
========
The Civic Data Warehouse (CDW) is an attempt at a data warehouse of publicly available government data.

Objectives
==========
1. Create an easy to use repository of government data to enable development of future research and development projects.
2. Provide a template for similar efforts in other cities with similar needs.
3. Create an open standard for the data schema of a civic data warehouse, so that other cities that follow this example will be able to collaborate on development of and share future data tools.

Architecture
============
All data pipelines will be built using python and sql as DAGs for Apache Airflow. An S3 bucket will be used as a datalake, and Postgres will serve as the data warehouse itself.

All database schema will be recorded in DBML, which can be easily edited and viewed with tools available at https://dbdiagram.io

Development
===========
Development and testing are being done using a local Airflow environment created using the Astro CLI. For information on setting up your own local environment for development, please consult https://docs.astronomer.io
