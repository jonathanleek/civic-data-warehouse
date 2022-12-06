Overview
========
The dags in the repository are designed to ingest data from the Saint Louis City open data portal and turn them into a functional data warehouse. The specifics of this process are outlined in the dags, but this file provides a general overview of the process.

A list of files to be ingested is stored in /include/gov_files.json

Each file is downloaded to a worker, which recursively unzips it until the root file is discovered. The file is then turned into one or more .csv files and uploaded to an S3 bucket.