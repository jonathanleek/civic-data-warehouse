#!/bin/bash
echo "Creating S3 bucket: civic-data-warehouse-lz"
awslocal s3 mb s3://civic-data-warehouse-lz
echo "S3 bucket created successfully"
