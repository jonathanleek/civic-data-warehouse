import shutil
import wget
import os
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from zipfile import ZipFile

def unpack_zip(path_to_zip, extract_path):
    """
    Recursively unzips a file and outputs its content to a directory.
    :return: None
    """
    parent_archive = ZipFile(path_to_zip)
    parent_archive.extractall(extract_path)
    namelist = parent_archive.namelist()
    parent_archive.close()
    for name in namelist:
        try:
            if name.endswith(".zip"):
                unpack_zip(path_to_zip=extract_path + name, extract_path=extract_path)
                os.remove(extract_path + name)
        except:
            print(name + " is not a .zip")
            print(extract_path + name)
            pass


def upload_to_s3(s3_conn_id, filename, key, bucket):
    # TODO This function requires testing
    """
    Uploads a target file to s3
    :return: None
    """
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    s3_hook.load_file(filename=filename, key=key, bucket_name=bucket, replace=True)


def retrieve_gov_file(file_name, file_url, bucket, s3_conn_id):
    # TODO This function requires testing
    """
    Downloads a single file to a temporary directory, recursively unzips it, and uploads it to s3
    :return: none
    """
    download_dest = "/tmp/" + file_name
    wget.download(file_url, download_dest)
    print(download_dest + " downloaded")
    if file_name.endswith(".zip"):
        unpack_zip(download_dest, "/tmp/prepped/")
        os.remove(download_dest)
    else:
        shutil.move(
            download_dest,
            "/tmp/prepped/" + os.path.basename(download_dest).split("/")[-1],
        )
        os.remove(download_dest)
    for file in os.listdir("/tmp/prepped/"):
        OBJECT = file
        PATH_TO_FILE = "/tmp/prepped/" + file
        BUCKET = bucket
        upload_to_s3(
            s3_conn_id=s3_conn_id, filename=PATH_TO_FILE, bucket=BUCKET, key=OBJECT
        )