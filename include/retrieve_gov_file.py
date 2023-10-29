import shutil
import wget
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from zipfile import ZipFile
import subprocess, os


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
    """
    Uploads a target file to s3
    :return: None
    """
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    s3_hook.load_file(filename=filename, key=key, bucket_name=bucket, replace=True)


def retrieve_gov_file(filename, file_url, bucket, s3_conn_id):
    """
    Downloads a single file to a temporary directory, recursively unzips it, and uploads it to s3
    :return: none
    """
    download_dest = "/tmp/" + filename
    wget.download(file_url, download_dest)
    print("clearing tmp directory")
    for root, dirs, files in os.walk("/tmp/prepped"):
        for file in files:
            os.remove(os.path.join(root, file))
    print("tmp directory cleared")
    print(download_dest + " downloaded")
    if filename.endswith(".zip"):
        unpack_zip(download_dest, "/tmp/prepped/")
    else:
        shutil.move(
            download_dest,
            "/tmp/prepped/" + os.path.basename(download_dest).split("/")[-1],
        )
    for file in os.listdir("/tmp/prepped/"):
        if file.endswith(".mdb"):
            print(file + " identified as .mdb")
            try:
                table_names = subprocess.Popen(
                    "mdb-tables /tmp/prepped/" + file,
                    stdout=subprocess.PIPE,
                    shell=True,
                )
                output = table_names.communicate()[0].decode("ascii")
                print(output)
                tables = output.split(" ")
                print(tables)
            except subprocess.CalledProcessError as e:
                raise RuntimeError(
                    "command '{}' return with error (code {}): {}".format(
                        e.cmd, e.returncode, e.output
                    )
                )
            for table in tables:
                if table != "" and table != "\n":
                    export_file = (
                        "/tmp/prepped/"
                        + os.path.splitext(file)[0]
                        + "_"
                        + table.replace(" ", "_")
                        + ".csv"
                    )
                    print("Exporting " + table)
                    with open(export_file, "wb") as f:
                        try:
                            subprocess.check_call(
                                ["mdb-export", "/tmp/prepped/" + file, table], stdout=f
                            )
                        except subprocess.CalledProcessError as e:
                            raise RuntimeError(
                                "command '{}' return with error (code {}): {}".format(
                                    e.cmd, e.returncode, e.output
                                )
                            )

    for file in os.listdir("/tmp/prepped/"):
        print(file + " found in /tmp/prepped/ for sending to S3")
        if file.endswith(".csv"):
            OBJECT = file.replace(" ", "")
            PATH_TO_FILE = "/tmp/prepped/" + file
            BUCKET = bucket
            upload_to_s3(
                s3_conn_id=s3_conn_id,
                filename=PATH_TO_FILE,
                bucket=BUCKET,
                key=OBJECT,
            )
            print(file + " successfully loaded to S3")
