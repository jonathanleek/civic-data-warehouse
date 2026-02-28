import subprocess, os
import shutil, wget
import pandas
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from zipfile import ZipFile
from airflow.utils.log import logging_mixin

def retrieve_gov_file(filename, file_url, bucket, s3_conn_id, task_id, base_prep_dir):
    """
    Downloads a single file to a temporary directory, recursively unzips it, converts .mdb files if needed, 
     and uploads it to s3
    :return: none
    """
    logger = logging_mixin.LoggingMixin().logger()

    prep_dir = build_temp_subdirectory(task_id, base_prep_dir, logger)

    download_dest = download_gov_file(filename, file_url, base_prep_dir, logger)

    if filename.endswith(".zip"):
        unpack_zip(download_dest, prep_dir, logger)
    else:
        logger.info(f"Moving {download_dest} into {prep_dir} directory")
        shutil.move(download_dest, prep_dir)
        
    for file in os.listdir(prep_dir):
        logger.info(f"{file} found in {prep_dir} for sending to S3")
        if file.endswith(".mdb"):
            export_mdb_file(bucket, s3_conn_id, logger, file, prep_dir)
        elif file.endswith(".xls"):
            export_xls_file(bucket, s3_conn_id, logger, file, prep_dir)
        elif file.endswith(".txt"):
            export_txt_file(bucket, s3_conn_id, logger, file, prep_dir)
        elif file.endswith(".csv"):
            upload_to_s3(bucket, s3_conn_id, logger, file, prep_dir)
        else:
            logger.warning(f"{file} is not an .mdb or .csv and is ignored")


def build_temp_subdirectory(task_id, base_prep_dir, logger):
    """
    Generate a subdirectory to contain task-specific files. Ensure directory exists and is empty.
    :return: temp directory to use
    """

    # Feel free to use any naming scheme you want here. Using the task_id was the simplest option,
    #  but possibly not the best.
    safe_dirname = "".join(char for char in task_id if char.isalnum())
    prep_dir = os.path.join(base_prep_dir, safe_dirname[:99])
    logger.info(f"using tmp directory '{prep_dir}'")
        
    if not os.path.exists(prep_dir):
        logger.info(f"Creating directory {prep_dir}")
        os.makedirs(prep_dir)

    clear_files_and_subdirs(prep_dir)
    
    return prep_dir


def download_gov_file(filename, file_url, base_prep_dir, logger):
    """
    Download the source file to the base prep directory.
    :return: full path to downloaded file
    """
    download_dest = os.path.join(base_prep_dir, filename)
    wget.download(file_url, download_dest)
    logger.info(f"{download_dest} downloaded")
    return download_dest


def unpack_zip(path_to_zip, extract_path, logger):
    """
    Recursively unzips a file and outputs its content to a directory.
    :return: None
    """
    logger.info(f"unzipping {path_to_zip} into {extract_path}")
    parent_archive = ZipFile(path_to_zip)
    parent_archive.extractall(extract_path)
    namelist = parent_archive.namelist()
    parent_archive.close()
    for name in namelist:
        try:
            if name.endswith(".zip"):
                inner_zipfile = os.path.join(extract_path, name)
                unpack_zip(path_to_zip=inner_zipfile, extract_path=extract_path, logger=logger)
                os.remove(inner_zipfile)
                logger.info(f"{name} successfully unzipped")
        except:
            logger.error(f"{name} is not a .zip - full path : {os.path.join(extract_path, name)}")
            pass


def upload_to_s3(bucket, s3_conn_id, logger, file, prep_dir):
    """
    Uploads a target file to s3
    :return: None
    """
    key = file.replace(" ", "")
    source_file = os.path.join(prep_dir, file)
    logger.info(f"Moving {source_file} into s3 bucket {bucket}")
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    s3_hook.load_file(filename=source_file, key=key, bucket_name=bucket, replace=True)
    logger.info(f"{source_file} successfully uploaded to s3 in bucket {bucket} and with key {key}")
    logger.info(f"{file} successfully loaded to S3")


def export_txt_file(bucket, s3_conn_id, logger, file, prep_dir):
    """
    Test the file to see if it's a valid .csv. If so, rename it and upload it.
    :return: None
    """
    logger.info(f"{file} identified as .txt")
    prepped_file = os.path.join(prep_dir, file)
    try:
        pandas.read_csv(prepped_file, dtype=str)
    except Exception:
        logger.exception(f"file {prepped_file} does not appear to be a csv")
        raise
    csv_filename = os.path.splitext(file)[0] + ".csv"
    full_csv_filename = os.path.join(prep_dir, csv_filename)
    shutil.move(prepped_file, full_csv_filename)
    logger.info(f"renamed {prepped_file} to {full_csv_filename}")
    upload_to_s3(bucket, s3_conn_id, logger, csv_filename, prep_dir)


def export_xls_file(bucket, s3_conn_id, logger, file, prep_dir):
    """
    Convert the .xls file to a .csv and upload it.
    :return: None
    """
    logger.info(f"{file} identified as .xls")
    prepped_file = os.path.join(prep_dir, file)
    excel_dataframe = pandas.read_excel(prepped_file, engine="xlrd", dtype=str)
    csv_filename = os.path.splitext(file)[0] + ".csv"
    full_csv_filename = os.path.join(prep_dir, csv_filename)
    excel_dataframe.to_csv(full_csv_filename)
    logger.info(f"loaded {prepped_file} and written all data to {full_csv_filename}")
    upload_to_s3(bucket, s3_conn_id, logger, csv_filename, prep_dir)

def export_mdb_file(bucket, s3_conn_id, logger, file, prep_dir):
    """
    Strip all tables in an .mdb file into separate .csv files. Upload the resulting .csv files to S3.
    :return: None
    """
    logger.info(f"{file} identified as .mdb")
    try:
        prepped_file = os.path.join(prep_dir, file)
        cmd_open_mdb = f"mdb-tables {prepped_file}"
        logger.info(f"executing command {cmd_open_mdb}")
        table_names = subprocess.Popen(
                    cmd_open_mdb,
                    stdout=subprocess.PIPE,
                    shell=True,
                )
        output = table_names.communicate()[0].decode("ascii")
        logger.debug(output)
        tables = output.split(" ")
        logger.debug(tables)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
                    "command '{}' return with error (code {}): {}".format(
                        e.cmd, e.returncode, e.output
                    )
                )
    for table in tables:
        if table != "" and table != "\n":
            export_file = os.path.splitext(file)[0] + "_" + table.replace(" ", "_") + ".csv"
            export_fullpath = os.path.join(prep_dir, export_file)
            logger.info(f"Exporting {table} to {export_fullpath}")
            with open(export_fullpath, "wb") as f:
                try:
                    subprocess.check_call(
                                ["mdb-export", prepped_file, table], stdout=f
                            )
                except subprocess.CalledProcessError as e:
                    raise RuntimeError(
                                "command '{}' return with error (code {}): {}".format(
                                    e.cmd, e.returncode, e.output
                                )
                            )
                
            upload_to_s3(bucket, s3_conn_id, logger, export_file, prep_dir)


def clear_files_and_subdirs(dir_to_clear):
    """
    Remove all files and subdirectories in a directory.
    :return: None
    """
    logger = logging_mixin.LoggingMixin().logger()
    logger.info(f"clearing all files in {dir_to_clear}")
    for root, dirs, files in os.walk(dir_to_clear):
        for file in files:
            file_to_remove = os.path.join(root, file)
            logger.debug(f"removing file {file_to_remove}")
            os.remove(file_to_remove)
    for root, dirs, files in os.walk(dir_to_clear):
        for dir in dirs:
            subdir_to_remove = os.path.join(root, dir)
            logger.debug(f"removing subdirectory {subdir_to_remove}")
            os.rmdir(subdir_to_remove)
    logger.info("tmp directory cleared")