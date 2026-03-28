from sqlalchemy import create_engine
import urllib.parse
import psycopg2
import subprocess
import os
import pandas as pd
import numpy as np
import re
import csv
from dateutil.parser import parse as parse_date
from datetime import datetime
import tempfile
from pathlib import Path

from dashboard.code.core.paths import (
    CITY_PARCELS_FILE, COUNTY_PARCELS_FILE, CITY_PARCEL_DATA_FILE, CREDENTIALS_DIR)
from dashboard.code.backend.project_root.config.env import ENV
import json

CREDENTIALS_FILE = CREDENTIALS_DIR / f"{ENV}.json"

def get_db_credentials():
    with open(CREDENTIALS_FILE, 'r') as f:
        return json.load(f)

credentials = get_db_credentials()

# Direct usage without encoding
raw_user = credentials['DB_USER']
raw_password = credentials['DB_PASS']
raw_host = credentials['DB_HOST']
raw_port = credentials['DB_PORT']
raw_db_name = credentials['DB_NAME']

def connect_with_cursor():
    conn = psycopg2.connect(
        dbname=raw_db_name,
        user=raw_user,
        password=raw_password,
        host=raw_host,
        port=raw_port
    )
    return conn, conn.cursor()

def safe_pg_identifier(name):
    # Replace unsafe characters with underscores and lowercase
    return re.sub(r'\W|^(?=\d)', '_', name).lower()

def create_temp_table_sql(table_name, schema):
    table_name = table_name.lower()
    cols_sql = ",\n  ".join([f"{safe_pg_identifier(col)} {dtype}" for col, dtype in schema])
    return f"CREATE TEMP TABLE {table_name} (\n  {cols_sql}\n);"


def remove_commas_from_numeric_fields(df):
    for col in df.columns:
        if df[col].dtype == object:
            cleaned = (
                df[col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .replace({"": np.nan})
            )
            cleaned = pd.to_numeric(cleaned, errors="coerce")
            df[col] = cleaned
            print(f"Cleaned numeric-like column: {col}")
    return df


def infer_schema_from_csv(csv_path, sample_size=100, force_text_cols=None):
    # Note: this methodology looks at individual values throughout each column. An alternate method would create a series of 
    # unique values within each column, first. However, this would require more memory and compute power.
    if force_text_cols is None:
        force_text_cols = []

    def is_boolean(val):
        val = str(val).strip().lower()
        return val in {'true', 'false', 'yes', 'no'}
    
    def is_integer(val):
        try:
            val_clean = val.replace(',', '').strip()
            int(val_clean)
            return True
        except:
            return False
        
    def is_float(val):
        try:
            val_clean = val.replace(',', '').strip()
            float(val_clean)
            return True
        except:
            return False
    
    def is_date(val):
        try:
            parse_date(val, fuzzy=False)
            return True
        except:
            return False
        
    def is_timestamp(val):
        try:
            dt = parse_date(val, fuzzy=False)
            return dt.time() != datetime.min.time()
        except:
            return False

    # Read and clean the data    
    df = pd.read_csv(csv_path, dtype=str)
    df_cleaned = remove_commas_from_numeric_fields(df)

    # Save cleaned DataFrame to a temp file
    tmp_file = tempfile.NamedTemporaryFile(mode='w', newline='', suffix='.csv', delete=False)
    cleaned_csv_path = tmp_file.name
    tmp_file.close()  # Close the file before writing

    df_cleaned.to_csv(cleaned_csv_path, index=False)

    with open(cleaned_csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        samples = [row for _, row in zip(range(sample_size), reader)]

        if not samples:
            raise ValueError("CSV file is empty or header missing.")
        
        schema = []
        for field in reader.fieldnames:
            if not field.strip():
                continue  # skip blank field names

            normalized_field = field.strip().lower()
            normalized_force_text_cols = [col.strip().lower() for col in force_text_cols]

            if normalized_field in normalized_force_text_cols:
                schema.append((field, 'TEXT'))
                continue
            
            # Infer type based on sample values
            values = [row[field] for row in samples if row[field] != '']
            
            # Filter out null values
            non_null_values = [v for v in values if v not in (None, '', 'null', 'NULL')]

            if len(non_null_values) == 0:
                schema.append((field, 'TEXT'))
                continue

            if all(is_boolean(v) for v in non_null_values):
                schema.append((field, 'BOOLEAN'))
            elif all(is_float(v) for v in non_null_values):
                schema.append((field, 'DOUBLE PRECISION'))
            elif all(is_integer(v) for v in non_null_values):
                try:
                    if all(-2147483648 <= int(v) < 2147483647 for v in non_null_values):
                        schema.append((field, 'INTEGER'))
                    else:
                        schema.append((field, 'BIGINT'))
                except ValueError:
                    schema.append((field, 'TEXT'))
            elif all(is_integer(v) or is_float(v) for v in non_null_values):
                schema.append((field, 'DOUBLE PRECISION'))
            elif all(is_timestamp(v) for v in non_null_values):
                schema.append((field, 'TIMESTAMP'))
            elif all(is_date(v) for v in non_null_values):
                schema.append((field, 'DATE'))
            else:
                schema.append((field, 'TEXT'))

    return schema, cleaned_csv_path


def merge_table_with_csv(conn, cursor, merged_table, temp_table, csv_path, target_table, target_col, temp_col):
    # 1. Load CSV into a temp table
    csv_path = Path(csv_path)
    try:
        with csv_path.open('r') as f:
            cursor.copy_expert(f"""
            COPY {temp_table} FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',', QUOTE '"');
            """, f)
    except Exception as e:
        print(f"Failed to load CSV into temp table: {e}")
        conn.rollback()
        raise

    # 2. Get temp table columns excluding join key
    cursor.execute(f"""
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = %s AND column_name != %s;
    """, (temp_table, temp_col))
    temp_cols = [row[0] for row in cursor.fetchall()]

    temp_cols_sql = ', '.join([f's.{col}' for col in temp_cols])

    query = f"""
        DROP TABLE IF EXISTS {merged_table};
        CREATE TABLE {merged_table} AS
        SELECT t.*, {temp_cols_sql}
        FROM {target_table} t
        LEFT JOIN {temp_table} s
        ON t.{target_col} = s.{temp_col};
    """
    cursor.execute(query)

    # 3. # Drop the temp table
    cursor.execute(f"DROP TABLE {temp_table};")

    # 4. Drop the old target table and rename the merged table
    cursor.execute(f"DROP TABLE {target_table};")
    cursor.execute(f"ALTER TABLE {merged_table} RENAME TO {target_table};")

    conn.commit()


def encode_credentials(credentials):
    # Function to URL-encode credentials
    user = urllib.parse.quote(credentials['DB_USER'])
    password = urllib.parse.quote(credentials['DB_PASS'])
    host = credentials['DB_HOST']
    port = credentials['DB_PORT']
    database = credentials['DB_NAME']
    return user, password, host, port, database


def shapefile_to_DB(shapefile_path: Path):
    user, password, host, port, database = encode_credentials(credentials)
    table_name = shapefile_path.stem.lower()

    shp2pgsql = subprocess.Popen(
        ["shp2pgsql", "-I", "-s", "4326", "-d", shapefile_path, f"public.{table_name}"],
        stdout=subprocess.PIPE
    )

    env = os.environ.copy()
    env["PGPASSWORD"] = urllib.parse.unquote(password)  # decode for shell usage

    psql = subprocess.Popen(
        ["psql", "-U", urllib.parse.unquote(user), "-h", host, "-p", port, "-d", database],
        stdin=shp2pgsql.stdout,
        env=env
    )

    shp2pgsql.stdout.close() # Allow shp2pgsql to receive a SIGPIPE if psql exits
    psql.communicate()
    

def get_srid(cursor, table, column='geom'):
    query = f"SELECT Find_SRID('public', '{table}', '{column}');"
    cursor.execute(query)
    return cursor.fetchone()[0]


def crs_transform(conn, cursor, table, target_srid):
        cursor.execute(f"""
        ALTER TABLE {table}
        ALTER COLUMN geom TYPE geometry(MULTIPOLYGON, {target_srid})
        USING ST_Transform(geom, {target_srid});
        """)
        conn.commit()


def reproject_parcels(conn, cursor, geotable, srid):
    cursor.execute(f"""
        UPDATE {geotable}
        SET geom = ST_Transform(
            ST_SetSRID(geom, {srid}),
            4326
        );
    """)
    conn.commit()


def create_all_parcels(cursor):
    cursor.execute(f"""
        CREATE TABLE all_parcels AS
        SELECT 
            geom, parcel_id, prop_add, prop_zip, municipali, county,
            asstlandva, asstimpval, totassmt, propclass, zoning, yearblt
        FROM (
            SELECT 
                geom, parcel_id, prop_add, prop_zip, municipali, county,
                asstlandva, asstimpval, totassmt, propclass, zoning, yearblt
            FROM city_parcels

            UNION ALL

            SELECT 
                geom, parcel_id, prop_add, prop_zip, municipali, county,
                asstlandva, asstimpval, totassmt, propclass, zoning, yearblt
            FROM county_parcels
        ) AS combined;
    """)

    print("Standardized city and county parcels merged successfully into all_parcels.")


def deduplicate_parcels(cursor):
    # De-duplicate parcel_ids in all_parcels
    # Step 1: Create deduplicated table
    cursor.execute(f"""
    CREATE TABLE deduped_parcels AS
    SELECT
        parcel_id,
        ST_Union(geom) AS geom,
        STRING_AGG(DISTINCT prop_add, ', ') AS prop_add,
        MIN(prop_zip) AS prop_zip,
        MIN(municipali) AS municipali,
        MIN(county) AS county,
        SUM(asstlandva) AS asstlandva,
        SUM(asstimpval) AS asstimpval,
        SUM(totassmt) AS totassmt,
        STRING_AGG(DISTINCT propclass, ', ') AS propclass,
        STRING_AGG(DISTINCT zoning, ', ') AS zoning,
        MAX(yearblt) AS yearblt
               
    FROM all_parcels
    GROUP BY parcel_id;
    """)

    print("Duplicate parcels in all_parcels merged successfully.")

    # Step 2: Drop and rename
    cursor.execute("DROP TABLE all_parcels;")
    cursor.execute("ALTER TABLE deduped_parcels RENAME TO all_parcels;")

    # Step 3: Add primary key
    cursor.execute("ALTER TABLE all_parcels ADD PRIMARY KEY (parcel_id);")
    print("PRIMARY KEY added to all_parcels.")



def main():
## PART I.
    # Read file directories from config
    city_shapefile = CITY_PARCELS_FILE
    county_shapefile = COUNTY_PARCELS_FILE

    shapefile_list = [city_shapefile, county_shapefile]

    for sf in shapefile_list:
        shapefile_to_DB(sf)

    conn, cursor = connect_with_cursor()

    city_srid = "26796"
    county_srid = "102696"

    reproject_parcels(conn, cursor, geotable="city_parcels", srid=city_srid)
    print("City parcels reprojected from {city_srid} to 4326")

    reproject_parcels(conn, cursor, geotable="county_parcels", srid=county_srid)
    print("County parcels reprojected from {county_srid} to 4326")

    conn.commit()
    cursor.close()
    conn.close()


## PART II.
    # Merge City Attributes w/ City Parcel geometry
    city_csv_path = CITY_PARCEL_DATA_FILE

    # Connect to database using psycopg2
    conn, cursor = connect_with_cursor()

    schema, clean_csv_path = infer_schema_from_csv(
        city_csv_path, 
        force_text_cols=
        ['handle', 
         'asrparcelid', 
         'colparcelid', 
         'addrtype', 
         'parcelid', 
         'natregsite', 
         'zip', 
         'ownerzip',
         'cityblock',
         'giscityblock',
         ])
    create_sql = create_temp_table_sql('city_attr', schema)
    print(create_sql)
    cursor.execute(create_sql)

    try:
        merge_table_with_csv(
            conn, 
            cursor,
            merged_table='city_parcels_attr',
            temp_table='city_attr',
            csv_path=clean_csv_path,
            target_table='city_parcels',
            target_col='handle',
            temp_col='handle'
        )
    finally:
        if os.path.exists(clean_csv_path):
            os.remove(clean_csv_path)

    cursor.close()
    conn.close()

## PART III.
    # Merge city_parcels and county_parcels into all_parcels
    create_all_parcels(cursor)
    deduplicate_parcels(cursor)

if __name__ == "__main__":
    main()