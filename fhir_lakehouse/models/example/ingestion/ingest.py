import glob
import pandas as pd
import duckdb


raw_path= "./data/raw/*.json"
con = duckdb.connect('data/healthcare.duckdb')
raw_files=glob.glob(raw_path) 

con.execute("""
    CREATE OR REPLACE TABLE bronze_fhir AS 
    SELECT 
            filename,
            json_extract(json, '$') AS raw_data,
            now() AS time_loaded
    FROM read_json_objects(
            './data/raw/*.json',
            filename=true,
            maximum_object_size=50000000
    )
""")