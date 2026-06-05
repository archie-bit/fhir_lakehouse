import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

USER= os.getenv('SNOWFLAKE_USERNAME')
PASSWORD= os.getenv('SNOWFLAKE_PASSWORD')
ACCOUNT= os.getenv('SNOWFLAKE_ACCOUNT')
WAREHOUSE= os.getenv('SNOWFLAKE_WAREHOUSE')
DATABASE= os.getenv('SNOWFLAKE_DATABASE')
SCHEMA= os.getenv('SNOWFLAKE_SCHEMA_BRONZE')

def initialize_snowflake():
    try:
        conn = snowflake.connector.connect(
            user=USER,
            password=PASSWORD,
            account=ACCOUNT,
            warehouse=WAREHOUSE,
            database=DATABASE,
            schema= SCHEMA
            )
        setup_queries = [
            "CREATE WAREHOUSE IF NOT EXISTS FHIR_LAKEHOUSE;",
            "USE WAREHOUSE FHIR_LAKEHOUSE;",
            
            "CREATE DATABASE IF NOT EXISTS FHIR_DATALAKE;",
            "USE DATABASE FHIR_DATALAKE;",
            
            "CREATE SCHEMA IF NOT EXISTS BRONZE;",
            "USE SCHEMA BRONZE;",
            
            "CREATE STAGE IF NOT EXISTS STAGETABLE;",
            
            """CREATE TABLE IF NOT EXISTS BRONZE.FHIR_RAW (
                RESOURCE_TYPE STRING,
                RAW_JSON      VARIANT,
                INGESTED_AT   TIMESTAMP_NTZ,
                FILENAME      STRING
            );"""
        ]
        
        with conn.cursor() as cur:
            for query in setup_queries:
                cur.execute(query)
                

    except Exception as e:
        print(e)
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    initialize_snowflake()