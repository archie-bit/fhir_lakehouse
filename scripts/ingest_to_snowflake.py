import snowflake.connector
from dotenv import load_dotenv
from pathlib import Path
import os

load_dotenv()

USER= os.getenv('SNOWFLAKE_USERNAME')
PASSWORD= os.getenv('SNOWFLAKE_PASSWORD')
ACCOUNT= os.getenv('SNOWFLAKE_ACCOUNT')
WAREHOUSE= os.getenv('SNOWFLAKE_WAREHOUSE')
DATABASE= os.getenv('SNOWFLAKE_DATABASE')
SCHEMA= os.getenv('SNOWFLAKE_SCHEMA_BRONZE')


base_dir= Path(__file__).resolve().parent.parent / "data" / "bronze"
parquet_files = str(base_dir / "*.parquet").replace("\\", "/")
conn = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema= SCHEMA
    )

cur= conn.cursor()
cur.execute(f'PUT file://{parquet_files} @stagetable')
cur.execute("COPY INTO FHIR_RAW FROM @stagetable FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME = CASE_SENSITIVE")



# cur.execute(f'LIST @"stagetable"')
# for row in cur.fetchall():
#     print("test")
#     print(row)
# cur.execute("SHOW STAGES")
# stages = cur.fetchall()
# for s in stages:
#     print(f"Stage Name: {s[1]}, Schema: {s[3]}")
# cur.execute('SELECT * FROM FHIR_RAW')
# for row in cur.fetchall():
#     print(row)
cur.close()
conn.close()
