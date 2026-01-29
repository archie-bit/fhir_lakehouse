import snowflake.connector
from dotenv import load_dotenv
from pathlib import Path
import os
import shutil

load_dotenv()

USER= os.getenv('SNOWFLAKE_USERNAME')
PASSWORD= os.getenv('SNOWFLAKE_PASSWORD')
ACCOUNT= os.getenv('SNOWFLAKE_ACCOUNT')
WAREHOUSE= os.getenv('SNOWFLAKE_WAREHOUSE')
DATABASE= os.getenv('SNOWFLAKE_DATABASE')
SCHEMA= os.getenv('SNOWFLAKE_SCHEMA_BRONZE')


ingested_dir = Path(__file__).resolve().parent.parent / "data" / "ingested"
ingested_dir.mkdir(parents=True, exist_ok=True)
base_dir= Path(__file__).resolve().parent.parent / "data" / "bronze"
parquet_files = str(base_dir / "*.parquet").replace("\\", "/")

try:
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
    cur.execute("""COPY INTO FHIR_RAW FROM (
                SELECT
                    $1:RESOURCE_TYPE::STRING,
                    PARSE_JSON($1:RAW_JSON),
                    $1:INGESTED_AT::TIMESTAMP_NTZ,
                    $1:FILENAME::STRING
                FROM @stagetable
                ) FILE_FORMAT = (TYPE = PARQUET)""")

    for file_path in base_dir.glob("*.parquet"):
        shutil.move(str(file_path), str(ingested_dir / file_path.name))

except Exception as e:
    print(e)
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
finally:
    cur.close()
    conn.close()
