import duckdb
import glob
import os

con= duckdb.connect('data/healthcare.duckdb')
raw_path= "./data/raw/*.json"
allfiles=glob.glob(raw_path) 

con.execute("""
    CREATE TABLE IF NOT EXISTS ingested_metadata (filename TEXT, ingested_at TIMESTAMP)
""")

con.execute("""
    CREATE TABLE IF NOT EXISTS bronze_fhir (
                filename TEXT,
                raw_data JSON,
                ingested_at TIMESTAMP)
""")

already_ingested= [row[0] for row in con.execute("SELECT filename FROM ingested_metadata").fetchall()]


new_files= [f for f in allfiles if os.path.basename(f) not in already_ingested]


for file in new_files:
    filename= os.path.basename(file)
    con.execute("""
                INSERT INTO bronze_fhir
                SELECT 
                ? AS filename,
                json_extract(json, '$') AS raw_data,
                now() AS ingested_at
                FROM read_json_objects(
                ?,
                filename=true,
                maximum_object_size=50000000)
                """, [filename, file])
    
    con.execute("""
    INSERT INTO ingested_metadata VALUES (?, now())
    """, [filename])