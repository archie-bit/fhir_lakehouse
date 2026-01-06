import duckdb
con = duckdb.connect('data/healthcare.duckdb')


print(con.sql("SELECT * FROM bronze_fhir"))
# print(con.sql("SHOW TABLES;"))
# con.execute("DROP TABLE bronze_fhir")