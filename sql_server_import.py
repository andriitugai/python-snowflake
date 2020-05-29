import pyodbc
import pandas as pd
import gzip
import shutil
import os
import time

# cnxn = pyodbc.connect("Driver={SQL Server};SERVER=hostname;Database=Practice;UID=XXX;PWD=XXX")

SQL_QUERY = "SELECT * from dbo.CustomersForSnowflake"
CSV_FILE_NAME = "../sql_server/customers.csv"
CHUNK_SIZE = 350000
TEMP_DIR = "../sql_server/temp_dir/"

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=EPUAKYIW04F0;'
                      'Database=Northwind;'
                      'Trusted_Connection=yes;')
with conn.cursor() as cursor:
    print("Started!")
    t0 = time.time()
    chunk_gen = pd.read_sql_query(SQL_QUERY, conn, chunksize=CHUNK_SIZE)
    ts = time.time()
    print(f"Connected in {ts-t0:.2f}s")
    for num, chunk in enumerate(chunk_gen, start=1):
        t00 = time.time()
        chunk_file_name = TEMP_DIR + CSV_FILE_NAME+f".{num:03}.csv"
        chunk.to_csv(chunk_file_name, index=False, header=True)
        with open(chunk_file_name, "rb") as f_in:
            with gzip.open(chunk_file_name + ".gz", "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        os.remove(chunk_file_name)
        t02 = time.time()
        print(f"Chunk {num:03} has been processed in {t02-t00:.2f}, total: {t02-t0:.2f}s.")

    t1 = time.time()

print(f"Total elapsed time: {t1 - t0:.2f}s.")


