import os
from dotenv import load_dotenv
import pymssql

load_dotenv()
server = os.getenv("DB_SERVER")
database = os.getenv("DB_NAME")
username = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")

try:
    conn = pymssql.connect(server=server, user=username, password=password, database=database, as_dict=True)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM TimeRecords ORDER BY timestamp DESC")
    rows = cursor.fetchall()
    print(f"Total records in SQL Server: {len(rows)}")
    for r in rows[:10]:
        print(r)
    
    cursor.execute("SELECT * FROM Users")
    users = cursor.fetchall()
    print("\nUsers in SQL Server:")
    for u in users:
        print(u)
        
    conn.close()
except Exception as e:
    print(f"Error: {e}")
