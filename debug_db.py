import sqlite3
import os
import time

print("Attempting to connect to local.db...")
try:
    conn = sqlite3.connect('local.db', timeout=5)
    print("Connected.")
    cursor = conn.cursor()
    print("Executing query...")
    cursor.execute("SELECT 1")
    row = cursor.fetchone()
    print(f"Result: {row}")
    conn.close()
    print("Closed.")
except Exception as e:
    print(f"Error: {e}")
