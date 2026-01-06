import os
import pyodbc
from dotenv import load_dotenv

load_dotenv()

server = os.getenv('DB_SERVER')
database = os.getenv('DB_NAME')
username = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')

print(f"Tentando conectar a: Server={server}, DB={database}, User={username}")

try:
    drivers = [d for d in pyodbc.drivers() if 'SQL Server' in d]
    print(f"Drivers encontrados: {drivers}")
    
    driver = drivers[0] if drivers else 'ODBC Driver 17 for SQL Server'
    conn_str = f'DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;Connection Timeout=5'
    
    print(f"String de conexao: {conn_str}")
    
    conn = pyodbc.connect(conn_str)
    print("CONEXAO BEM SUCEDIDA!")
    conn.close()
except Exception as e:
    print(f"ERRO DE CONEXAO: {str(e)}")
