import os
import pymssql
from dotenv import load_dotenv

load_dotenv()

server = os.getenv('DB_SERVER')
# pymssql requires host:port if port is non-standard, or just host.
# It does NOT use instance names (HOST\INSTANCE) easily on non-Windows sometimes,
# but let's try standard connection first.
# If server has 'localhost,1433' format, we might need to parse it.

database = os.getenv('DB_NAME')
username = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')

print(f"Tentando conectar com pymssql a: Server={server}, DB={database}, User={username}")

try:
    # pymssql.connect(server, user, password, database)
    # Note: server usually needs to be just hostname or IP. If it has instance, it might need escaping or port.
    
    conn = pymssql.connect(server=server, user=username, password=password, database=database)
    print("CONEXAO BEM SUCEDIDA COM PYMSSQL!")
    
    cursor = conn.cursor()
    cursor.execute('SELECT @@VERSION')
    row = cursor.fetchone()
    print(f"Versao do SQL Server: {row[0]}")
    
    print(f"Paramstyle: {pymssql.paramstyle}")
    
    conn.close()
except Exception as e:
    print(f"ERRO DE CONEXAO PYMSSQL: {str(e)}")
