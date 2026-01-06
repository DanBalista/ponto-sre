import sqlite3
import bcrypt
import os

sqlite_path = 'local.db'
admin_mat = 'admin'
admin_pass = 'admin'

hashed = bcrypt.hashpw(admin_pass.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

conn = sqlite3.connect(sqlite_path)
cur = conn.cursor()
cur.execute("UPDATE Users SET password = ? WHERE matricula = ?", (hashed, admin_mat))
conn.commit()
print(f"Admin password reset to 'admin'. Rows affected: {cur.rowcount}")
conn.close()
