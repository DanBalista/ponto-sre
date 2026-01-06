import os
import subprocess
import sys
import time
import threading

def run_backend():
    print("Iniciando Backend na porta 5005...")
    env = os.environ.copy()
    env["PORT"] = "5005"
    subprocess.run([sys.executable, "app.py"], env=env)

if __name__ == "__main__":
    print("--- SISTEMA PONTO ELETRONICO SRE CARAPINA ---")
    print("Iniciando serviços...")
    
    # Start backend in a separate thread or just run it directly since it's the main blocker
    # We want it to be blocking so the script stays alive
    try:
        run_backend()
    except KeyboardInterrupt:
        print("\nParando sistema...")
