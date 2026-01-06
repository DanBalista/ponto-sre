import os
import sys
import subprocess
import re
import time

def update_config_js(public_url):
    config_path = os.path.join("netlify", "config.js")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        if public_url.endswith('/'):
            public_url = public_url[:-1]

        # Insert into API_CANDIDATES
        if public_url not in content:
            new_content = content.replace(
                'const API_CANDIDATES = [',
                f'const API_CANDIDATES = [\n  "{public_url}",'
            )
            with open(config_path, "w", encoding="utf-8") as f:
                f.write(new_content)
            print(f"✅ netlify/config.js atualizado com: {public_url}")
            sys.stdout.flush()
        else:
            print(f"ℹ️ URL já presente em netlify/config.js")
            
    except Exception as e:
        print(f"❌ Erro ao atualizar config.js: {e}")

def start_tunnel():
    print("🌍 Iniciando Túnel Público (via Serveo) para porta 5001...")
    sys.stdout.flush()
    
    # Tunnel localhost:5001 (Docker) to public internet
    cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-R", "80:localhost:5001", "serveo.net"]
    
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )
    
    print("⏳ Aguardando URL pública...")
    sys.stdout.flush()
    url = None
    
    while True:
        line = process.stdout.readline()
        if not line:
            break
        # print(f"   [Serveo] {line.strip()}")
        if "Forwarding HTTP traffic from" in line:
            match = re.search(r'https://[a-zA-Z0-9.-]+', line)
            if match:
                url = match.group(0)
                break
    
    if url:
        print(f"\n✨ TÚNEL ONLINE: {url} ✨\n")
        update_config_js(url)
        print("Mantenha este processo rodando...")
        sys.stdout.flush()
        
        # Keep alive
        for line in process.stdout:
            pass
    else:
        print("❌ Falha ao obter URL.")

if __name__ == "__main__":
    start_tunnel()
