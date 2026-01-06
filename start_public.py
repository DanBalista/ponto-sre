import os
import sys
import time
import subprocess
import threading
import re

def update_config_js(public_url):
    config_path = os.path.join("netlify", "config.js")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        # Remove barra final se houver
        if public_url.endswith('/'):
            public_url = public_url[:-1]

        # Adiciona na lista API_CANDIDATES
        if public_url not in content:
            new_content = content.replace(
                'const API_CANDIDATES = [',
                f'const API_CANDIDATES = [\n  "{public_url}",'
            )
            with open(config_path, "w", encoding="utf-8") as f:
                f.write(new_content)
            print(f"✅ netlify/config.js atualizado com: {public_url}")
        else:
            print(f"ℹ️ URL já presente em netlify/config.js")
            
    except Exception as e:
        print(f"❌ Erro ao atualizar config.js: {e}")

def run_backend():
    print("🚀 Iniciando Backend Flask na porta 5005...")
    env = os.environ.copy()
    env["PORT"] = "5005"
    # Rodar app.py
    subprocess.call([sys.executable, "app.py"], env=env)

def start_tunnel():
    print("🌍 Iniciando Túnel Público (via Serveo/Localhost.run)...")
    
    # Tentativa 1: Localhost.run (Geralmente mais rápido)
    cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-R", "80:localhost:5005", "nokey@localhost.run"]
    
    while True:
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
            print("⏳ Aguardando URL pública...")
            url = None
            
            # Timeout para esperar a URL
            start_wait = time.time()
            while time.time() - start_wait < 15:
                line = process.stdout.readline()
                if not line: break
                print(f"   [Tunnel] {line.strip()}")
                match = re.search(r'https://[a-zA-Z0-9.-]+', line)
                if match:
                    url = match.group(0)
                    break
            
            if url:
                print(f"\n✨ TÚNEL ONLINE: {url} ✨")
                update_config_js(url)
                print("\n✅ PASSO FINAL:")
                print(f"1. A página 'netlify/config.js' foi atualizada com a URL: {url}")
                print("2. Você deve fazer o upload da pasta 'netlify' novamente para o seu site no Netlify.")
                print("3. O endereço https://pontoeletronico-srecp.netlify.app já está autorizado no servidor.\n")
                
                # Manter o processo vivo
                process.wait()
            else:
                print("⚠️ Falha ao obter URL. Tentando novamente em 5s...")
                process.terminate()
                time.sleep(5)
                
        except Exception as e:
            print(f"❌ Erro no túnel: {e}. Reiniciando em 5s...")
            time.sleep(5)

if __name__ == "__main__":
    # Iniciar backend em thread separada
    backend_thread = threading.Thread(target=run_backend)
    backend_thread.daemon = True
    backend_thread.start()
    
    # Dar um tempo para o backend subir
    time.sleep(3)
    
    # Iniciar túnel (esta função agora tem loop infinito)
    try:
        start_tunnel()
    except KeyboardInterrupt:
        print("\n👋 Encerrando sistema...")
        sys.exit(0)
