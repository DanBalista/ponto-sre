import os
import sys
import time
import subprocess
import threading
from pyngrok import ngrok
import re

AUTHTOKEN = "37rYktf7cs1PRM1XUZqJPzTjIIV_5VQh47diEHV6c8JNA6u1f"

def update_config_js(public_url):
    config_path = os.path.join("netlify", "config.js")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        if public_url.endswith('/'):
            public_url = public_url[:-1]

        # Use regex to replace the first URL in the API_CANDIDATES array
        pattern = r'const API_CANDIDATES = \[\s*"[^"]*"'
        replacement = f'const API_CANDIDATES = [\n  "{public_url}"'
        
        if re.search(pattern, content):
            new_content = re.sub(pattern, replacement, content)
        else:
            new_content = content.replace(
                'const API_CANDIDATES = [',
                f'const API_CANDIDATES = [\n  "{public_url}",'
            )
            
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(new_content)
        print(f"✅ netlify/config.js atualizado com: {public_url}")
            
    except Exception as e:
        print(f"❌ Erro ao atualizar config.js: {e}")

def run_backend():
    print("🚀 Iniciando Backend Flask...")
    # Kill any existing process on 5005
    try:
        if sys.platform == "darwin":
            subprocess.run("lsof -ti:5005 | xargs kill -9", shell=True, stderr=subprocess.DEVNULL)
    except:
        pass
        
    env = os.environ.copy()
    env["PORT"] = "5005"
    subprocess.call([sys.executable, "app.py"], env=env)

def start_system():
    # 1. Configurar Ngrok
    print("🌍 Configurando Ngrok com o token fornecido...")
    ngrok.set_auth_token(AUTHTOKEN)

    # 2. Iniciar Backend em Segundo Plano
    backend_thread = threading.Thread(target=run_backend, daemon=True)
    backend_thread.start()
    time.sleep(3)

    # 3. Iniciar Túnel
    print("⏳ Abrindo túnel público...")
    try:
        public_url = ngrok.connect(5005).public_url
        print(f"\n✨ SISTEMA ONLINE: {public_url} ✨")
        
        # 4. Atualizar config.js
        update_config_js(public_url)
        
        print("\n✅ TUDO PRONTO!")
        print(f"1. O seu site no Netlify usará: {public_url}")
        print("2. Agora, basta fazer o upload da pasta 'netlify' uma última vez para o Netlify.")
        print("3. Esté endereço é FIXO enquanto este script estiver rodando.")
        print("\n⚠️ MANTENHA ESTA JANELA ABERTA!")
        
        # Keep the script running
        while True:
            time.sleep(10)
            
    except Exception as e:
        print(f"❌ Erro ao iniciar sistema: {e}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        start_system()
    except KeyboardInterrupt:
        print("\n👋 Encerrando sistema...")
        ngrok.kill()
        sys.exit(0)
