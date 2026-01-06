import os
import sys
import time
import subprocess
import threading
import re
import json

AUTHTOKEN = "37rYktf7cs1PRM1XUZqJPzTjIIV_5VQh47diEHV6c8JNA6u1f"

def update_config_js(public_url):
    config_path = os.path.join("netlify", "config.js")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        if public_url.endswith('/'):
            public_url = public_url[:-1]

        # Regex to find and update the first URL in API_CANDIDATES
        # This is safer than just prepending if we want to keep it clean
        pattern = r'const API_CANDIDATES = \[\s*"[^"]*"'
        replacement = f'const API_CANDIDATES = [\n  "{public_url}"'
        
        if re.search(pattern, content):
            new_content = re.sub(pattern, replacement, content)
        else:
            # Fallback if pattern not found
            new_content = content.replace(
                'const API_CANDIDATES = [',
                f'const API_CANDIDATES = [\n  "{public_url}",'
            )
            
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(new_content)
        print(f"✅ netlify/config.js atualizado com Ngrok: {public_url}")
            
    except Exception as e:
        print(f"❌ Erro ao atualizar config.js: {e}")

def run_backend():
    print("🚀 Iniciando Backend Flask...")
    env = os.environ.copy()
    env["PORT"] = "5005"
    subprocess.call([sys.executable, "app.py"], env=env)

def start_ngrok():
    print("🌍 Configurando Ngrok...")
    
    # 1. Instalar/Configurar Token
    try:
        subprocess.run(["ngrok", "config", "add-authtoken", AUTHTOKEN], check=True, capture_output=True)
        print("✅ Authtoken configurado com sucesso.")
    except Exception as e:
        print(f"⚠️ Erro ao configurar token (pode já estar configurado): {e}")

    # 2. Iniciar Túnel
    print("⏳ Iniciando túnel Ngrok...")
    # Usando --log=stdout para capturar a URL mais facilmente ou via porta de inspeção
    ngrok_proc = subprocess.Popen(
        ["ngrok", "http", "5005", "--log=stdout"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    # Aguardar um pouco para o Ngrok subir e tentar pegar a URL via API local do Ngrok
    # (É mais confiável que ler o log)
    public_url = None
    for _ in range(10):
        time.sleep(2)
        try:
            import urllib.request
            with urllib.request.urlopen("http://127.0.0.1:4040/api/tunnels") as response:
                data = json.loads(response.read().decode())
                public_url = data['tunnels'][0]['public_url']
                if public_url:
                    break
        except Exception:
            continue

    if public_url:
        print(f"\n✨ TÚNEL NGROK ATIVO: {public_url} ✨")
        update_config_js(public_url)
        print("\n✅ PRÓXIMOS PASSOS:")
        print("1. O arquivo 'netlify/config.js' já foi atualizado.")
        print("2. Faça o upload da pasta 'netlify' para o seu site no Netlify.")
        print("3. Mantenha esta janela aberta para o sistema funcionar.")
        
        try:
            ngrok_proc.wait()
        except KeyboardInterrupt:
            ngrok_proc.terminate()
    else:
        print("❌ Não foi possível obter a URL do Ngrok. Verifique se o ngrok está instalado (brew install ngrok).")
        ngrok_proc.terminate()

if __name__ == "__main__":
    # Iniciar backend
    t = threading.Thread(target=run_backend, daemon=True)
    t.start()
    
    time.sleep(2)
    
    try:
        start_ngrok()
    except KeyboardInterrupt:
        print("\n👋 Encerrando...")
        sys.exit(0)
