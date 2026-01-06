# Instruções para Rodar Localmente

O ambiente foi configurado para rodar localmente.

## 1. Backend (API)
O backend foi configurado para rodar na porta 5005.
Para iniciar:
```bash
PORT=5005 .venv/bin/python app.py
```
Se você tiver o SQL Server rodando no Docker, configure as credenciais no arquivo `.env` para que o status fique "Online" (conectado ao banco).
Caso contrário, o sistema usará o banco de dados local `local.db` (SQLite) e funcionará normalmente, mas indicará status de conexão com banco como "Offline" (embora o sistema esteja funcional).

## 2. Frontend
Os arquivos do frontend estão na pasta `netlify/`.
Você pode abrir o arquivo `netlify/index.html` diretamente no navegador ou usar um servidor simples:
```bash
python3 -m http.server 8000 --directory netlify
```
Acesse: http://localhost:8000

## Solução dos Problemas Relatados

1. **Site Offline**: Foi corrigido adicionando `http://localhost:5005` nas configurações do frontend (`netlify/config.js`). Agora o frontend consegue encontrar o backend local.
2. **Cadastro na Admin**: Com a conexão restabelecida, novos cadastros feitos irão para o banco de dados do backend e aparecerão na área administrativa. Cadastros feitos anteriormente enquanto o sistema estava "offline" (apenas no navegador) não foram sincronizados e precisarão ser feitos novamente.
3. **Modo Online/Offline**: O sistema já possui suporte para operar offline (guardando pontos localmente) e sincronizar quando volta online.
