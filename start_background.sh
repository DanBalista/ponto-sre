#!/bin/bash
# Script para iniciar o sistema em background (sem terminal aberto)
# Salva logs em system.log

cd "$(dirname "$0")"

# Verifica se já está rodando e mata
pkill -f "app.py"
pkill -f "run_system.py"

echo "Iniciando Ponto Eletrônico em background..."
nohup .venv/bin/python run_system.py > system.log 2>&1 &

echo "Sistema iniciado! Pode fechar este terminal."
echo "Para verificar logs: tail -f system.log"
