# Andorinha Jobs — Orquestrador de Tarefas (MVP, stdlib)

Orquestrador com fila de prioridade, leasing, heartbeats, retries com backoff,
cron scheduler, rate limit, métricas e painel admin HTTP — **apenas stdlib**.

## Requisitos
- Windows 11 + WSL2 (Ubuntu)
- Python 3.10+ (stdlib)
- SQLite3

## Setup rápido
```bash
sudo apt update && sudo apt install -y python3-venv python3-dev sqlite3 curl git
python3 -m venv .venv
source .venv/bin/activate
python -m unittest -v
