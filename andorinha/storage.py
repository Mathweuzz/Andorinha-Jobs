from __future__ import annotations

import os
import sqlite3
import threading
from datetime import datetime, timezone

from .clock import now

# --- Conexões por thread ---
# Cada thread mantém um dict {db_path: sqlite3.Connection}
_thread_state = threading.local()

DEFAULT_DB = os.getenv("ANDORINHA_DB", os.path.join(os.getcwd(), "andorinha.db"))

def _ensure_thread_dict() -> dict:
    d = getattr(_thread_state, "conns", None)
    if d is None:
        d = {}
        _thread_state.conns = d
    return d

def utc_now_str() -> str:
    """Retorna timestamp UTC ISO-8601 com milissegundos e sufixo 'Z'."""
    t = now()
    # yyyy-mm-ddTHH:MM:SS.mmmZ
    return t.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

def _configure_connection(conn: sqlite3.Connection) -> None:
    conn.row_factory = sqlite3.Row
    # transações explícitas (BEGIN/COMMIT) quando necessário
    conn.isolation_level = None  # autocommit; usamos BEGIN IMMEDIATE manualmente
    # PRAGMAs por conexão
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    mode = conn.execute("PRAGMA journal_mode = WAL;").fetchone()[0]
    if str(mode).lower() != "wal":
        raise RuntimeError(f"Falha ao ativar WAL, journal_mode={mode!r}")

def get_conn(db_path: str | None = None) -> sqlite3.Connection:
    """
    Retorna a conexão SQLite para o 'db_path' no thread atual.
    Se não informado, usa DEFAULT_DB.
    """
    db_path = db_path or DEFAULT_DB
    conns = _ensure_thread_dict()
    conn = conns.get(db_path)
    if conn is None:
        conn = sqlite3.connect(db_path, check_same_thread=True)
        _configure_connection(conn)
        conns[db_path] = conn
    return conn

def close_thread_connections() -> None:
    """Fecha todas as conexões mantidas pelo thread atual."""
    conns = _ensure_thread_dict()
    for path, c in list(conns.items()):
        try:
            c.close()
        finally:
            conns.pop(path, None)

# --- Migração (schema v1) ---

SCHEMA_V1 = """
-- controle de versão
CREATE TABLE IF NOT EXISTS schema_migrations(
    version INTEGER PRIMARY KEY
);

-- tabela principal de jobs
CREATE TABLE IF NOT EXISTS jobs(
    id               INTEGER PRIMARY KEY,
    status           TEXT NOT NULL CHECK (status IN ('queued','leased','succeeded','failed','canceled')),
    priority         INTEGER NOT NULL DEFAULT 0,
    queue            TEXT NOT NULL DEFAULT 'default',
    payload          TEXT,
    attempt          INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL DEFAULT 1,
    scheduled_at     TEXT,
    lease_expires_at TEXT,
    rate_group       TEXT,
    cron             TEXT,
    next_run_at      TEXT,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);

-- histórico de execuções (runs)
CREATE TABLE IF NOT EXISTS runs(
    id          INTEGER PRIMARY KEY,
    job_id      INTEGER NOT NULL,
    worker_id   INTEGER,
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    exit_code   INTEGER,
    error       TEXT,
    log         TEXT,
    FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
);

-- workers conhecidos
CREATE TABLE IF NOT EXISTS workers(
    id             INTEGER PRIMARY KEY,
    name           TEXT NOT NULL,
    last_heartbeat TEXT,
    pid            INTEGER,
    host           TEXT
);

-- limites por grupo (token bucket)
CREATE TABLE IF NOT EXISTS rate_limits(
    rate_group        TEXT PRIMARY KEY,
    capacity          INTEGER NOT NULL,
    refill_every_sec  INTEGER NOT NULL,
    tokens            INTEGER NOT NULL,
    updated_at        TEXT NOT NULL
);

-- Índices úteis
CREATE INDEX IF NOT EXISTS idx_jobs_available
    ON jobs(status, scheduled_at, priority, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_queue
    ON jobs(queue);
CREATE INDEX IF NOT EXISTS idx_jobs_rate_group
    ON jobs(rate_group);
CREATE INDEX IF NOT EXISTS idx_runs_job_id
    ON runs(job_id);
"""

def _current_version(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_migrations';"
    ).fetchone()
    if not row:
        return 0
    row = conn.execute("SELECT MAX(version) AS v FROM schema_migrations;").fetchone()
    return int(row["v"]) if row and row["v"] is not None else 0

def migrate(conn: sqlite3.Connection) -> int:
    """
    Aplica migrações até a versão mais recente. Retorna versão aplicada.
    Idempotente: chamar múltiplas vezes não altera estado após atualizado.
    Usa BEGIN IMMEDIATE + commit/rollback do driver (conn.commit/rollback).
    """
    ver = _current_version(conn)
    if ver >= 1:
        return ver

    # Abrir transação explicitamente (em autocommit, precisamos do BEGIN)
    try:
        conn.execute("BEGIN IMMEDIATE;")
    except sqlite3.OperationalError:
        # fallback para BEGIN normal em FS mais chatos
        conn.execute("BEGIN;")

    try:
        conn.executescript(SCHEMA_V1)
        conn.execute("INSERT INTO schema_migrations(version) VALUES (1);")
        conn.commit()
        return 1
    except Exception:
        # Garanta que rollback não cause novo erro se a transação já caiu
        try:
            conn.rollback()
        except Exception:
            pass
        raise
