from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Optional, Dict

from .storage import get_conn, utc_now_str
from .clock import now as real_now


def _fmt_iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _iso_after(seconds: int, *, now_fn=real_now) -> str:
    t = now_fn()
    t2 = t + timedelta(seconds=seconds)
    return _fmt_iso(t2)


def _parse_iso_z(s: str) -> datetime:
    # Aceita "YYYY-MM-DDTHH:MM:SS.mmmZ"
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def enqueue(
    *,
    db_path: Optional[str] = None,
    queue: str = "default",
    priority: int = 0,
    payload: Optional[str] = None,
    max_attempts: int = 1,
    scheduled_at: Optional[str] = None,
    rate_group: Optional[str] = None,
    cron: Optional[str] = None,
    next_run_at: Optional[str] = None,
    now_fn=real_now,
) -> int:
    """
    Insere um job em status 'queued'. Retorna o id do job.
    Todos os timestamps são UTC (ISO-8601 com 'Z').
    """
    conn = get_conn(db_path)
    created = utc_now_str() if now_fn is real_now else _fmt_iso(now_fn())
    updated = created
    payload_str = payload if (payload is None or isinstance(payload, str)) else json.dumps(payload)
    conn.execute("BEGIN IMMEDIATE;")
    try:
        cur = conn.execute(
            """INSERT INTO jobs(status, priority, queue, payload, attempt, max_attempts,
                                scheduled_at, lease_expires_at, rate_group, cron, next_run_at,
                                created_at, updated_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?);""",
            (
                "queued",
                int(priority),
                str(queue),
                payload_str,
                0,
                int(max_attempts),
                scheduled_at,
                None,
                rate_group,
                cron,
                next_run_at,
                created,
                updated,
            ),
        )
        job_id = cur.lastrowid
        conn.commit()
        return int(job_id)
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise


def dequeue_with_lease(
    lease_ttl_sec: int,
    *,
    db_path: Optional[str] = None,
    queue: Optional[str] = None,
    now_fn=real_now,
) -> Optional[Dict[str, Any]]:
    """
    Seleciona 1 job disponível (status='queued' e agendado, ou 'leased' expirado)
    obedecendo a ordenação (priority ASC, created_at ASC), marca como 'leased'
    com novo lease (TTL) e retorna o registro como dict. Se não houver, retorna None.
    """
    conn = get_conn(db_path)
    now_str = utc_now_str() if now_fn is real_now else _fmt_iso(now_fn())
    lease_exp = _iso_after(lease_ttl_sec, now_fn=now_fn)

    conn.execute("BEGIN IMMEDIATE;")
    try:
        if queue is None:
            sel = conn.execute(
                """
                SELECT id FROM jobs
                WHERE
                  (
                    (status='queued' AND (scheduled_at IS NULL OR scheduled_at <= ?))
                    OR
                    (status='leased' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?)
                  )
                ORDER BY priority ASC, created_at ASC
                LIMIT 1;
                """,
                (now_str, now_str),
            ).fetchone()
        else:
            sel = conn.execute(
                """
                SELECT id FROM jobs
                WHERE
                  (
                    (status='queued' AND (scheduled_at IS NULL OR scheduled_at <= ?))
                    OR
                    (status='leased' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?)
                  )
                  AND queue = ?
                ORDER BY priority ASC, created_at ASC
                LIMIT 1;
                """,
                (now_str, now_str, queue),
            ).fetchone()

        if not sel:
            conn.commit()
            return None

        job_id = int(sel["id"])
        conn.execute(
            """
            UPDATE jobs
            SET status='leased',
                lease_expires_at=?,
                updated_at=?
            WHERE id=?;
            """,
            (lease_exp, now_str, job_id),
        )
        row = conn.execute("SELECT * FROM jobs WHERE id=?;", (job_id,)).fetchone()
        conn.commit()
        return dict(row)
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise


def extend_lease(
    job_id: int,
    add_ttl_sec: int,
    *,
    db_path: Optional[str] = None,
    now_fn=real_now,
) -> bool:
    """
    Estende o lease **somando** `add_ttl_sec` ao valor atual de lease_expires_at,
    desde que o job ainda esteja 'leased' e que o lease **não tenha expirado**.
    Retorna True se atualizado; False caso contrário.
    """
    conn = get_conn(db_path)
    now_str = utc_now_str() if now_fn is real_now else _fmt_iso(now_fn())

    conn.execute("BEGIN IMMEDIATE;")
    try:
        row = conn.execute(
            """
            SELECT lease_expires_at FROM jobs
            WHERE id = ?
              AND status='leased'
              AND lease_expires_at IS NOT NULL
              AND lease_expires_at > ?;
            """,
            (int(job_id), now_str),
        ).fetchone()

        if not row:
            conn.commit()
            return False

        old_exp = _parse_iso_z(row["lease_expires_at"])
        new_exp = old_exp + timedelta(seconds=add_ttl_sec)

        conn.execute(
            """
            UPDATE jobs
            SET lease_expires_at = ?,
                updated_at = ?
            WHERE id = ?;
            """,
            (_fmt_iso(new_exp), now_str, int(job_id)),
        )
        conn.commit()
        return True
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise


def release(
    job_id: int,
    *,
    success: bool,
    db_path: Optional[str] = None,
    now_fn=real_now,
) -> None:
    """
    Finaliza o lease do job:
      - success=True  -> status='succeeded', limpa lease, atualiza timestamp
      - success=False -> status='queued', attempt=attempt+1, limpa lease, volta para fila
    (Backoff será integrado no passo 4.)
    """
    conn = get_conn(db_path)
    now_str = utc_now_str() if now_fn is real_now else _fmt_iso(now_fn())

    conn.execute("BEGIN IMMEDIATE;")
    try:
        if success:
            conn.execute(
                """
                UPDATE jobs
                SET status='succeeded',
                    lease_expires_at=NULL,
                    updated_at=?
                WHERE id=?;
                """,
                (now_str, int(job_id)),
            )
        else:
            conn.execute(
                """
                UPDATE jobs
                SET status='queued',
                    attempt=attempt+1,
                    lease_expires_at=NULL,
                    updated_at=?,
                    -- reencaminha imediatamente (sem backoff por enquanto)
                    scheduled_at=COALESCE(scheduled_at, ?)
                WHERE id=?;
                """,
                (now_str, now_str, int(job_id)),
            )
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise