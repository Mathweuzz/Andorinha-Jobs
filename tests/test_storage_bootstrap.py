import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone

from andorinha import clock
from andorinha import logging as and_logging
from andorinha import __all__ as pkg_export  # sanity of package exports
from andorinha.storage import (
    get_conn,
    migrate,
    close_thread_connections,
    utc_now_str,
)

class TestStorageBootstrap(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test.db")
        self.conn = get_conn(self.db_path)
        migrate(self.conn)

    def tearDown(self):
        close_thread_connections()
        self.tmp.cleanup()

    def test_pragmas_wal_and_foreign_keys(self):
        mode = self.conn.execute("PRAGMA journal_mode;").fetchone()[0]
        self.assertEqual(str(mode).lower(), "wal", "journal_mode deve ser WAL")
        fk = self.conn.execute("PRAGMA foreign_keys;").fetchone()[0]
        self.assertEqual(int(fk), 1, "foreign_keys deve estar ON")

    def test_tables_exist_and_columns_subset(self):
        # Tabelas esperadas
        names = {r["name"] for r in self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        )}
        for t in {"schema_migrations", "jobs", "runs", "workers", "rate_limits"}:
            self.assertIn(t, names, f"tabela {t} deve existir")

        # Colunas mínimas de jobs
        cols_jobs = [r["name"] for r in self.conn.execute("PRAGMA table_info('jobs');")]
        for c in ["id","status","priority","queue","created_at","updated_at"]:
            self.assertIn(c, cols_jobs)

        # Colunas mínimas de runs
        cols_runs = [r["name"] for r in self.conn.execute("PRAGMA table_info('runs');")]
        for c in ["id","job_id","started_at"]:
            self.assertIn(c, cols_runs)

    def test_insert_and_fk_and_check_constraint(self):
        # Inserir worker
        self.conn.execute(
            "INSERT INTO workers(name, last_heartbeat, pid, host) VALUES (?,?,?,?);",
            ("w1", utc_now_str(), 1234, "localhost"),
        )

        # Inserir job válido
        tnow = utc_now_str()
        self.conn.execute(
            """INSERT INTO jobs(status, priority, queue, payload, attempt, max_attempts,
                                 scheduled_at, lease_expires_at, rate_group, cron, next_run_at,
                                 created_at, updated_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?);""",
            ("queued", 5, "emails", "{}", 0, 3,
             tnow, None, "rg1", None, None,
             tnow, tnow)
        )
        job_id = self.conn.execute("SELECT id FROM jobs LIMIT 1;").fetchone()["id"]

        # Inserir run amarrado ao job (FK)
        self.conn.execute(
            """INSERT INTO runs(job_id, worker_id, started_at, finished_at, exit_code, error, log)
               VALUES(?,?,?,?,?,?,?);""",
            (job_id, 1, tnow, None, None, None, "started")
        )
        count_runs = self.conn.execute("SELECT COUNT(*) AS c FROM runs WHERE job_id=?;", (job_id,)).fetchone()["c"]
        self.assertEqual(count_runs, 1)

        # CHECK(status) deve impedir status inválido
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                """INSERT INTO jobs(status, priority, queue, created_at, updated_at)
                   VALUES(?,?,?,?,?);""",
                ("nope", 0, "default", tnow, tnow)
            )

    def test_migrate_is_idempotent_and_version_stable(self):
        # Rodar de novo não deve mudar nada
        v1 = self.conn.execute("SELECT MAX(version) FROM schema_migrations;").fetchone()[0]
        v2 = migrate(self.conn)
        v3 = self.conn.execute("SELECT MAX(version) FROM schema_migrations;").fetchone()[0]
        self.assertEqual(v1, 1)
        self.assertEqual(v2, 1)
        self.assertEqual(v3, 1)

if __name__ == "__main__":
    unittest.main(verbosity=2)