import threading
import time
import os
import tempfile
import unittest
from datetime import datetime, timezone, timedelta

from andorinha.clock import FakeClock
from andorinha.storage import get_conn, migrate, close_thread_connections
from andorinha.queue import (
    enqueue,
    dequeue_with_lease,
    extend_lease,
    release,
)

def parse_iso_z(s: str) -> datetime:
    # Converte "YYYY-MM-DDTHH:MM:SS.mmmZ" para datetime aware UTC
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

class TestQueueLeasing(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "q.db")
        self.conn = get_conn(self.db_path)
        migrate(self.conn)
        # Relógio determinístico padrão
        self.clock = FakeClock()
        # Base = 2000-01-01T00:00Z

    def tearDown(self):
        close_thread_connections()
        self.tmp.cleanup()

    def test_priority_and_fifo(self):
        # A: prio 5, B: prio 1, C: prio 1 (B antes de C por created_at)
        enqueue(db_path=self.db_path, queue="emails", priority=5, payload="{}", now_fn=self.clock.now)
        self.clock.advance(seconds=1)
        idB = enqueue(db_path=self.db_path, queue="emails", priority=1, payload="{}", now_fn=self.clock.now)
        self.clock.advance(seconds=1)
        idC = enqueue(db_path=self.db_path, queue="emails", priority=1, payload="{}", now_fn=self.clock.now)

        # Dequeue deve pegar B, depois C
        j1 = dequeue_with_lease(60, db_path=self.db_path, queue="emails", now_fn=self.clock.now)
        self.assertIsNotNone(j1)
        self.assertEqual(j1["id"], idB)

        j2 = dequeue_with_lease(60, db_path=self.db_path, queue="emails", now_fn=self.clock.now)
        self.assertIsNotNone(j2)
        self.assertEqual(j2["id"], idC)

    def test_scheduled_at_respected(self):
        # Job agendado para +10 minutos
        future = (self.clock.now() + timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        enqueue(db_path=self.db_path, queue="default", priority=0, payload="{}", scheduled_at=future, now_fn=self.clock.now)

        # Antes do horário -> nada
        j = dequeue_with_lease(60, db_path=self.db_path, now_fn=self.clock.now)
        self.assertIsNone(j)

        # Avança relógio para além do agendamento
        self.clock.advance(minutes=10, seconds=1)
        j2 = dequeue_with_lease(60, db_path=self.db_path, now_fn=self.clock.now)
        self.assertIsNotNone(j2)

    def test_competing_dequeues_single_winner(self):
        # Um único job; dois threads concorrendo no dequeue
        jid = enqueue(db_path=self.db_path, queue="default", priority=0, payload="{}", now_fn=self.clock.now)

        results = []

        def worker():
            # Cada thread terá sua própria conexão (get_conn usa thread-local)
            res = dequeue_with_lease(60, db_path=self.db_path, now_fn=self.clock.now)
            results.append(res["id"] if res else None)

        t1 = threading.Thread(target=worker)
        t2 = threading.Thread(target=worker)
        t1.start(); t2.start()
        t1.join(); t2.join()

        got = [r for r in results if r is not None]
        self.assertEqual(len(got), 1, f"apenas um thread deve obter o job, got={results}")
        self.assertEqual(got[0], jid)

    def test_lease_expiration_makes_job_available_again(self):
        jid = enqueue(db_path=self.db_path, payload="{}", now_fn=self.clock.now)
        j1 = dequeue_with_lease(60, db_path=self.db_path, now_fn=self.clock.now)
        self.assertIsNotNone(j1)
        self.assertEqual(j1["id"], jid)

        # Avança além do TTL => lease expira
        self.clock.advance(seconds=61)
        j2 = dequeue_with_lease(60, db_path=self.db_path, now_fn=self.clock.now)
        self.assertIsNotNone(j2)
        self.assertEqual(j2["id"], jid)

    def test_extend_lease(self):
        jid = enqueue(db_path=self.db_path, payload="{}", now_fn=self.clock.now)
        j = dequeue_with_lease(60, db_path=self.db_path, now_fn=self.clock.now)
        old_exp = parse_iso_z(j["lease_expires_at"])
        # Avança 10s, estende +120s; nova expiração ~ old_exp + 120s (menos os 10s já passados)
        self.clock.advance(seconds=10)
        ok = extend_lease(jid, 120, db_path=self.db_path, now_fn=self.clock.now)
        self.assertTrue(ok)

        row = self.conn.execute("SELECT lease_expires_at FROM jobs WHERE id=?;", (jid,)).fetchone()
        new_exp = parse_iso_z(row["lease_expires_at"])
        self.assertGreater(new_exp, old_exp)
        # Deve ser aproximadamente old_exp + 120s
        self.assertEqual(int((new_exp - old_exp).total_seconds()), 120)

    def test_release_success_and_fail(self):
        jid = enqueue(db_path=self.db_path, payload="{}", now_fn=self.clock.now)
        j = dequeue_with_lease(30, db_path=self.db_path, now_fn=self.clock.now)
        self.assertIsNotNone(j)

        # sucesso
        release(jid, success=True, db_path=self.db_path, now_fn=self.clock.now)
        st = self.conn.execute("SELECT status, attempt FROM jobs WHERE id=?;", (jid,)).fetchone()
        self.assertEqual(st["status"], "succeeded")
        self.assertEqual(st["attempt"], 0)

        # Re-enfileira e falha
        jid2 = enqueue(db_path=self.db_path, payload="{}", now_fn=self.clock.now)
        j2 = dequeue_with_lease(30, db_path=self.db_path, now_fn=self.clock.now)
        self.assertIsNotNone(j2)
        release(jid2, success=False, db_path=self.db_path, now_fn=self.clock.now)
        st2 = self.conn.execute("SELECT status, attempt FROM jobs WHERE id=?;", (jid2,)).fetchone()
        self.assertEqual(st2["status"], "queued")
        self.assertEqual(st2["attempt"], 1)

if __name__ == "__main__":
    unittest.main(verbosity=2)
