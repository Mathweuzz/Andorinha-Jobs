import io
import sys
import types
import unittest
from datetime import datetime, timezone, timedelta

import andorinha
from andorinha import clock
from andorinha import logging as and_logging


class TestClock(unittest.TestCase):
    def test_now_returns_utc_aware_datetime(self):
        t = clock.now()
        self.assertIsNotNone(t.tzinfo, "now() deve retornar datetime aware")
        self.assertEqual(t.utcoffset(), timedelta(0), "now() deve estar em UTC")

    def test_fake_clock_determinism_and_advance(self):
        start = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        clk = clock.FakeClock()
        clk.set(start)
        self.assertEqual(clk.now(), start)
        clk.advance(seconds=30)
        self.assertEqual(clk.now(), start + timedelta(seconds=30))
        clk.advance(minutes=1, hours=1, days=1)
        self.assertEqual(
            clk.now(),
            start + timedelta(seconds=30, minutes=1, hours=1, days=1),
        )

    def test_fake_clock_set_requires_utc_aware(self):
        clk = clock.FakeClock()
        with self.assertRaises(ValueError):
            clk.set(datetime(2024, 1, 1)) 
        with self.assertRaises(ValueError):
            from datetime import timezone as tz
            clk.set(datetime(2024, 1, 1, tzinfo=tz(timedelta(hours=-3))))


class TestLogging(unittest.TestCase):
    def test_setup_logger_writes_to_stdout_and_is_idempotent(self):
        original_stdout = sys.stdout
        buf = io.StringIO()
        sys.stdout = buf
        try:
            lg = and_logging.setup_logger("andorinha", level="INFO")
            lg.info("hello-world")
            output = buf.getvalue()
            self.assertIn("hello-world", output)
            self.assertIn("INFO", output)
            self.assertIn("andorinha", output)

            before = len(lg.handlers)
            and_logging.setup_logger("andorinha", level="DEBUG")
            after = len(lg.handlers)
            self.assertEqual(before, after, "setup_logger deve ser idempotente")
        finally:
            sys.stdout = original_stdout


if __name__ == "__main__":
    unittest.main(verbosity=2)
