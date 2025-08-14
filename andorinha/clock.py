from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

def now() -> datetime:
    """
    retorna o instante atual como dateime (COM FUSO) em UTC.
    usar sempre este relogio pra evitar dependencia de timezone local
    """
    return datetime.now(timezone.utc)

@dataclass
class FakeClock:
    """
    Relógio controlável para testes determinísticos.

    Exemplos:
        clk = FakeClock()  # inicia em 2000-01-01T00:00:00Z
        t0 = clk.now()
        clk.advance(seconds=30)
        t1 = clk.now()  # 30s depois
        clk.set(datetime(2025, 1, 1, tzinfo=timezone.utc))
    """
    _current: datetime = datetime(2000, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)

    def now(self) -> datetime:
        """retorna o instante atual do relogio falso"""
        return self._current
    
    def advance(
            self,
            *,
            seconds: int = 0,
            minutes: int = 0,
            hours: int = 0,
            days: int = 0,
    ) -> None:
        """avança o relogio pelo delta especificado"""
        delta = timedelta(
            days=days, hours=hours, minutes=minutes, seconds=seconds
        )
        self._current = self._current + delta

    def set(self, dt: datetime) -> None:
        """define o instante atual do relogio"""
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) != timedelta(0):
            raise ValueError("FakeClock.set: espera datetime aware em UTC")
        self._current = dt
