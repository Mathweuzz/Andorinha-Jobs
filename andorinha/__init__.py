"""
Andorinha Jobs — pacote principal.

Neste estágio inicial, expomos utilidades básicas:
- clock.now / clock.FakeClock
- logging.setup_logger
"""

from .clock import now, FakeClock
from .logging import setup_logger

__all__ = ["now", "FakeClock", "setup_logger"]