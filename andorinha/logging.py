from __future__ import annotations

import os
import sys
import logging as _logging
from typing import Optional

def _parse_level(level: Optional[str]) -> int:
    if isinstance(level, int):
        return level
    if level is None:
        level = os.getenv("ANDORINHA_LOG_LEVEL", "INFO")
    try:
        return getattr(_logging, str(level).upper())
    except AttributeError:
        return _logging.INFO
    
def setup_logger(name: str = "andorinha", level: Optional[str | int] = None) -> _logging.Logger:
    """
    configura e retorna um logger com ScreamHandler (stdout) e formato ISO-8601 (Z).
    Idempotente: evita criar multiplos handlers iguais no mesmo logger.
    """
    lg = _logging.getLogger(name)
    lg.setLevel(_parse_level(level))

    # evitar duplicidade de handlers
    needs_handler = True
    for h in lg.handlers:
        if isinstance(h, _logging.StreamHandler) and getattr(h, "stream", None) is sys.stdout:
            needs_handler = False
            break

    if needs_handler:
        handler = _logging.StreamHandler(stream=sys.stdout)
        # Ex.: 2025-08-14T12:34:56.123Z [INFO] andorinha: mensagem
        formatter = _logging.Formatter(
            fmt="%(asctime)s.%(msecs)03dZ [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
        handler.setFormatter(formatter)
        lg.addHandler(handler)

    # evita propagration
    lg.propagate = False
    return lg
