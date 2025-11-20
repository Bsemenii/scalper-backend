import json
import os
import sys
import time
from typing import Any, Dict, Optional

from bot.core.config import get_settings


class NDJsonLogger:
    def __init__(self, name: str):
        s = get_settings()
        self._dir = s.log_dir
        os.makedirs(self._dir, exist_ok=True)
        self._path = os.path.join(self._dir, f"{name}.ndjson")
        self._level = (s.log_level or "INFO").upper()

    def _write(self, rec: Dict[str, Any]):
        rec.setdefault("ts", int(time.time() * 1000))
        with open(self._path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    def info(self, msg: str, **kv: Any):
        if self._level in ("INFO", "DEBUG"):
            self._write({"lvl": "INFO", "msg": msg, **kv})

    def debug(self, msg: str, **kv: Any):
        if self._level == "DEBUG":
            self._write({"lvl": "DEBUG", "msg": msg, **kv})

    def warn(self, msg: str, **kv: Any):
        if self._level in ("INFO", "DEBUG", "WARN"):
            self._write({"lvl": "WARN", "msg": msg, **kv})

    def error(self, msg: str, **kv: Any):
        self._write({"lvl": "ERROR", "msg": msg, **kv})
        # дублируем в stderr
        sys.stderr.write(f"[ERROR] {msg} {kv}\n")


# удобные экземпляры
trace_log = NDJsonLogger("trace")    # сквозные трейсы сделок
snap_log = NDJsonLogger("snapshots") # снепы/фичи/кандидаты
