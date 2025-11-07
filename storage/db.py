from __future__ import annotations

import os
from pathlib import Path

from sqlalchemy import create_engine, text

# Путь к файлу БД
DB_PATH = Path("data/trading.db")

# Создаём engine SQLAlchemy (версия 2.x)
_engine = None

def get_engine():
    global _engine
    if _engine is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _engine = create_engine(f"sqlite:///{DB_PATH}", echo=False, future=True)
    return _engine

def init_db() -> None:
    """
    Инициализирует БД: создаёт файл, применяет schema.sql внутри одной транзакции.
    Вызывать один раз на старте FastAPI.
    """
    schema_file = Path("storage/schema.sql")
    if not schema_file.exists():
        raise FileNotFoundError("storage/schema.sql not found. Make sure you added the schema file.")

    sql = schema_file.read_text(encoding="utf-8")
    # Разбиваем по ';' аккуратно: простая схема — ОК (без триггеров с ';' внутри)
    statements = [s.strip() for s in sql.split(";") if s.strip()]

    eng = get_engine()
    with eng.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))

    # sanity-лог в stdout (не шумит)
    size = DB_PATH.stat().st_size if DB_PATH.exists() else 0
    print(f"[storage] SQLite initialized at {DB_PATH} ({size} bytes)")
