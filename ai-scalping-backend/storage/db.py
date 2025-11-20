from __future__ import annotations

from pathlib import Path
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Путь к файлу БД
DB_PATH = Path("data/trading.db")

# Глобальный engine (лениво инициализируется)
_engine: Optional[Engine] = None


def get_engine() -> Engine:
    """
    Возвращает singleton Engine для SQLite.
    Создаёт файл/директорию при первом вызове.
    """
    global _engine
    if _engine is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _engine = create_engine(
            f"sqlite:///{DB_PATH}",
            echo=False,    # можно включить True для SQL-дебага
            future=True,
            connect_args={"check_same_thread": False},  # безопасно для uvicorn/async
        )
    return _engine


def init_db() -> None:
    """
    Инициализирует БД: читает storage/schema.sql и прогоняет его
    внутри одной транзакции. Идемпотентно (CREATE TABLE IF NOT EXISTS).
    Вызывать один раз на старте FastAPI.
    """
    schema_file = Path("storage/schema.sql")
    if not schema_file.exists():
        raise FileNotFoundError(
            "storage/schema.sql not found. Make sure you added the schema file."
        )

    sql = schema_file.read_text(encoding="utf-8")

    # Простое разбиение по ';' — подходит для текущей схемы без триггеров.
    statements = [s.strip() for s in sql.split(";") if s.strip()]

    eng = get_engine()
    with eng.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))

    size = DB_PATH.stat().st_size if DB_PATH.exists() else 0
    print(f"[storage] SQLite initialized at {DB_PATH} ({size} bytes)")
