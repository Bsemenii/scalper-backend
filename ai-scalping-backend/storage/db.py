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
            poolclass=None,  # SQLite doesn't use connection pooling
            connect_args={
                "check_same_thread": False,  # безопасно для uvicorn/async
                "timeout": 30.0,  # Increased timeout for operations (seconds)
            },
        )
        # Enable WAL mode for better concurrency and reduced locking
        with _engine.connect() as conn:
            conn.execute(text("PRAGMA journal_mode=WAL"))
            conn.execute(text("PRAGMA synchronous=NORMAL"))
            conn.execute(text("PRAGMA busy_timeout=10000"))  # Increased to 10 seconds
            conn.execute(text("PRAGMA wal_autocheckpoint=1000"))  # Auto-checkpoint WAL
            conn.commit()
    return _engine


def migrate_db() -> None:
    """
    Выполняет миграции БД: добавляет отсутствующие колонки и таблицы.
    Вызывается после init_db() для обновления существующих БД.
    """
    eng = get_engine()
    try:
        with eng.begin() as conn:
            # Проверяем наличие всех необходимых колонок в таблице trades
            try:
                # SQLite не поддерживает IF NOT EXISTS для ALTER TABLE ADD COLUMN,
                # поэтому проверяем через PRAGMA table_info
                result = conn.execute(
                    text("PRAGMA table_info(trades)")
                ).fetchall()
                
                if not result:
                    # Таблица не существует, миграция не нужна (будет создана схемой)
                    return
                
                columns = [row[1] for row in result]  # row[1] это имя колонки
                
                # Список колонок, которые должны быть добавлены при миграции
                migrations_needed = []
                
                if "pnl_r" not in columns:
                    migrations_needed.append(("pnl_r", "REAL NOT NULL DEFAULT 0"))
                
                if "fees_usd" not in columns:
                    migrations_needed.append(("fees_usd", "REAL NOT NULL DEFAULT 0"))
                
                # Выполняем миграции
                for col_name, col_def in migrations_needed:
                    print(f"[storage] Adding missing column: trades.{col_name}")
                    conn.execute(
                        text(f"ALTER TABLE trades ADD COLUMN {col_name} {col_def}")
                    )
                
                if migrations_needed:
                    print(f"[storage] Migration completed: added {len(migrations_needed)} column(s)")
                    
            except Exception as e:
                # Если таблица не существует или другая ошибка - это нормально при первом запуске
                # init_db создаст таблицу со всеми колонками из schema.sql
                print(f"[storage] Migration check failed (may be OK if table doesn't exist yet): {e}")
    except Exception as e:
        # Общая ошибка миграции - логируем, но не падаем
        print(f"[storage] Migration failed (non-critical): {e}")


def init_db() -> None:
    """
    Инициализирует БД: читает storage/schema.sql и прогоняет его
    внутри одной транзакции. Идемпотентно (CREATE TABLE IF NOT EXISTS).
    Затем выполняет миграции для обновления существующих БД.
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

    # Выполняем миграции для обновления существующих БД
    migrate_db()

    size = DB_PATH.stat().st_size if DB_PATH.exists() else 0
    print(f"[storage] SQLite initialized at {DB_PATH} ({size} bytes)")
