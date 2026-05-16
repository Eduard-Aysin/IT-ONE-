"""
Задание: Benchmark ClickHouse vs PostgreSQL
Генерация 10 млн строк логов, загрузка, замер запросов, мутации CH.
"""

import time
import random
import datetime
import clickhouse_connect
import psycopg2
import psycopg2.extras

# ─── Настройки подключения ───────────────────────────────────────────────────

CH_HOST     = "localhost"
CH_PORT     = 8123
CH_USER     = "admin"
CH_PASSWORD = "admin"
CH_DB       = "db"

PG_HOST     = "localhost"
PG_PORT     = 5432
PG_USER     = "IT1_user"
PG_PASSWORD = "password12345"
PG_DB       = "IT1_db"

ROWS        = 10_000_000
BATCH_SIZE  = 100_000

# ─── Генерация данных ─────────────────────────────────────────────────────────

def generate_batch(start_id: int, size: int):
    """Генерирует батч строк логов."""
    base_time = datetime.datetime(2026, 1, 1)
    actions   = ["login", "logout", "view", "click", "purchase", "search"]
    pages     = [f"/page/{i}" for i in range(1, 101)]

    rows = []
    for i in range(size):
        uid  = random.randint(1, 100_000)
        ts   = base_time + datetime.timedelta(seconds=random.randint(0, 15_552_000))  # ~6 мес
        act  = random.choice(actions)
        page = random.choice(pages)
        dur  = round(random.uniform(0.01, 3.0), 3)
        rows.append((uid, ts, act, page, dur))
    return rows

# ─── ClickHouse ───────────────────────────────────────────────────────────────

def setup_clickhouse():
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASSWORD,
        database=CH_DB
    )

    print("[CH] Создаём таблицу logs_ch...")
    client.command("DROP TABLE IF EXISTS logs_ch")
    client.command("""
        CREATE TABLE logs_ch (
            user_id    UInt32,
            timestamp  DateTime,
            action     String,
            page       String,
            duration   Float32
        )
        ENGINE = MergeTree()
        ORDER BY (user_id, timestamp)
        PARTITION BY toYYYYMM(timestamp)
    """)
    print("[CH] Таблица создана.")
    return client


def load_clickhouse(client):
    print(f"[CH] Загружаем {ROWS:,} строк батчами по {BATCH_SIZE:,}...")
    t0 = time.perf_counter()

    loaded = 0
    while loaded < ROWS:
        size  = min(BATCH_SIZE, ROWS - loaded)
        batch = generate_batch(loaded, size)

        client.insert(
            "logs_ch",
            batch,
            column_names=["user_id", "timestamp", "action", "page", "duration"]
        )
        loaded += size
        pct = loaded / ROWS * 100
        print(f"  {loaded:>10,} / {ROWS:,}  ({pct:.1f}%)", end="\r")

    elapsed = time.perf_counter() - t0
    print(f"\n[CH] Загрузка завершена за {elapsed:.1f} с  ({ROWS/elapsed:,.0f} строк/с)")


def benchmark_clickhouse(client):
    cutoff = datetime.datetime(2026, 4, 1)
    sql = f"""
        SELECT user_id, count(*) AS cnt
        FROM logs_ch
        WHERE timestamp > '{cutoff}'
        GROUP BY user_id
        ORDER BY cnt DESC
        LIMIT 10
    """
    print("\n[CH] Выполняем benchmark-запрос...")
    print(f"  SQL: SELECT user_id, count(*) FROM logs_ch WHERE timestamp > '{cutoff}' GROUP BY user_id")

    # Прогрев (первый запрос может быть медленнее из-за кеша)
    client.query(sql)

    # Замер
    t0     = time.perf_counter()
    result = client.query(sql)
    ch_ms  = (time.perf_counter() - t0) * 1000

    print(f"[CH] Время выполнения: {ch_ms:.1f} мс")
    print("[CH] Топ-10 пользователей:")
    for row in result.result_rows[:5]:
        print(f"  user_id={row[0]:>6}  count={row[1]:>8,}")

    return ch_ms

# ─── PostgreSQL ───────────────────────────────────────────────────────────────

def setup_postgres():
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASSWORD,
        dbname=PG_DB
    )
    conn.autocommit = True
    cur = conn.cursor()

    print("\n[PG] Создаём таблицу logs_pg...")
    cur.execute("DROP TABLE IF EXISTS logs_pg")
    cur.execute("""
        CREATE TABLE logs_pg (
            user_id    INTEGER,
            timestamp  TIMESTAMP,
            action     VARCHAR(20),
            page       VARCHAR(50),
            duration   REAL
        )
    """)
    print("[PG] Таблица создана.")
    return conn, cur


def load_postgres(conn, cur):
    print(f"[PG] Загружаем {ROWS:,} строк батчами по {BATCH_SIZE:,}...")
    t0 = time.perf_counter()

    loaded = 0
    while loaded < ROWS:
        size  = min(BATCH_SIZE, ROWS - loaded)
        batch = generate_batch(loaded, size)

        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO logs_pg (user_id, timestamp, action, page, duration) VALUES %s",
            batch,
            page_size=BATCH_SIZE
        )
        loaded += size
        pct = loaded / ROWS * 100
        print(f"  {loaded:>10,} / {ROWS:,}  ({pct:.1f}%)", end="\r")

    elapsed = time.perf_counter() - t0
    print(f"\n[PG] Загрузка завершена за {elapsed:.1f} с  ({ROWS/elapsed:,.0f} строк/с)")


def benchmark_postgres(cur):
    cutoff = "2026-04-01"
    sql = f"""
        SELECT user_id, count(*) AS cnt
        FROM logs_pg
        WHERE timestamp > '{cutoff}'
        GROUP BY user_id
        ORDER BY cnt DESC
        LIMIT 10
    """
    print("\n[PG] Выполняем benchmark-запрос...")
    print(f"  SQL: SELECT user_id, count(*) FROM logs_pg WHERE timestamp > '{cutoff}' GROUP BY user_id")

    # Прогрев
    cur.execute(sql)

    # Замер
    t0 = time.perf_counter()
    cur.execute(sql)
    rows  = cur.fetchall()
    pg_ms = (time.perf_counter() - t0) * 1000

    print(f"[PG] Время выполнения: {pg_ms:.1f} мс")
    print("[PG] Топ-10 пользователей:")
    for row in rows[:5]:
        print(f"  user_id={row[0]:>6}  count={row[1]:>8,}")

    return pg_ms

# ─── Мутации ClickHouse ───────────────────────────────────────────────────────

def mutations_demo(client):
    print("\n" + "="*60)
    print("ЗАДАЧА 2: Мутации в ClickHouse")
    print("="*60)

    # Сколько строк у user_id=1 до удаления
    before = client.query("SELECT count() FROM logs_ch WHERE user_id = 1").result_rows[0][0]
    print(f"\n[CH] Строк с user_id=1 до DELETE: {before:,}")

    print("\n[CH] Выполняем: DELETE FROM logs_ch WHERE user_id = 1")
    print("     (В ClickHouse это ALTER TABLE ... DELETE — асинхронная мутация)")
    client.command("ALTER TABLE logs_ch DELETE WHERE user_id = 1")

    # Смотрим system.mutations
    print("\n[CH] Статус из system.mutations:")
    mutations = client.query("""
        SELECT
            table,
            command,
            create_time,
            is_done,
            parts_to_do,
            latest_fail_reason
        FROM system.mutations
        WHERE table = 'logs_ch'
        ORDER BY create_time DESC
        LIMIT 5
    """)
    for row in mutations.result_rows:
        print(f"  table={row[0]}")
        print(f"  command={row[1]}")
        print(f"  create_time={row[2]}")
        print(f"  is_done={row[3]}")
        print(f"  parts_to_do={row[4]}")
        if row[5]:
            print(f"  fail_reason={row[5]}")
        print()

    print("""
[ВЫВОД] Почему DELETE нельзя делать часто в ClickHouse:
─────────────────────────────────────────────────────────
  1. DELETE — это не мгновенная операция, а мутация.
     ClickHouse перезаписывает целые части (parts) данных на диске.

  2. Каждый DELETE порождает фоновую задачу в system.mutations,
     которая читает и переписывает все затронутые куски — это
     дорогая IO-операция даже при удалении одной строки.

  3. Пока мутация выполняется, старые данные ещё видны в SELECT.
     Это eventual consistency, а не ACID.

  4. Частые мутации накапливают нагрузку на фоновые merge-процессы,
     ухудшают производительность всей базы.

  5. Альтернативы:
     - ReplacingMergeTree  — «мягкое» удаление через версии
     - CollapsingMergeTree — отмена строки через sign=-1
     - TTL-выражения       — автоматическое удаление по времени
     - TRUNCATE            — если нужно стереть всё целиком
""")

# ─── ИТОГОВЫЙ ОТЧЁТ ──────────────────────────────────────────────────────────

def print_report(ch_ms, pg_ms):
    print("\n" + "="*60)
    print("ИТОГОВЫЙ BENCHMARK: ClickHouse vs PostgreSQL")
    print("="*60)
    print(f"  ClickHouse:  {ch_ms:>8.1f} мс")
    print(f"  PostgreSQL:  {pg_ms:>8.1f} мс")
    ratio = pg_ms / ch_ms if ch_ms > 0 else 0
    print(f"  Разница:     ClickHouse быстрее в {ratio:.1f}x")
    print("""
Почему ClickHouse быстрее на аналитике:
  - Колоночное хранение: читаются только нужные колонки
  - Векторизованное выполнение: SIMD-инструкции CPU
  - Сжатие по колонкам: меньше IO
  - Партиционирование: пропускаются лишние куски
  - PostgreSQL — строчная СУБД, оптимизирована под OLTP (точечные запросы)
""")

# ─── MAIN ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("="*60)
    print("ЗАДАЧА 1: ClickHouse Benchmark")
    print("="*60)

    # ClickHouse
    ch_client     = setup_clickhouse()
    load_clickhouse(ch_client)
    ch_ms         = benchmark_clickhouse(ch_client)

    # PostgreSQL
    pg_conn, pg_cur = setup_postgres()
    load_postgres(pg_conn, pg_cur)
    pg_ms           = benchmark_postgres(pg_cur)

    # Итог
    print_report(ch_ms, pg_ms)

    # Мутации
    mutations_demo(ch_client)

    pg_conn.close()
