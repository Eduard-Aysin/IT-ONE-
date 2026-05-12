"""
DAG №2 — DQ: проверяет качество данных и отправляет результат в Telegram
"""
import time
import csv
import io
from datetime import datetime

import boto3
import psycopg2
import requests

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from tg_notification import TelegramNotification

S3_ENDPOINT = "http://localhost:9000"
S3_KEY      = "minioadmin"
S3_SECRET   = "minioadmin"
BUCKET      = "airflow-test"

FAILURE_TEMPLATE = """
❌ DQ — Ошибка в таске!

DAG: {DAG_NAME}
Task: {TASK_ID}
Дата: {DATE}
Лог: {TASK_LOG_URL}
Ответственные: {USERS}
"""

dq_notifier = TelegramNotification(
    message_template=FAILURE_TEMPLATE,
    responsible_users=["no_loging"],  # замени на свой TG username без @
    intervals=[1, 60, 600],
)


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET,
    )


def get_pg_conn():
    conn = BaseHook.get_connection("postgres_default")
    return psycopg2.connect(
        host=conn.host,
        port=conn.port,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
    )


def send_telegram_message(text: str):
    token   = Variable.get("TELEGRAM_TOKEN")
    chat_id = Variable.get("TELEGRAM_CHAT_ID")
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    for interval in [1, 60, 600]:
        try:
            requests.post(url, json={"chat_id": chat_id, "text": text})
            break
        except Exception:
            time.sleep(interval)


def run_dq_check(**context):
    conn = get_pg_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT pf.file_name
        FROM processed_files pf
        LEFT JOIN dq_checks dq ON pf.file_name = dq.file_name
        WHERE pf.status = 'processed'
          AND dq.file_name IS NULL
        ORDER BY pf.processed_at DESC
        LIMIT 1
    """)
    row = cur.fetchone()

    if not row:
        print("Нет файлов для DQ-проверки")
        cur.close()
        conn.close()
        return

    file_name = row[0]
    print(f"DQ-проверка для: {file_name}")

    # Считаем строки в файле
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=BUCKET, Key=file_name)
    content = obj["Body"].read().decode("utf-8")
    rows_in_file = len(list(csv.DictReader(io.StringIO(content))))

    # Считаем строки в БД
    cur.execute(
        "SELECT COUNT(*) FROM target_table WHERE file_name = %s",
        (file_name,)
    )
    rows_in_db = cur.fetchone()[0]
    is_valid = rows_in_file == rows_in_db

    cur.execute("""
        INSERT INTO dq_checks (file_name, rows_in_file, rows_in_db, is_valid)
        VALUES (%s, %s, %s, %s)
    """, (file_name, rows_in_file, rows_in_db, is_valid))

    conn.commit()
    cur.close()
    conn.close()

    print(f"Строк в файле: {rows_in_file}")
    print(f"Строк в БД:    {rows_in_db}")
    print(f"Результат:     {'OK' if is_valid else 'ОШИБКА'}")

    if is_valid:
        send_telegram_message(
            f"✅ DQ — Проверка пройдена\n\n"
            f"Файл: {file_name}\n"
            f"Строк в файле: {rows_in_file}\n"
            f"Строк в БД: {rows_in_db}\n"
            f"Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
    else:
        send_telegram_message(
            f"❌ DQ — Проверка провалена!\n\n"
            f"Файл: {file_name}\n"
            f"Строк в файле: {rows_in_file}\n"
            f"Строк в БД: {rows_in_db}\n"
            f"Расхождение: {abs(rows_in_file - rows_in_db)} строк\n"
            f"Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        raise ValueError(
            f"DQ провалена! В файле: {rows_in_file}, в БД: {rows_in_db}"
        )


with DAG(
    dag_id="dag_dq",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args={"on_failure_callback": dq_notifier.send},
    tags=["dq", "postgres"],
) as dag:

    dq_task = PythonOperator(
        task_id="run_dq_check",
        python_callable=run_dq_check,
    )