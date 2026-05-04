"""
DAG №1 — ETL: ждёт новый файл в MinIO, проверяет схему и загружает в PostgreSQL
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
from airflow.sensors.base import BaseSensorOperator

from tg_notification import TelegramNotification

S3_ENDPOINT = "http://localhost:9000"
S3_KEY      = "minioadmin"
S3_SECRET   = "minioadmin"
BUCKET      = "airflow-test"
PREFIX      = "input/"

EXPECTED_COLUMNS = {
    "order_id", "product_name", "quantity",
    "price", "customer_id", "order_date"
}

FAILURE_TEMPLATE = """
❌ ETL — Ошибка в таске!

DAG: {DAG_NAME}
Task: {TASK_ID}
Дата: {DATE}
Лог: {TASK_LOG_URL}
Ответственные: {USERS}
"""

etl_notifier = TelegramNotification(
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


def get_unprocessed_file():
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)

    if "Contents" not in response:
        return None

    all_files = [obj["Key"] for obj in response["Contents"]]

    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute("SELECT file_name FROM processed_files")
    processed = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()

    for file_key in all_files:
        if file_key not in processed:
            return file_key

    return None


def validate_schema(content: str):
    reader = csv.DictReader(io.StringIO(content))
    actual  = set(reader.fieldnames or [])
    missing = EXPECTED_COLUMNS - actual
    extra   = actual - EXPECTED_COLUMNS
    return len(missing) == 0 and len(extra) == 0, missing, extra


def mark_file(cur, file_name: str, status: str):
    cur.execute("""
        INSERT INTO processed_files (file_name, status)
        VALUES (%s, %s)
        ON CONFLICT (file_name) DO UPDATE SET status = EXCLUDED.status
    """, (file_name, status))


class NewS3FileSensor(BaseSensorOperator):
    def poke(self, context):
        file_key = get_unprocessed_file()
        if file_key:
            print(f"Найден новый файл: {file_key}")
            return True
        print("Новых файлов нет, ждём...")
        return False


def extract_and_load(**context):
    file_key = get_unprocessed_file()

    if not file_key:
        print("Нет файлов для обработки")
        return

    print(f"Обрабатываем: {file_key}")

    s3 = get_s3_client()
    obj = s3.get_object(Bucket=BUCKET, Key=file_key)
    content = obj["Body"].read().decode("utf-8")

    is_valid, missing, extra = validate_schema(content)

    conn = get_pg_conn()
    cur = conn.cursor()

    if not is_valid:
        print(f"❌ Неверная схема: {file_key}")
        mark_file(cur, file_key, "invalid_schema")
        conn.commit()
        cur.close()
        conn.close()

        send_telegram_message(
            f"❌ ETL — Неверная схема файла\n\n"
            f"Файл: {file_key}\n"
            f"Отсутствуют колонки: {missing or '—'}\n"
            f"Лишние колонки: {extra or '—'}\n"
            f"Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        return

    reader = csv.DictReader(io.StringIO(content))
    rows = list(reader)

    for row in rows:
        cur.execute("""
            INSERT INTO target_table
                (file_name, order_id, product_name, quantity, price, customer_id, order_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            file_key,
            int(row["order_id"]),
            row["product_name"],
            int(row["quantity"]),
            float(row["price"]),
            int(row["customer_id"]),
            row["order_date"],
        ))

    mark_file(cur, file_key, "processed")
    conn.commit()
    cur.close()
    conn.close()

    print(f"✅ Загружено {len(rows)} строк из {file_key}")

    send_telegram_message(
        f"✅ ETL — Файл успешно загружен\n\n"
        f"Файл: {file_key}\n"
        f"Строк загружено: {len(rows)}\n"
        f"Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    context["ti"].xcom_push(key="file_name", value=file_key)
    context["ti"].xcom_push(key="rows_count", value=len(rows))


with DAG(
    dag_id="dag_etl",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args={"on_failure_callback": etl_notifier.send},
    tags=["etl", "s3", "postgres"],
) as dag:

    wait_for_file = NewS3FileSensor(
        task_id="wait_for_new_file",
        poke_interval=30,
        timeout=3600,
        mode="poke",
    )

    etl_task = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_and_load,
    )

    wait_for_file >> etl_task