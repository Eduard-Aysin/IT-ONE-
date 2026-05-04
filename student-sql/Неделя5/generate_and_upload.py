# generate_and_upload.py
import boto3
import csv
import io
import random
from datetime import datetime, timedelta

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)

BUCKET = "airflow-test"
PRODUCTS = ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones", "Webcam"]

def generate_csv(date_str, num_rows=10):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["order_id", "product_name", "quantity", "price", "customer_id", "order_date"])
    for i in range(1, num_rows + 1):
        writer.writerow([
            i,
            random.choice(PRODUCTS),
            random.randint(1, 10),
            round(random.uniform(10.0, 1500.0), 2),
            random.randint(100, 200),
            date_str,
        ])
    return output.getvalue()

# Генерируем 5 файлов — по одному на каждый день
start_date = datetime(2026, 1, 1)
for i in range(5):
    date = start_date + timedelta(days=i)
    date_str = date.strftime("%Y_%m_%d")
    filename = f"sales_{date_str}.csv"
    content = generate_csv(date.strftime("%Y-%m-%d"))
    s3.put_object(Bucket=BUCKET, Key=f"input/{filename}", Body=content)
    print(f"✅ Загружен: input/{filename}")

print("Готово!")