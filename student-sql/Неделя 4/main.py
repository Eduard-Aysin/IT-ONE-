import pandas as pd
import logging
from sqlalchemy import create_engine, text
from datetime import datetime
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataExtractor:
    def load_csv(self, file_path):
        try:
            logging.info(f"Чтение файла: {file_path}")
            df = pd.read_csv(file_path)

            return df
        except Exception as e:
            logging.error(f"Ошибка чтения файла {file_path}: {e}")

            return None


class DataTransformer:
    def __init__(self, df):
        self.df = df

    def sales_transformer(self, df):
        logging.info("Трансформация данных о продажах (sales.csv)")
        df['total_price'] = df['quantity'] * df['unit_price']
        logging.info(f"Расчитана общая стоимость заказа. \n{df.head()}")

        df['order_date'] = pd.to_datetime(df['order_date'])
        df['month'] = df['order_date'].dt.to_period('M').astype(str)
        logging.info(f"Добавлен месяц заказа \n{df.head()}")

        df = df.drop_duplicates(subset=['order_id'])
        df = df.dropna(subset=['order_id', 'quantity', 'unit_price'])
        logging.info("Удалены дубликаты заказов и строки с пустыми значениями")

        return df

    def customers_transformer(self, df):
        logging.info("Трансформация данных о клиентах (customers.csv)")
        email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        df['is_email_valid'] = df['email'].str.match(email_regex)
        logging.info("Удалены строки с невалидными email")

        df['registration_date'] = pd.to_datetime(df['registration_date'])
        df['customer_days'] = (datetime.now() - df['registration_date']).dt.days
        logging.info("Доавлен столбец с длительностью клиентских отношений")

        return df

    def get_aggregations(self, sales_df):
        summary = sales_df.groupby('category').agg(
            total_sales=('total_price', 'sum'),
            average_order_value=('total_price', 'mean')
        ).reset_index()

        ranking = sales_df.groupby('product_name').agg(
            total_sold=('quantity', 'sum')
        ).sort_values('total_sold', ascending=False).head(5).reset_index()
        ranking['rank_position'] = range(1, 6)

        return summary, ranking


class DataLoader:
    def __init__(self, db_config):
        self.engine = create_engine(f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")

    def prepare_tables(self, sql_file_path):
        try:
            logging.info("Подготовка и запись в БД")
            with self.engine.connect() as connection:
                connection.execute(text("DROP TABLE IF EXISTS product_ranking CASCADE;"))
                connection.execute(text("DROP TABLE IF EXISTS sales_summary CASCADE;"))
                connection.execute(text("DROP TABLE IF EXISTS sales CASCADE;"))
                connection.execute(text("DROP TABLE IF EXISTS customers CASCADE;"))
                connection.commit()
                logging.info("Удалены предыдущие таблицы (CASCADE).")

                if os.path.exists(sql_file_path):
                    with open(sql_file_path, 'r', encoding='utf-8') as file:
                        sql_script = file.read()
                        connection.execute(text(sql_script))
                        connection.commit()
                    logging.info("Схема БД успешно создана из файла db.sql.")
                else:
                    logging.error(f"Файл {sql_file_path} не найден!")
        except Exception as e:
            logging.error(f"Ошибка при подготовке таблиц: {e}")

    def load_to_db(self, df, table_name):
        try:
            logging.info(f"Загрузка {len(df)} строк в таблицу {table_name}...")
            df.to_sql(table_name, self.engine, if_exists='append', index=False)
            logging.info(f"Таблица {table_name} успешно обновлена.")
        except Exception as e:
            logging.error(f"Ошибка при загрузке в {table_name}: {e}")



if __name__ == "__main__":
    db_params = {
        "host": "localhost",
        "database": "IT1_db",
        "port": 5432,
        "user": "IT1_user",
        "password": "password12345"
    }
    logging.info("--- СТАРТ ETL ПРОЦЕССА ---")

    extractor = DataExtractor()
    transformer = DataTransformer(None)
    loader = DataLoader(db_params)

    loader.prepare_tables('db.sql')

    sales_raw = extractor.load_csv('sales.csv')
    customers_raw = extractor.load_csv('customers.csv')

    if sales_raw is not None and customers_raw is not None:
        sales_clean = transformer.sales_transformer(sales_raw)
        customers_clean = transformer.customers_transformer(customers_raw)
        summary_df, ranking_df = transformer.get_aggregations(sales_clean)

        if 'is_email_valid' in customers_clean.columns:
            customers_clean = customers_clean.drop(columns=['is_email_valid'])

        loader.load_to_db(customers_clean, 'customers')
        loader.load_to_db(sales_clean, 'sales')
        loader.load_to_db(summary_df, 'sales_summary')
        loader.load_to_db(ranking_df, 'product_ranking')

        logging.info("--- ETL ПРОЦЕСС ЗАВЕРШЕН УСПЕШНО ---")
    else:
        logging.error("Критическая ошибка: не удалось загрузить исходные данные.")



