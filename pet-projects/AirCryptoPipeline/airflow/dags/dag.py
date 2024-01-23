from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.sqlite_hook import SqliteHook
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator

from airflow.utils.dates import days_ago


import requests
import pandas as pd
import sqlite3




class FileSQLiteTransferHook(SqliteHook):

    def fetch_crypto_data(self, url, headers):

        response = requests.get(url, headers=headers)
        dictionary = response.json()

        self.data = pd.DataFrame(columns=['Last Updated', 'Name', 'Price', 'Rank'])

        # Создаем список для хранения данных
        buffer = []

        # Заполняем DataFrame данными из ответа API
        for k in dictionary["data"]:
            name, last_updated, price, rank = k["name"], k["last_updated"], k["quote"]["USD"]["price"], k["cmc_rank"]

            buffer.append({
                'Name': name,
                'Last Updated': last_updated,
                'Price': price,
                'Rank': rank
            })

        self.data = pd.concat([self.data, pd.DataFrame(buffer)], ignore_index=True)


    def insert_df_to_db(self, data):

      data.to_sql('project', con=sqlite3.connect("/project.db"), if_exists='append', index=False)


class FileSQLiteTransferOperator(BaseOperator):

    def __init__(self, url, headers, **kwargs):
        super().__init__(**kwargs)
        self.hook = None
        self.url = url # Путь до файла
        self.headers = headers


    def execute(self, context):

        # Создание объекта хука
        self.hook = FileSQLiteTransferHook()

        # Ваш код вызовите метод который
        # читает данные и затем записывает данные в БД

        self.hook.fetch_crypto_data(self.url, self.headers)
        self.hook.insert_df_to_db(self.hook.data)




dag = DAG(
    "crypto_data_fetch",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False
)

t1 = FileSQLiteTransferOperator(
  task_id='transfer_data',
  start_date=days_ago(1),
  url='https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest',
  headers = {
        'X-CMC_PRO_API_KEY': '9c8dfc88-e968-41ab-946f-8a50441a585b',
        'start': '1',  # Начальная позиция для списка криптовалют
        'limit': '100',  # Число криптовалют для запроса
        'convert': 'USD'  # Валюта для конвертации цен
    },
  dag=dag)


t1