import time

import psycopg2

"""Функция для создания базы данных"""


def create_database(database_name: str, params: dict):

    # Подключаемся к БД

    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    # Создаем БД. Если БД с таким именем уже существует, то она удаляется
    # и записывается нужная нам БД

    try:
        cur.execute(f'DROP DATABASE {database_name}')
        time.sleep(3)
    except:
        pass
    finally:
        cur.execute(f'CREATE DATABASE {database_name}')

    cur.close()
    conn.close()


    # Формируем архитектруру БД - таблицу, колонки, типы данных

    with psycopg2.connect(dbname=database_name, **params) as conn:

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE vacancies (
                    company_name varchar NOT NULL,
                    vacancy_name varchar NOT NULL,
                    pay INT,
                    link TEXT
                )
            """)


    conn.close()
