import psycopg2

from src.config import config


def fill_the_table(db_name: str, data_to_record: list):
    """
    Записывает данные в БД
    :param db_name: имя БД
    :param data_to_record: список кортежей с данными
    """

    # Парсим данные для подключения

    params = config()

    # Подключаемся к БД
    conn = psycopg2.connect(database=db_name, **params)
    try:
        with conn:
            with conn.cursor() as cur:
                # Записываем данные в таблицу
                cur.executemany(
                    f"INSERT INTO vacancies VALUES (%s, %s, %s, %s)", data_to_record
                )

    finally:
        conn.close()
