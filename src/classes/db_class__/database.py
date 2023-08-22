from _decimal import Decimal

import psycopg2

from general_code.config import config


class DataBase:

    def __init__(self, db_name):
        self.db_name = db_name

    def __str__(self):
        return self.db_name

    def get_companies_and_vacancies_count(self):
        """
        Получает список всех компаний и количество вакансий у каждой компании
        """

        # Парсим данные для подключения к БД

        params = config()

        # Подключаемся к БД
        conn = psycopg2.connect(database=self.db_name, **params)

        # Реализуем SQL - cкрипт
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """SELECT company_name, COUNT (*) FROM vacancies
                                GROUP BY company_name
                                ORDER BY COUNT (*) DESC"""
                    )

                    # Выводим список все компаний и количество вакансий у каждой компании

                    rows = cur.fetchall()
                    for row in rows:
                        print(f'Название компании: {row[0]}\n'
                              f'Количество вакансий: {row[1]}\n ')


        finally:
            conn.close()

    """
    Далее все шаги по подключению к БД, вводу SQL-скрипта аналогично.
    Меняется только назначение функций и результат вывода в консоль
    """





    def get_all_vacancies(self):
        """
        Получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию.
        """
        params = config()
        conn = psycopg2.connect(database=self.db_name, **params)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """SELECT * FROM vacancies"""
                    )

                    rows = cur.fetchall()
                    for row in rows:
                        print(f'Название компании: {row[0]}\n'
                              f'Название вакансии: {row[1]}\n'
                              f'Заработная плата: {row[2]}\n'
                              f'Ссылка на вакансию: {row[3]}\n')


        finally:
            conn.close()

    def get_avg_salary(self):
        """
        Получает среднюю зарплату по вакансиям.

        """
        params = config()
        conn = psycopg2.connect(database=self.db_name, **params)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """SELECT AVG(pay) as avg_pay FROM vacancies"""
                    )

                    rows = cur.fetchall()
                    for row in rows:
                        decimal_value = Decimal(row[0])
                        int_value = int(decimal_value)
                        print(f'Средняя ЗП: {int_value} руб.')


        finally:
            conn.close()



    def get_vacancies_with_higher_salary(self):
        """
        Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.
        """
        params = config()
        conn = psycopg2.connect(database=self.db_name, **params)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """SELECT vacancy_name, pay FROM vacancies
                        WHERE pay > (SELECT AVG(pay) as avg_pay FROM vacancies)
                        ORDER BY pay"""
                    )

                    rows = cur.fetchall()
                    for row in rows:
                        print(f'Название вакансии: {row[0]}\n'
                              f'Заработная плата: {row[1]}\n')



        finally:
            conn.close()




    def get_vacancies_with_keyword(self, keyword: str):
        """
        Получает список всех вакансий, в названии которых есть ключевое слово
        :param keyword: ключевое слово
        :return:
        """

        params = config()
        conn = psycopg2.connect(database=self.db_name, **params)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""SELECT * FROM vacancies
                        WHERE vacancy_name LIKE '%{keyword}%'"""
                    )

                    rows = cur.fetchall()
                    for row in rows:
                        print(f'Название компании: {row[0]}\n'
                              f'Название вакансии: {row[1]}\n'
                              f'Заработная плата: {row[2]}\n'
                              f'Ссылка на вакансию: {row[3]}\n')



        finally:
            conn.close()



