import json
import time

from src.config import config
from src.create_db import create_database
from src.fill_table import fill_the_table
from src.classes.JSON_saver import JSON_Saver
from src.classes.hh import HH
from src.utils import load_data_hh_obj



# Список компаний, вакансии которых будем парсить с hh.ru

COMPANIES_VC = [
    "Лукойл",
    "Роснефть",
    "Сбербанк",
    "Новатэк",
    "Сургутнефтегаз",
    "Северсталь",
    "Норникель",
    "Транснефть",
    "Газпром",
    "Росатом",
]

# Список имен, которые мы будем присваивать БД

COMPANIES_DB = [
    "lukoil",
    "rosneft",
    "sberbank",
    "novatek",
    "surgutneftegas",
    "severstal",
    "nornickel",
    "transneft",
    "gazprom",
    "rosatom",
]


def main():
    """
    Парсим вакансии и записываем их в БД
    """

    for x in range(len(COMPANIES_VC)):
        print('#######################################################')
        print()
        print(f"Началась загрузка вакансий компании {COMPANIES_VC[x]}")
        print()
        print('#######################################################')

        time.sleep(1)
        hh = HH()

        # Парсим с hh.ru
        parced_vacancies = hh.get_vacancies(COMPANIES_VC[x], 113)

        # Сохраняем вакансии в экземпляры класса Vacancy
        data_to_record = load_data_hh_obj(parced_vacancies)

        # Извлекаем данные из объектов класса Vacancy и записываем в json
        js_obj = JSON_Saver(f"Вакансии {COMPANIES_VC[x]}")
        js_obj.save_to_JSON(data_to_record)

        # Получаем параметры для подключения к БД
        params = config()

        # Создаем БД
        create_database(COMPANIES_DB[x], params)

        # Читаем вакансии из json и подготавливаем данные для записи в БД

        with open(
            f"../parced_vacancies/Вакансии {COMPANIES_VC[x]} в РФ.json",
            encoding="utf-8",
        ) as file:
            content = json.load(file)

        data_to_record = []
        for data in content:
            vac_name = data["name"]
            url = data["url"]
            pay = data["pay"]
            employer = data["employer"]
            data_to_record.append((employer, vac_name, pay, url))

        # Записываем данные в БД

        fill_the_table(COMPANIES_DB[x], data_to_record)


if __name__ == "__main__":
    main()
