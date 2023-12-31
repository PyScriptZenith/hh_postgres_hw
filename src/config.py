import os
from configparser import ConfigParser

from dotenv import load_dotenv

load_dotenv()
def config(
    filename=os.getenv("DB_CONNECTION_KEY", default="database.ini"),
    section="postgresql",
):
    """
    Парсит данные для подключения к БД
    :param filename: файл с данными
    """
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]

    else:
        raise Exception(
            "Section {0} is not found in the {1} file".format(section, filename)
        )
    return db
