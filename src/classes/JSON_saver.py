import json
from abc import ABC, abstractmethod


class File_Saver(ABC):
    @abstractmethod
    def __init__(self, filename):
        pass

class JSON_Saver(File_Saver):

    def __init__(self, filename: str):
        self.filename = filename


    def save_to_JSON(self, data: list):
        """
        Сохраняет вакансии в json
        :param data: список с вакансиями
        """
        with open(f'../parced_vacancies/{self.filename} в РФ.json', 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=4, ensure_ascii=False)




