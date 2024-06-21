import json
from typing import List, Dict, Any, Optional

import requests


class HH_Api:
    def __init__(self, url: str):
        self.url = url

    @staticmethod
    def format_vacancy(vacancy: Dict[str, Any]) -> Dict[str, Any]:
        """
                Форматирует информацию о вакансии.

                :param vacancy: Словарь с информацией о вакансии.
                :return: Отформатированный словарь с ключевыми данными о вакансии.
        """
        if not vacancy['salary']['from']:
            vacancy['salary']['from'] = vacancy['salary']['to']
        if not vacancy['salary']['to']:
            vacancy['salary']['to'] = vacancy['salary']['from']

        new_vacancy = {
            "name": vacancy.get("name", "-"),
            "salary": (vacancy['salary']['to'] + vacancy['salary']['from']) // 2,
            "url": vacancy.get("alternate_url", "-"),
            "company_name": vacancy.get("employer", {}).get("name", "-")
        }

        return new_vacancy

    @staticmethod
    def format_data(vacancies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
                Форматирует список вакансий и группирует их по компаниям.

                :param vacancies: Список словарей с информацией о вакансиях.
                :return: Список словарей, где каждая компания содержит список своих вакансий.
        """
        companies_dict = {}

        for vacancy in vacancies:
            salary = vacancy.get("salary")
            if salary and salary.get("currency") == 'RUR':
                formatted_vacancy = HH_Api.format_vacancy(vacancy)

                company_name = formatted_vacancy["company_name"]

                if company_name not in companies_dict:
                    companies_dict[company_name] = {
                        "company_name": company_name,
                        "vacancies": []
                    }
                companies_dict[company_name]["vacancies"].append(formatted_vacancy)

        return list(companies_dict.values())

    def get_companies_vacancies(self, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
                Получает список вакансий с API и форматирует их.

                :param params: Параметры запроса для API.
                :return: Список компаний с их вакансиями.
        """
        response = requests.get(self.url, params=params)
        response.raise_for_status()
        data = response.json()
        return HH_Api.format_data(data['items'])


