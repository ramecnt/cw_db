from typing import List, Tuple
from db.connection import connect_db


class DBManager:
    """
    Класс для управления взаимодействием с базой данных PostgreSQL.

    Атрибуты:
        cursor: Объект курсора psycopg2 для выполнения запросов.
    """

    def __init__(self):
        """
        Инициализация соединения с базой данных и курсора.
        """
        self.cursor = connect_db().cursor()

    def get_companies_and_vacancies_count(self) -> List[Tuple[str, int]]:
        """
        Получить список всех компаний и количества вакансий у каждой компании.

        Возвращает:
            List[Tuple[str, int]]: Список кортежей, где каждый кортеж содержит
            название компании и количество вакансий.
        """
        query = """
        SELECT c.company_name, COUNT(v.id) AS vacancy_count
        FROM companies c
        LEFT JOIN vacancies v ON c.company_name = v.company_name
        GROUP BY c.company_name;
        """
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        return results

    def get_all_vacancies(self) -> List[Tuple[str, str, int, str]]:
        """
        Получить список всех вакансий с деталями, включая название компании, название вакансии, зарплату и ссылку.

        Возвращает:
            List[Tuple[str, str, int, str]]: Список кортежей, где каждый кортеж содержит
            название вакансии, название компании, зарплату и ссылку на вакансию.
        """
        query = """
        SELECT v.name, c.company_name, v.salary, v.url
        FROM vacancies v
        JOIN companies c ON v.company_name = c.company_name;
        """
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        return results

    def get_avg_salary(self) -> float:
        """
        Получить среднюю зарплату по всем вакансиям.

        Возвращает:
            float: Средняя зарплата по всем вакансиям.
        """
        query = """
        SELECT AVG(salary) FROM vacancies;
        """
        self.cursor.execute(query)
        avg_salary = self.cursor.fetchone()[0]
        return avg_salary

    def get_vacancies_with_higher_salary(self) -> List[Tuple[str, str, int, str]]:
        """
        Получить список вакансий с зарплатой выше средней по всем вакансиям.

        Возвращает:
            List[Tuple[str, str, int, str]]: Список кортежей, где каждый кортеж содержит
            название вакансии, название компании, зарплату и ссылку на вакансию.
        """
        avg_salary = self.get_avg_salary()
        query = """
        SELECT v.name, c.company_name, v.salary, v.url
        FROM vacancies v
        JOIN companies c ON v.company_name = c.company_name
        WHERE v.salary > %s;
        """
        self.cursor.execute(query, (avg_salary,))
        results = self.cursor.fetchall()
        return results

    def get_vacancies_with_keyword(self, keyword: str) -> List[Tuple[str, str, int, str]]:
        """
        Получить список вакансий, название которых содержит указанное ключевое слово.

        Аргументы:
            keyword (str): Ключевое слово для поиска в названии вакансии.

        Возвращает:
            List[Tuple[str, str, int, str]]: Список кортежей, где каждый кортеж содержит
            название вакансии, название компании, зарплату и ссылку на вакансию.
        """
        query = """
        SELECT v.name, c.company_name, v.salary, v.url
        FROM vacancies v
        JOIN companies c ON v.company_name = c.company_name
        WHERE v.name ILIKE %s;
        """
        self.cursor.execute(query, (f'%{keyword}%',))
        results = self.cursor.fetchall()
        return results
