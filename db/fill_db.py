import json
from db.connection import connect_db
from psycopg2 import errors


def connection(func):
    """
    Декоратор для управления соединением с базой данных и транзакциями.

    Args:
        func: Функция, которую нужно выполнить в рамках транзакции.

    Returns:
        wrapper: Обёртка для выполнения функции с управлением соединением и транзакциями.
    """
    def wrapper(*args, **kwargs):
        connection = connect_db()
        cursor = connection.cursor()
        try:
            func(*args, **kwargs, cursor=cursor)
            connection.commit()
        except errors.UniqueViolation:
            connection.rollback()
        finally:
            cursor.close()
            connection.close()

    return wrapper


@connection
def insert_company(name, cursor):
    """
    Вставка новой компании в таблицу companies.

    Args:
        name (str): Название компании.
        cursor: Курсор для выполнения SQL-запроса.
    """
    cursor.execute("""
        INSERT INTO companies (company_name)
        VALUES (%s);
    """, (name,))


@connection
def insert_vacancy(name, salary, url, company_name, cursor):
    """
    Вставка новой вакансии в таблицу vacancies.

    Args:
        name (str): Название вакансии.
        salary (int): Зарплата.
        url (str): URL вакансии.
        company_name (str): Название компании, к которой относится вакансия.
        cursor: Курсор для выполнения SQL-запроса.
    """
    cursor.execute("""
        INSERT INTO vacancies (name, salary, url, company_name)
        VALUES (%s, %s, %s, %s) RETURNING id;
    """, (name, salary, url, company_name))


def fill_data():
    """
    Заполнение базы данных данными из JSON-файла.

    Открывает файл companies.json и последовательно добавляет компании и их вакансии
    в базу данных, используя функции insert_company и insert_vacancy.
    """
    with open("../companies.json", "r", encoding="utf-8") as f:
        data = json.load(f)
        for company in data:
            insert_company(company['company_name'])
            for vacancy in company['vacancies']:
                insert_vacancy(vacancy["name"], vacancy["salary"], vacancy["url"],
                               vacancy["company_name"])

