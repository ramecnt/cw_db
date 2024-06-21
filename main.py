import json

from api.hh_api import HH_Api
from db.create import create_tables
from db.fill_db import fill_data

if __name__ == "__main__":
    url = "https://api.hh.ru/vacancies"
    hh_api = HH_Api(url)
    companies = hh_api.get_companies_vacancies({'per_page': 50})

    with open('companies.json', 'w', encoding='utf-8') as f:
        json.dump(companies, f, ensure_ascii=False)

    create_tables()
    fill_data()
