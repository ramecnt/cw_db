from db.connection import connect_db

def create_tables():
    """
    Создание таблиц companies и vacancies в базе данных PostgreSQL.

    Создаёт таблицы companies и vacancies, если они не существуют, с заданными структурами и ключами.
    """
    connection = connect_db()
    cursor = connection.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            id SERIAL PRIMARY KEY,
            company_name VARCHAR(256) UNIQUE NOT NULL
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS vacancies (
            id SERIAL PRIMARY KEY,
            name VARCHAR(256) NOT NULL,
            salary INT NOT NULL,
            url VARCHAR(256) NOT NULL,
            company_name VARCHAR(256),
            FOREIGN KEY (company_name) REFERENCES companies(company_name) ON DELETE CASCADE
        );
    """)

    connection.commit()

    cursor.close()
    connection.close()