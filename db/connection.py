import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()


def connect_db() -> psycopg2.extensions.connection:
    """
    Устанавливает соединение с базой данных PostgreSQL.

    Использует параметры подключения, указанные в переменных окружения:
    - DB_NAME: имя базы данных
    - DB_USER: имя пользователя
    - DB_PASSWORD: пароль пользователя
    - DB_HOST: хост базы данных
    - DB_PORT: порт подключения

    Возвращает:
        psycopg2.extensions.connection: Объект соединения с базой данных PostgreSQL.
    """
    connect = psycopg2.connect(
        dbname=os.environ.get('DB_NAME'),
        user=os.environ.get('DB_USER'),
        password=os.environ.get('DB_PASSWORD'),
        host=os.environ.get('DB_HOST'),
        port=os.environ.get('DB_PORT')
    )
    return connect
