import json
import psycopg2
from config import host, user, password, db_name
from timestamp_serializer import DatetimeEncoder


# Создание таблицы
def create_table(cursor):
    cursor.execute(
        """CREATE TABLE users(
            id serial PRIMARY KEY,
            first_name CHARACTER VARYING(30) NOT NULL,
            last_name CHARACTER VARYING(30) NOT NULL,
            date timestamp,
            price REAL NOT NULL,
            amount INTEGER);"""
    )
    print("[INFO] Таблица создана")


# Добавление данных
def create_data(cursor):
    cursor.execute(
        """INSERT INTO users (first_name, last_name, date, price, amount) VALUES
        ('Alex', 'Popkov', NOW(), '135.86', '34'),
        ('Egor', 'Sidorov', NOW(), '15.85', '12');"""
    )
    print("[INFO] Данные были добавлены")


# Получение данных
def select_data(cursor):
    cursor.execute(
        """SELECT * FROM users;"""
    )
    print("[INFO] Данные были получены")


# Создание JSON файла
def json_create(cursor):
    cursor.execute("""SELECT * FROM users;""")
    user_list = cursor.fetchall()
    user_list_json = json.dumps(user_list, cls=DatetimeEncoder)
    with open("user_list.json", "w") as file:
        file.write(user_list_json)
    print("[INFO] JSON файл создан")


def main():
    try:
        # Подключение к базе данных
        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=db_name
        )
        connection.autocommit = True

        # Создание таблицы
        with connection.cursor() as cursor:
            create_table(cursor)
            create_data(cursor)
            select_data(cursor)
            json_create(cursor)

    except Exception as _ex:
        print("[INFO] Ошибка при работе с PostgreSQL", _ex)
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL соединение закрыто")


if __name__ == "__main__":
    main()
