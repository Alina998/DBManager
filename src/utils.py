import requests
import psycopg2
from typing import Any, List


employer_ids = [1473866, 64174, 3809, 84585, 2871786, 1057, 15478, 1440683, 9311920, 2523]


def get_employers_data() -> List[dict]:
    """ Функция для получения данных о компаниях с сайта HH.ru. """
    employers = []
    for employer_id in employer_ids:
        url_emp = f"https://api.hh.ru/employers/{employer_id}"
        response = requests.get(url_emp)
        try:
            employer_info = response.json()  # Попробуем декодировать как JSON
            employers.append(employer_info)
        except ValueError as e:
            print(f"Ошибка декодирования данных для работодателя {employer_id}: {e}")
            continue  # Пропустим этого работодателя, если произошла ошибка

    return employers


def get_vacancies_data() -> List[dict]:
    """ Функция для получения данных о вакансиях с сайта HH.ru. """
    vacancies = []
    for employer_id in employer_ids:
        vacancy_url = f"https://api.hh.ru/vacancies?employer_id={employer_id}"
        response = requests.get(vacancy_url, params={'page': 0, 'per_page': 100})
        try:
            vacancy_info = response.json()  # Попробуем декодировать как JSON
            vacancies.extend(vacancy_info['items'])
        except ValueError as e:
            print(f"Ошибка декодирования данных для вакансий работодателя {employer_id}: {e}")
            continue  # Пропустим этого работодателя, если произошла ошибка
    return vacancies


def create_database(database_name: str, params: dict) -> None:
    """ Функция для создания БД и таблиц в БД. """
    conn = None  # Инициализируем conn как None
    try:
        # Подключаемся к базе данных 'postgres' для создания новой базы данных
        conn = psycopg2.connect(dbname='postgres', **params)
        conn.autocommit = True
        cur = conn.cursor()

        # Удаляем базу данных, если она существует
        cur.execute(f'DROP DATABASE IF EXISTS {database_name}')
        # Создаем новую базу данных
        cur.execute(f'CREATE DATABASE {database_name}')

    except Exception as e:
        print(f"Ошибка при создании базы данных: {e}")
        raise  # Повторно выбрасываем исключение для дальнейшей обработки
    finally:
        if conn:
            conn.close()

    # Подключаемся к новой базе данных
    conn = None  # Снова инициализируем conn как None
    try:
        conn = psycopg2.connect(dbname=database_name, **params)

        with conn.cursor() as cur:
            # Создаем таблицы в новой базе данных
            cur.execute("""
                CREATE TABLE employers (
                    employer_id INTEGER PRIMARY KEY,
                    employer_name TEXT NOT NULL,
                    employer_area TEXT NOT NULL,
                    url TEXT,
                    open_vacancies INTEGER
                )
            """)

            cur.execute("""
                CREATE TABLE vacancies (
                    vacancy_id INTEGER PRIMARY KEY,
                    vacancy_name VARCHAR NOT NULL,
                    vacancy_area VARCHAR,
                    salary FLOAT,
                    employer_id INTEGER REFERENCES employers(employer_id),
                    vacancy_url VARCHAR
                )
            """)

            conn.commit()
    except Exception as e:
        print(f"Ошибка при создании таблиц: {e}")
        raise  # Повторно выбрасываем исключение для дальнейшей обработки
    finally:
        if conn:
            conn.close()


def save_employers_to_database(employers: List[dict], database_name: str, params: dict) -> None:
    """ Функция для заполнения таблицы компаний в БД. """
    conn = None
    try:
        conn = psycopg2.connect(dbname=database_name, **params)

        with conn.cursor() as cur:
            for employer in employers:
                cur.execute("""
                    INSERT INTO employers (employer_id, employer_name, employer_area, url, open_vacancies)
                    VALUES (%s, %s, %s, %s, %s) """,
                    (employer['id'], employer['name'], employer['area']['name'], employer['alternate_url'], employer['open_vacancies']))
            conn.commit()
    except Exception as e:
        print(f"Ошибка подключения к базе данных: {e}")
        raise
    finally:
        if conn:
            conn.close()


def save_vacancies_to_database(vacancies: List[dict], database_name: str, params: dict) -> None:
    """ Функция для заполнения таблицы вакансий в БД. """
    conn = None
    try:
        conn = psycopg2.connect(dbname=database_name, **params)

        with conn.cursor() as cur:
            for vacancy in vacancies:
                salary = vacancy['salary']['from'] if vacancy['salary'] else 0
                cur.execute("""
                    INSERT INTO vacancies (vacancy_id, vacancy_name, vacancy_area, salary, employer_id, vacancy_url)
                    VALUES (%s, %s, %s, %s, %s, %s) """,
                    (vacancy.get('id'), vacancy['name'], vacancy['area']['name'], salary,
                     vacancy['employer']['id'], vacancy['alternate_url']))
            conn.commit()
    except Exception as e:
        print(f"Ошибка подключения к базе данных: {e}")
        raise
    finally:
        if conn:
            conn.close()