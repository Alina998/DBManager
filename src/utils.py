import requests
import psycopg2
from typing import Any

employer_ids = [1473866, 64174, 3809, 84585, 2871786, 1057, 15478, 1440683, 9311920, 2523]


def get_employers_data():
    """ функция для получения данных о компаниях с сайта HH.ru. """
    employers = []
    for employer_id in employer_ids:
        url_emp = f"https://api.hh.ru/employers/{employer_id}"
        employer_info = requests.get(url_emp, ).json()
        employers.append(employer_info)

    return employers


def get_vacancies_data():
    """ функция для получения данных о вакансиях с сайта HH.ru. """
    vacancies = []
    for employer_id in employer_ids:
        vacancy_url = f"https://api.hh.ru/vacancies?employer_id={employer_id}"
        vacancy_info = requests.get(vacancy_url, params={'page': 0, 'per_page': 100}).json()
        vacancies.extend(vacancy_info['items'])
    return vacancies


def create_database(database_name: str, params: dict) -> None:
    """ Функция для создания БД и таблиц в БД. """
    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f'DROP DATABASE IF EXISTS {database_name}')
    cur.execute(f'CREATE DATABASE {database_name}')

    conn.close()

    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE employers (
                employer_id INTEGER,
                employer_name TEXT not null,
                employer_area TEXT not null,
                url TEXT,
                open_vacancies INTEGER
            )
        """)

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE vacancy (
                vacancy_id INTEGER,
                vacancy_name VARCHAR,
                vacancy_area VARCHAR,
                salary INTEGER,
                employer_id INTEGER,
                vacancy_url VARCHAR
            )
        """)

    conn.commit()
    conn.close()


def save_employers_to_database(employers_data: list[dict[str, Any]], database_name: str, params: dict) -> None:
    """ Функция для заполнения таблицы компаний в БД. """
    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        for employer in employers_data:
            cur.execute("""
                INSERT INTO employers (employer_id, employer_name, employer_area, url, open_vacancies)
                VALUES (%s, %s, %s, %s, %s) """,
                (employer['id'], employer['name'], employer['area']['name'], employer['alternate_url'], employer['open_vacancies']))

    conn.commit()
    conn.close()


def save_vacancies_to_database(vacancies_data: list[dict[str, Any]], database_name: str, params: dict) -> None:
    """ Функция для заполнения таблицы вакансий в БД. """

    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        for vacancy in vacancies_data:
            if vacancy['salary'] is None or vacancy['salary']['from'] is None:
                cur.execute("""
                   INSERT INTO vacancy (vacancy_id, vacancy_name, vacancy_area, salary, employer_id, vacancy_url)
                   VALUES (%s, %s, %s, %s, %s, %s) """,
                    (vacancy.get('id'), vacancy['name'], vacancy['area']['name'], 0, vacancy['employer']['id'],
                    vacancy['alternate_url']))
            else:
                cur.execute("""
                    INSERT INTO vacancy (vacancy_id, vacancy_name, vacancy_area, salary, employer_id, vacancy_url)
                    VALUES (%s, %s, %s, %s, %s, %s) """,
                    (vacancy.get('id'), vacancy['name'], vacancy['area']['name'], vacancy['salary']['from'],
                    vacancy['employer']['id'], vacancy['alternate_url']))

    conn.commit()
    conn.close()
