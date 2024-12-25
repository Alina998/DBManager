from utils import (get_employers_data, create_database, save_employers_to_database, save_vacancies_to_database,
                   get_vacancies_data)
from src.config import config
from src.dbmanager import DBManager


def main():
    params = config()

    data_emp = get_employers_data()
    data_vac = get_vacancies_data()

    # Создание базы данных и таблиц
    create_database('hh', params)

    # Сохранение данных
    save_employers_to_database(data_emp, 'hh', params)
    save_vacancies_to_database(data_vac, 'hh', params)

    db_manager = DBManager(params)
    print(f"Выберите запрос: \n"
          f"1 - Список всех компаний и количество вакансий у каждой компании.\n"
          f"2 - Список всех вакансий с указанием названия компании, названия вакансии, зарплаты и ссылки на вакансию.\n"
          f"3 - Средняя зарплата по вакансиям.\n"
          f"4 - Список всех вакансий, у которых зарплата выше средней по всем вакансиям.\n"
          f"5 - Список всех вакансий, в названии которых содержится искомое слово.\n"
          f"0 - Выход из программы")

    while True:
        user_input = input('Введите номер запроса\n')
        if user_input == "1":
            companies_and_vacancies_count = db_manager.get_companies_and_vacancies_count()
            print(f"Список всех компаний и количество вакансий у каждой компании: {companies_and_vacancies_count}\n")

        elif user_input == "2":
            all_vacancies = db_manager.get_all_vacancies()
            print(f"Список всех вакансий: {all_vacancies}\n")

        elif user_input == '3':
            avg_salary = db_manager.get_avg_salary()
            print(f"Средняя зарплата по вакансиям: {avg_salary}\n")

        elif user_input == "4":
            vacancies_with_higher_salary = db_manager.get_vacancies_with_higher_salary()
            print(f"Список вакансий с зарплатой выше средней: {vacancies_with_higher_salary}\n")

        elif user_input == "5":
            keyword = input('Введите ключевое слово ')
            vacancies_with_keyword = db_manager.get_vacancies_with_keyword(keyword)
            print(f"Список вакансий с ключевым словом '{keyword}': {vacancies_with_keyword}\n")

        elif user_input == '0':
            break
        else:
            print("Некорректный ввод. Пожалуйста, попробуйте снова.")


if __name__ == '__main__':
    main()