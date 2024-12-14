import psycopg2


class DBManager:
    """Класс, который подключается к БД PostgreSQL."""

    def __init__(self, params):
        self.conn = psycopg2.connect(dbname='hh', **params)
        self.cur = self.conn.cursor()

    def get_companies_and_vacancies_count(self):
        """ Функция, которая получает список всех компаний и количество вакансий у каждой компании."""
        self.cur.execute("""
        SELECT employers.employer_name, COUNT(vacancies.employer_id)
        FROM employers
        JOIN vacancies USING (employer_id)
        GROUP BY employers.employer_name
        ORDER BY COUNT DESC
        """)

        return self.cur.fetchall()

    def get_all_vacancies(self):
        """ Функция, которая получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию. """
        self.cur.execute("""
        SELECT employers.employer_name, vacancy_name, salary, vacancy_url
        FROM vacancies
        JOIN employers USING (employer_id)
        ORDER BY salary desc""")
        return self.cur.fetchall()

    def get_avg_salary(self):
        """ Функция, которая получает среднюю зарплату по вакансиям """
        self.cur.execute("""
        SELECT avg(salary) from vacancies""")
        return self.cur.fetchall()

    def get_vacancies_with_higher_salary(self):
        """ Функция, которая получает список всех вакансий,
        у которых зарплата выше средней по всем вакансиям. """
        self.cur.execute("""
        SELECT vacancy_name, salary
        FROM vacancies
        GROUP BY vacancy_name, salary
        having salary > (SELECT AVG(salary) FROM vacancies)
        ORDER BY salary DESC
        """)
        return self.cur.fetchall()

    def get_vacancies_with_keyword(self, keyword):
        """ Функция, которая получает список всех вакансий, в названии которых
        содержатся переданные в метод слова."""
        request_sql = """
        SELECT * FROM vacancies
        WHERE LOWER (vacancy_name) LIKE %s
        """
        self.cur.execute(request_sql, ('%' + keyword.lower() + '%',))
        return self.cur.fetchall()
