import psycopg2


class DBManager:
    def __init__(self, db_params):
        '''
        Инициализация класса
        :param db_params: инициализирует параметры для подключения к бд
        '''
        self.conn = psycopg2.connect(**db_params)
        self.cursor = self.conn.cursor()

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def execute_query(self, query, values=None):
        '''
        Этот метод предназначен для выполнения SQL-запросов в базе данных с использованием курсора
        '''
        self.cursor.execute(query, values)
        self.conn.commit()

    def get_companies_and_vacancies_count(self):
        '''
        Метод для подсчёта кол-ва компаний и вакансий
        :return: Возвращает название компании и суммарное кол-во имебщихся у них вакансий
        '''
        fetch_db = """
            SELECT company_name, COUNT(*) as vacancy_count
            FROM vacancies
            GROUP BY company_name
        """
        self.cursor.execute(fetch_db)
        result = self.cursor.fetchall()

        return result

    def get_all_vacancies(self):
        '''
        Метод для вызова всех вакансий из таблицы
        :return: Возвращает название компании, название вакансии, за, ссылку на вакансию
        '''
        fetch_db = """
            SELECT company_name, vacancy_name, salary, vacancy_link
            FROM vacancies
        """
        self.cursor.execute(fetch_db)
        result = self.cursor.fetchall()
        return result

    def get_avg_salary(self):
        '''
        Метод вычисляет среднюю зп по таблице вакансий
        :return: возвращает значение средней зп
        '''
        fetch_db = """
            SELECT AVG(salary) as average_salary
            FROM vacancies
        """
        self.cursor.execute(fetch_db)
        result = self.cursor.fetchone()
        return result[0]

    def get_vacancies_with_higher_salary(self):
        '''
        Метод вычисляет среднюю зп по таблице
        :return: Возвращает значения зп которые выше средней по таблице
        '''
        avg_salary_query = """
            SELECT AVG(salary) as average_salary
            FROM vacancies
        """
        self.cursor.execute(avg_salary_query)
        avg_salary_result = self.cursor.fetchone()
        avg_salary = avg_salary_result[0]

        fetch_db = """
            SELECT company_name, vacancy_name, salary
            FROM vacancies
            WHERE salary > %s
        """
        self.cursor.execute(fetch_db, (avg_salary,))
        result = self.cursor.fetchall()
        vacancies_list = []
        for row in result:
            company_name, vacancy_name, salary = row
            vacancies_list.append(f"{company_name} - {vacancy_name} - {salary}")
        return vacancies_list

    def get_vacancies_with_keyword(self, keyword):
        '''
        Метод, для поиска в таблице по введённому слову пользователя
        :return: Возвращает результат поиска по ключевому слову в таблице
        '''
        fetch_db = """
            SELECT company_name, vacancy_name, salary, vacancy_link
            FROM vacancies
            WHERE vacancy_name ILIKE %s
        """
        self.cursor.execute(fetch_db, ('%' + keyword + '%',))
        result = self.cursor.fetchall()
        return result
