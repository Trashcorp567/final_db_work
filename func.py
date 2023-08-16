import requests
from Classes import DBManager
import psycopg2

db_params = {
    "host": 'localhost',
    "database": 'vacancies_kr',
    "user": 'postgres',
    "password": 'jj15mnbl'
}


def fetch_hh_vacancies():
    'Получение данных с hh по API'
    api_url = "https://api.hh.ru/vacancies"
    params = {
        "per_page": 10,
        "page": 0,
        "area": 1,  # Москва
        "only_with_salary": True
    }
    response = requests.get(api_url, params=params)
    vacancies = response.json()
    return vacancies


def fetch_sq_db():
    'Сортирует данные для работы с БД'
    db_manager = DBManager(db_params)
    vacancies = fetch_hh_vacancies()

    for vacancy in vacancies['items']:
        company_name = vacancy['employer']['name']
        vacancy_name = vacancy['name']
        salary = vacancy['salary']['from'] if vacancy['salary'] and vacancy['salary']['from'] else None
        vacancy_link = vacancy['alternate_url']

        fetch_db = """
                    INSERT INTO vacancies (company_name, vacancy_name, salary, vacancy_link)
                    VALUES (%s, %s, %s, %s)
                """
        values = (company_name, vacancy_name, salary, vacancy_link)
        db_manager.execute_query(fetch_db, values)


def interact():
    'Функция для проверки работы функциона'
    db_func = DBManager(db_params)
    user = int(input('1 - получает список всех компаний и количество вакансий у каждой компании.\n'
                     '2 - получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию\n'
                     '3 - получает среднюю зарплату по вакансиям\n'
                     '4 - получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.\n'
                     '5 - получает список всех вакансий, в названии которых содержатся переданные в метод слова\n'))
    if user == 1:
        fetch_sq_db()
        print(db_func.get_companies_and_vacancies_count())
    elif user == 2:
        fetch_sq_db()
        print(db_func.get_all_vacancies())
    elif user == 3:
        fetch_sq_db()
        print(db_func.get_avg_salary())
    elif user == 4:
        fetch_sq_db()
        print(db_func.get_vacancies_with_higher_salary())
    elif user == 5:
        fetch_sq_db()
        user_word = input("Введите слово для запроса:")
        results = db_func.get_vacancies_with_keyword(user_word)
        if len(results) > 0:
            for result in results:
                company_name, vacancy_name, salary, vacancy_link = result
                print(f"{company_name} - {vacancy_name} - {salary} - {vacancy_link}")
        else:
            print('Данного слова нет среди доступных вакансий')


def create_vacancies_table(db_params):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    create_table_query = """
    CREATE TABLE vacancies (
        id SERIAL PRIMARY KEY,
        company_name VARCHAR,
        vacancy_name VARCHAR,
        salary INTEGER,
        vacancy_link VARCHAR
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    cursor.close()
    conn.close()