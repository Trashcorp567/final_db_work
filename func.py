import requests
from Classes import DBManager
import psycopg2
import configparser
from psycopg2 import sql

# Создание объекта для работы с конфигурацией
config = configparser.ConfigParser()

# Чтение значений из файла config.ini
config.read('config.ini')

# Получение значений
DB_HOST = config['database']['host']
DB_NAME = config['database']['database']
DB_USER = config['database']['user']
DB_PASSWORD = config['database']['password']

# Переменные для подключений к БД
db_params = {
    "host": DB_HOST,
    "user": DB_USER,
    "password": DB_PASSWORD
}
# Имя базы данных
db_name = "vacancies_kr"

# Подключение к PostgreSQL без указания базы данных
conn = psycopg2.connect(**db_params)
conn.autocommit = True
cur = conn.cursor()

# Проверка существования базы данных
check_db_query = sql.SQL("SELECT datname FROM pg_catalog.pg_database WHERE datname = %s")
cur.execute(check_db_query, (db_name,))
exists = cur.fetchone()

# Если база данных существует, ничего не делаем, если нет, создаём
if not exists:
    # Создаём базу данных
    cur.execute(f"CREATE DATABASE {db_name}")

# Подключение к созданной базе данных
db_params["database"] = db_name
conn = psycopg2.connect(**db_params)
conn.autocommit = True
cur = conn.cursor()

# Здесь можно продолжить остальную часть кода

cur.close()
conn.close()


def fetch_hh_vacancies():
    '''
    Получение данных с hh по API
    Только Москва, только с мин.зп
    '''
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
    '''
    Сортирует данные для работы с БД
    '''
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
    '''
    Функция для проверки работы функциона
    '''
    create_vacancies_table(db_params)
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
    '''
    Создаёт таблицу в БД, если таблица существет, ловит ошибку
    '''
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    table_exists_query = """
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = 'vacancies'
    )
    """
    cursor.execute(table_exists_query)
    table_exists = cursor.fetchone()[0]

    if not table_exists:
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
    else:
        print("")

    cursor.close()
    conn.close()
