import dateparser
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv, json
import psycopg2
from airflow import settings
from psycopg2.extras import execute_values
from psycopg2.extensions import register_adapter, AsIs
from airflow import settings
from sqlalchemy import create_engine
from airflow import DAG
from selenium.common.exceptions import NoSuchElementException
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from typing import Callable
from airflow.utils.task_group import TaskGroup
import logging
from logging import handlers
from airflow import models
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
import time
from datetime import datetime, timedelta, date
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options as ChromeOptions
import pandas as pd
import numpy as np
import os
#далее импорт из скрипта и минус то что уже есть
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import requests
from fake_useragent import UserAgent
import lxml
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")

# Connections settings
# Загружаем данные подключений из JSON файла
with open('/opt/airflow/dags/config_connections.json', 'r') as conn_file:
    connections_config = json.load(conn_file)

# Получаем данные конфигурации подключения и создаем конфиг для клиента
conn_config = connections_config['psql_connect']

config = {
    'database': conn_config['database'],
    'user': conn_config['user'],
    'password': conn_config['password'],
    'host': conn_config['host'],
    'port': conn_config['port'],
}

conn = psycopg2.connect(**config)

# Variables settings
# Загружаем переменные из JSON файла
with open('/opt/airflow/dags/config_variables.json', 'r') as config_file:
    my_variables = json.load(config_file)

# Проверяем, существует ли переменная с данным ключом
if not Variable.get("shares_variable", default_var=None):
    # Если переменная не существует, устанавливаем ее
    Variable.set("shares_variable", my_variables, serialize_json=True)

dag_variables = Variable.get("shares_variable", deserialize_json=True)

url_careerist = "https://careerist.ru/"

raw_tables = ['raw_vk', 'raw_sber', 'raw_tinkoff', 'raw_yandex', 'del_vacancy_core', 'raw_careerist']

#options = ChromeOptions()
#options = webdriver.ChromeOptions()
#options.add_argument("--headless")
#options.add_argument('--no-sandbox')
#options.add_argument('--disable-dev-shm-usage')

profs = dag_variables.get('professions')

logging_level = os.environ.get('LOGGING_LEVEL', 'DEBUG').upper()
logging.basicConfig(level=logging_level)
log = logging.getLogger(__name__)
log_handler = handlers.RotatingFileHandler('/opt/airflow/logs/airflow.log',
                                           maxBytes=5000,
                                           backupCount=5)

log_handler.setLevel(logging.DEBUG)
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_handler.setFormatter(log_formatter)
log.addHandler(log_handler)

# Параметры по умолчанию
default_args = {
    "owner": "admin_1T",
    # 'start_date': days_ago(1),
    'retry_delay': timedelta(minutes=5),
}

# Создаем DAG ручного запуска (инициализирующий режим).
initial_dag = DAG(dag_id='initial_dag',
                tags=['admin_1T'],
                start_date=datetime(2023, 10, 29),
                schedule_interval=None,
                default_args=default_args
                )

class DatabaseManager:
    def __init__(self, conn):
        self.conn = conn
        self.cur = conn.cursor()
        self.raw_tables = raw_tables
        self.log = LoggingMixin().log

    def create_raw_tables(self):
        table_name = 'raw_careerist'
        try:
            drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
            self.cur.execute(drop_table_query)
            self.log.info(f'Удалена таблица {table_name}')

            create_table_query = f"""                
            CREATE TABLE {table_name}(
               vacancy_id VARCHAR(2083) NOT NULL,
               vacancy_name VARCHAR(255),
               towns VARCHAR(255),
               level VARCHAR(255),
               company VARCHAR(255),
               salary_from BIGINT,
               salary_to BIGINT,
               exp_from SMALLINT,
               exp_to SMALLINT,
               description TEXT,
               job_type VARCHAR(255),
               job_format VARCHAR(255),
               languages VARCHAR(255),
               skills VARCHAR(511),
               source_vac VARCHAR(255),
               date_created DATE,
               date_of_download DATE NOT NULL, 
               status VARCHAR(32),
               date_closed DATE,
               version_vac INTEGER NOT NULL,
               actual SMALLINT,
               PRIMARY KEY(vacancy_id)
            );
            """
            self.cur.execute(create_table_query)
            self.conn.commit()
            self.log.info(f'Таблица {table_name} создана в базе данных.')
        except Exception as e:
            self.log.error(f'Ошибка при создании таблицы {table_name}: {e}')
            self.conn.rollback()



#class BaseJobParser:
#    def __init__(self, url_careerist, profs, log, conn):
#        self.browser = webdriver.Remote(command_executor='http://selenium-router:4444/wd/hub', options=options)
#        self.url_careerist = url_careerist
#        self.browser.get(self.url_careerist)
#        self.browser.maximize_window()
#        self.browser.delete_all_cookies()
#        time.sleep(2)
#        self.profs = profs
#        self.log = log
#        self.conn = conn
#
#    def scroll_down_page(self, page_height=0):
#        """
#        Метод прокрутки страницы
#        """
#        self.browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
#        time.sleep(2)
#        new_page_height = self.browser.execute_script('return document.body.scrollHeight')
#        if new_page_height > page_height:
#            self.scroll_down_page(new_page_height)
#
#    def stop(self):
#        """
#        Метод для выхода из Selenium Webdriver
#        """
#        self.browser.quit()
#
#    def find_vacancies(self):
#        """
#        Метод для парсинга вакансий, должен быть переопределен в наследниках
#        """
#        raise NotImplementedError("Вы должны определить метод find_vacancies")
#
#    def save_df(self):
#        """
#        Метод для сохранения данных из pandas DataFrame
#        """
#        raise NotImplementedError("Вы должны определить метод save_df")


class careeristJobParser:
    """
    Парсер вакансий с сайта careerist, наследованный от BaseJobParser
    """
    df = pd.DataFrame({

    'vacancy_name': [],
    'company': [],
    'salary_from': [],
    'towns': [],
    'date': [],
    'url_careerist': [],
    'description': [],
    'current_date': pd.to_datetime(date.today())
    })
    
    professions = [
        {"Data engineer"},
        # {"Data analyst"},
        # {"Data scientist"},
        # {"System analyst"},
        # {"Business analyst"},
        # {"Project manager"},
        # {"Product manager"},
        # {"Backend developer"},
        # {"Frontend developer"},
        # {"Fullstack developer"},
        # {"digital marketing specialist"},
        # {"Web designer"},
        # {"UI/UX"},
        # {"UX/UI"},
        # {"QA engineer"},
        {"DevOps"}
    ]
     
    # обходит капчу и берет самый первый адрес странницы с поиском
    def bypass_captcha(url_careerist, proff):
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
#        with webdriver.Chrome(service=ChromeService(ChromeDriverManager().install())) as browser:
        with webdriver.Remote(command_executor='http://127.0.0.1:10144/wd/hub', options=options) as browser
            browser.get(url_careerist)
            time.sleep(2)
    
            try:
                # ищем поле ввода и вводим текст
                inp = browser.find_element(By.XPATH, '//*[@id="keyword-text"]')
                inp.send_keys(proff)
                time.sleep(2)
            except:
                pass
    
            try:
                # ищем кнопку "найти" и жмем ёё
                browser.find_element(By.XPATH,
                                    '/html/body/div[1]/div[2]/div[1]/div/div/div/div[3]/div/div/div/div/form/div[1]/div[3]/button').click()
                time.sleep(1)
            except:
                pass
    
            try:
                # ищем кнопку "закрыть рекламу" и жмем ёё
                browser.find_element(By.XPATH, '/html/body/div[3]/div/form/div/div[1]/button').click()
                time.sleep(4)
            except:
                pass
    
            try:
                # ищем кнопку "снять фильтр" и жмем ёё
                browser.find_element(By.XPATH,
                                    '/html/body/div[1]/div[2]/div/section/div[2]/div[2]/div/div[1]/form/div/div[1]/div[3]/div/ul/li/a/span').click()
                time.sleep(4)
            except:
                pass
    
            try:
                # ищем кнопку "ПОКАЗАТЬ" и жмем ёё
                browser.find_element(By.XPATH,
                                    '/html/body/div[1]/div[2]/div/section/div[2]/div[2]/div/div[1]/form/div/div[1]/div[1]/div[2]/a').click()
                time.sleep(4)
            except:
                pass
    
                # получаем текущий URL страницы
            current_url_careerist = browser.current_url_careerist
    
            return current_url_careerist
        
    # сохраняет все url_careerist вакансии
    def get_all_links(result):
        link_url_careerist = []
        current_page = result + "&page="
        page_number = 0
    
        while True:
            page_url_careerist = current_page + str(page_number)
    
            try:
                response = requests.get(page_url_careerist, headers, verify=False)
                if response.status_code == 404:
                    break
                soup = BeautifulSoup(response.content, 'lxml')
                links = soup.find_all('p', class_='h5 card-text')
                for link in links:
                    link = link.find('a')
                    url_careerists = link['href']
                    link_url_careerist.append(url_careerists)
    
                #             links = get_links_from_page(page_url_careerist)
                #             link_url_careerist.extend(links)
                page_number += 1
            except:
                pass  # requests.exceptions.RequestException:
        #            break
    
        return link_url_careerist


    
    def scrape_data(all_links, df):
        with tqdm(total=len(all_links), desc="Scraping Data", unit="link") as pbar:
           for url_careerist in all_links:
                scrap = {}  # Создайте новый пустой словарь для каждой вакансии
                response = requests.get(url_careerist, headers=headers, timeout=10, verify=False)
                soup = BeautifulSoup(response.content, 'lxml')
                try:
                    scrap['vacancy_name'] = soup.find('h1').text.strip()
                    print(scrap['vacancy_name'])
                except:
                    pass
                try:
                    scrap['company'] = soup.find('div', class_='m-b-10').text.strip()
                except:
                    pass
                try:
                    bam = soup.find_all('div', attrs={'class', 'b-b-1'})[1]
                    scrap['salary_from'] = bam.find('p', attrs={'class': 'h5'}).text
                except:
                    pass
                try:
                    scrap['towns'] = soup.find('p', class_='col-xs-8 col-sm-9').text.strip()
                except:
                    pass
                try:
                    scrap['date'] = soup.find('p', class_='pull-xs-right m-l-1 text-small').text.strip()
                except:
                    pass
                scrap['url_careerist'] = url_careerist
                try:
                    scrap['description'] = soup.find_all('div', attrs={'class', 'b-b-1'})[2].text.strip()
                except:
                    pass

                df = df._append(scrap, ignore_index=True)

                pbar.update(1)

                time.sleep(1)

        # Удаление дубликатов
        df.drop_duplicates(inplace=True)

        # Сохранение в CSV-файл с дополнением
        df.to_csv('caryerist_debag.csv', mode='a', index=False, header=False)

        self.df = self.df.drop_duplicates()
        self.log.info("Общее количество найденных вакансий после удаления дубликатов: " + str(len(self.df)) + "\n")
#        self.df['vacancy_id'] = hash(url_careerist, self.df['description'])
#        self.df['date_created'] = datetime.now().date()
        self.df['date_of_download'] = datetime.now().date()
#        self.df['source_vac'] = url_careerist
        self.df['status'] = 'existing'
        self.df['version_vac'] = 1
        self.df['actual'] = 1
        return df
        
    def find_vacancies(self, url_careerist, profs, df):
        for proff in professions:
            result = bypass_captcha(url_careerist, proff)
            print(result)
            print(len(result))
            all_links = get_all_links(result)
            print(all_links)
            print(len(all_links))
            df = scrape_data(all_links, df)


    def save_df(self):
        """
        Метод для сохранения данных в базу данных careerist
        """
        self.cur = self.conn.cursor()

        def addapt_numpy_float64(numpy_float64):
            return AsIs(numpy_float64)

        def addapt_numpy_int64(numpy_int64):
            return AsIs(numpy_int64)

        register_adapter(np.float64, addapt_numpy_float64)
        register_adapter(np.int64, addapt_numpy_int64)

        try:
            if not self.df.empty:
                self.log.info(f"Проверка типов данных в DataFrame: \n {self.df.dtypes}")
                table_name = raw_tables[0]
                # данные, которые вставляются в таблицу PosqtgreSQL
                data = [tuple(x) for x in self.df.to_records(index=False)]
                # формируем строку запроса с плейсхолдерами для значений
                query = f"INSERT INTO {table_name} (vacancy_id, vacancy_name, towns, company, source_vac, " \
                        f"date_created, date_of_download, status, version_vac, actual, description) VALUES %s"
                # исполняем запрос с использованием execute_values
                self.log.info(f"Запрос вставки данных: {query}")
                execute_values(self.cur, query, data)

                self.conn.commit()
                # логируем количество обработанных вакансий
                self.log.info("Общее количество загруженных в БД вакансий: " + str(len(self.df)) + "\n")

        except Exception as e:
            self.log.error(f"Ошибка при сохранении данных в функции 'save_df': {e}")
            raise

        finally:
            # закрываем курсор и соединение с базой данных
            self.cur.close()
            self.conn.close()


db_manager = DatabaseManager(conn=conn)

def init_run_careerist_parser(**context):
    """
    Основной вид задачи для запуска парсера для вакансий careerist
    """
    log = context['ti'].log
    log.info('Запуск парсера careerist')
    try:
        parser = careeristJobParser(url_careerist, profs, log, conn)
#        parser.bypass_captcha(url_careerist, profs)
#        parser.get_all_links(url_careerist)
        parser.find_vacancies(url_careerist, profs)
#        parser.find_vacancies_description()
        parser.save_df()
        parser.stop()
        log.info('Парсер careerist успешно провел работу')
    except Exception as e:
        log.error(f'Ошибка во время работы парсера careerist: {e}')


hello_bash_task = BashOperator(
    task_id='hello_task',
    bash_command='echo "Желаю удачного парсинга! Да прибудет с нами безотказный интернет!"')

# Определение задачи
create_raw_tables = PythonOperator(
    task_id='create_raw_tables',
    python_callable=db_manager.create_raw_tables,
    provide_context=True,
    dag=initial_dag
)

parse_careeristjobs = PythonOperator(
    task_id='parse_careeristjobs',
    python_callable=init_run_careerist_parser,
    provide_context=True,
    dag=initial_dag
)

end_task = DummyOperator(
    task_id="end_task"
)

hello_bash_task >> create_raw_tables >> parse_careeristjobs >> end_task
