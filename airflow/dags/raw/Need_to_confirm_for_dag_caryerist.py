import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import requests
from fake_useragent import UserAgent
import lxml
from tqdm import tqdm
import warnings

warnings.filterwarnings("ignore")
import pandas as pd
from datetime import datetime
from datetime import date


ua = UserAgent()
random_ua = ua.random
headers = {'User-Agent': random_ua}


df = pd.DataFrame({

    'vacancy_name': [],
    'company': [],
    'salary_from': [],
    'towns': [],
    'date': [],
    'url': [],
    'description': [],
    'current_date': pd.to_datetime(date.today())
})

#разкомментировать, тестировал на двух, т.к. долго отрабатывается
professions = [
    {"Data engineer"},
    {"Data analyst"},
    {"Data scientist"},
    {"System analyst"},
    {"Business analyst"},
    {"Project manager"},
    {"Product manager"},
    {"Backend developer"},
    {"Frontend developer"},
    {"Fullstack developer"},
    {"digital marketing specialist"},
    {"Web designer"},
    {"UI/UX"},
    {"UX/UI"},
    {"QA engineer"},
    {"DevOps"}
]

options = webdriver.ChromeOptions()
options.add_argument("--lang=de")


# обходит капчу и берет самый первый адрес странницы с поиском
def bypass_captcha(url, proff):
    options = webdriver.ChromeOptions()
    # в контейнере раскомментировать строку с selenium, а с Chrome закомментить
    with webdriver.Chrome(service=ChromeService(ChromeDriverManager().install())) as browser:
    #with browser = webdriver.Remote(command_executor='http://selenium-router:4444/wd/hub', options=options)
        browser.get(url)
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
        current_url = browser.current_url

        return current_url

# сохраняет все url вакансии
def get_all_links(result):
    link_url = []
    current_page = result + "&page="
    page_number = 0

    while True:
        page_url = current_page + str(page_number)

        try:
            response = requests.get(page_url, headers, verify=False)
            if response.status_code == 404:
                break
            soup = BeautifulSoup(response.content, 'lxml')
            links = soup.find_all('p', class_='h5 card-text')
            for link in links:
                link = link.find('a')
                urls = link['href']
                link_url.append(urls)
            page_number += 1
        except:
            pass 

    return link_url





def scrape_data(all_links, df):
    with tqdm(total=len(all_links), desc="Scraping Data", unit="link") as pbar:
       for url in all_links:
            scrap = {}  # Создайте новый пустой словарь для каждой вакансии
            response = requests.get(url, headers=headers, timeout=10, verify=False)
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
            scrap['url'] = url
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

    return df


url = "https://careerist.ru/"
for proff in professions:
    result = bypass_captcha(url, proff)
    # print(result)
    # print(len(result))
    all_links = get_all_links(result)
    # print(all_links)
    # print(len(all_links))
    df = scrape_data(all_links, df)


pd.set_option('display.max_colwidth', None)

df = pd.read_csv('caryerist_debag.csv')

df.duplicated().sum()

df.to_csv('caryerist_debag.csv', index=False)


