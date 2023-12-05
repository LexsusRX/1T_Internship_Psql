from airflow.utils.task_group import TaskGroup
import logging
import time
from datetime import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.utils.dates import days_ago
import re

import sys
import os
sys.path.insert(0, '/opt/airflow/dags/')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from variables_settings import variables, base_getmatch
from raw.base_job_parser import BaseJobParser

table_name = variables['raw_tables'][6]['raw_tables_name']
url = base_getmatch

logging.basicConfig(
    format='%(threadName)s %(name)s %(levelname)s: %(message)s',
    level=logging.INFO
)

log = logging

# Параметры по умолчанию
default_args = {
    "owner": "admin_1T",
    'start_date': days_ago(1)
}

class GetMatchJobParser(BaseJobParser):
    def find_vacancies(self):
        """
        This method parses job vacancies from the GetMatch website.
        It retrieves the vacancy details such as company name, vacancy name, skills required, location, job format,
        salary range, date created, and other relevant information.
        The parsed data is stored in a DataFrame for further processing.
        """
        self.items = []
        HEADERS = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:98.0) Gecko/20100101 Firefox/98.0",
        }
        self.log.info(f'Creating an empty list')
        self.items = []
        # seen_ids = set()
        self.all_links = []
        self.log.info(f'Parsing data')
        # Parse vacancy links from each page (approximately 50 pages on the website, 100 is more than enough)

        for i in range(1, 100):
            response = requests.get(url.format(i=i), headers=HEADERS)
            soup = BeautifulSoup(response.content, 'html.parser')
            divs = soup.find_all('div', class_='b-vacancy-card-title')
            for div in divs:
                vacancy_url = 'https://getmatch.ru/' + div.find('a').get('href')
                self.all_links.append(vacancy_url)

        # vacancy_count = 0
        for link in self.all_links:
            # if vacancy_count < 10:
            resp = requests.get(link, HEADERS)
            vac = BeautifulSoup(resp.content, 'lxml')

            try:
                # Parse job grade
                term_element = vac.find('div', {'class': 'col b-term'}, text='Уровень')
                level = term_element.find_next('div', {'class': 'col b-value'}).text.strip() if term_element else None

                # Parse foreign languages
                lang = vac.find('div', {'class': 'col b-term'}, text='Английский')
                if lang is not None:
                    level_lang= lang.find_next('span', {'class': 'b-language-description d-md-none'}).text.strip()
                    lang=lang.text.strip()
                    language = f"{lang}: {level_lang}"
                    if level==level_lang:
                        level=None
                else:
                    language=None

                # Parse skills
                stack_container = vac.find('div', class_='b-vacancy-stack-container')
                if stack_container is not None:
                    labels = stack_container.find_all('span', class_='g-label')
                    page_stacks = ', '.join([label.text.strip('][') for label in labels])
                else:
                    page_stacks=None

                # Parse job description
                description_element = vac.find('section', class_='b-vacancy-description')
                description_lines = description_element.stripped_strings
                description = '\n'.join(description_lines)

                # Parse and parse salary range
                salary = vac.find('h3').text
                salary_text = vac.find('h3').text.strip()
                salary_text = salary_text.replace('\u200d', '-').replace('—', '-')
                salary_parts = list(map(str.strip, salary_text.split('-')))
                if ('₽' or '€' or '$' or '₸') in salary:
                    сurr_salary_from = salary_parts[0]
                    if len(salary_parts) == 1:
                        сurr_salary_to = None if 'от' in salary else salary_parts[0]
                    elif len(salary_parts) > 1:
                        сurr_salary_to = salary_parts[2]
                if '€' in salary:
                    currency_id = 'EUR'
                elif '$' in salary:
                    currency_id = 'USD'
                elif '₸' in salary:
                    currency_id = 'KZT'
                elif '₽' in salary:
                    currency_id = 'KZT'
                if сurr_salary_from is not None:
                    numbers = re.findall(r'\d+', сurr_salary_from)
                    combined_number = ''.join(numbers)
                    сurr_salary_from = int(combined_number) if combined_number else None
                if сurr_salary_to is not None:
                    numbers = re.findall(r'\d+', сurr_salary_to)
                    combined_number = ''.join(numbers)
                    сurr_salary_to = int(combined_number) if combined_number else None
                # if '₽/мес на руки' in vac.find('h3').text:
                #     salary_parts = list(map(str.strip, salary_text.split('-')))
                #     salary_from = salary_parts[0]
                #     if len(salary_parts) == 1:
                #         salary_to = None if 'от' in vac.find('h3').text else salary_parts[0]
                #     elif len(salary_parts) > 1:
                #         salary_to = salary_parts[2]
                # else:
                #     salary_from = None
                #     salary_to = None
                #
                # # Convert salary range to numeric format
                # if salary_from is not None:
                #     numbers = re.findall(r'\d+', salary_from)
                #     combined_number = ''.join(numbers)
                #     salary_from = int(combined_number) if combined_number else None
                # if salary_to is not None:
                #     numbers = re.findall(r'\d+', salary_to)
                #     combined_number = ''.join(numbers)
                #     salary_to = int(combined_number) if combined_number else None

                # Parse job format
                job_form_classes = ['g-label-linen', 'g-label-zanah', 'ng-star-inserted']
                job_form = vac.find('span', class_=job_form_classes)
                job_format = job_form.get_text(strip=True) if job_form is not None else None

                # Get other variables
                date_created = date_of_download = datetime.now().date()
                status ='existing'
                version_vac=1
                actual=1

                item = {
                    "company": vac.find('h2').text.replace('\xa0', ' ').strip('в'),
                    "vacancy_name": vac.find('h1').text,
                    "skills": page_stacks,
                    "towns": ', '.join([span.get_text(strip=True).replace('📍', '') for span in vac.find_all('span', class_='g-label-secondary')]),
                    "vacancy_url": link,
                    "description": description,
                    "job_format": job_format,
                    "level": level,
                    "сurr_salary_from": сurr_salary_from,
                    "сurr_salary_to": сurr_salary_to,
                    "currency_id": currency_id,
                    "date_created": date_created,
                    "date_of_download": date_of_download,
                    "source_vac": 1,
                    "status": status,
                    "version_vac": version_vac,
                    "actual": actual,
                    "languages":language,
                }
                print(f"Adding item: {item}")
                item_df = pd.DataFrame([item])
                self.df = pd.concat([self.df, item_df], ignore_index=True)
                time.sleep(3)
                # vacancy_count += 1
            except AttributeError as e:
                print(f"Error processing link {link}: {e}")
            # else:
            #     break
        self.df = self.df.drop_duplicates()
        self.log.info("Total number of found vacancies after removing duplicates: " + str(len(self.df)) + "\n")

