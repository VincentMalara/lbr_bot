import time
import sys

from selenium.webdriver.support.select import Select
from selenium.webdriver.common.keys import Keys
import pandas as pd
import bs4 as beautifulsoup
from urllib.request import urlopen
import xmltodict

from .utils import *
from src.scrapers.scraper import scraper


translator = str.maketrans({chr(10): '', chr(9): ''})

def extract_table(page, month, year):
    AA = page.find('table', {"class": "commonTable"})
    rows = []
    for i in AA.find_all('tr'):
        row = i.find_all('td')
        if len(row) > 6:
            date = row[0].text.strip()
            codeRESA = row[1].text.translate(translator).strip()
            name = row[2].text.translate(translator).strip()
            url = [x['href'] for x in row[5].findAll('a')]
            rows.append(
                {'date': date, 'codeRESA': codeRESA, 'name': name, 'url': url, 'month': month, 'year': year})
    return rows


class Resa(scraper):
    def __init__(self, mongoRESA='', mongoRESAparsed='', **kwargs):
        print("---running in resa mode---")
        self.type = 'resa'
        self.pages = []
        self.mongoRESA = mongoRESA
        self.mongoRESAparsed = mongoRESAparsed
        scraper.__init__(self, type_='resa', **kwargs)

    def scrap_month(self, year='2022', month='Février'):
        self.check_search_page()
        print(self.status)
        self.pages = self.build_resa_list(year, month)

    def build_resa_list(self, year, month):
        pages = pd.DataFrame()
        Y = self.driver.find_element_by_name('selectedYear')
        if year in [y.get_attribute("text") for y in Y.find_elements_by_tag_name("option")]:
            Select(Y).select_by_visible_text(year)
            time.sleep(10)
            M = self.driver.find_element_by_name('selectedMonth')
            if month in [m.get_attribute("text") for m in M.find_elements_by_tag_name("option")]:
                Select(M).select_by_visible_text(month)
                time.sleep(10)
                page_bs = beautifulsoup.BeautifulSoup(self.driver.page_source, features="html.parser")
                pages = pages.append(extract_table(page_bs, month, year))
        self.pages = pages
        print(self.pages)
        return pages

    def push_pages_to_mongo(self):
        if self.pages.shape[0] > 0:
            alreadydone = self.mongoRESA.find()
            if alreadydone.shape[0] > 0:
                self.pages = self.pages[~self.pages['codeRESA'].isin(alreadydone['codeRESA'].to_list())].reset_index(drop=True)
            self.mongoRESA.insert(self.pages)
        else:
            print("error at push_pages_to_mongo: nothing sraped yet from RESA")

    def get_pages_from_mongo(self, dictin={}):
        self.pages = self.mongoRESA.find(dictin)


    def extract_xmls(self):
        if self.pages.shape[0] > 0:
            DF = pd.DataFrame(self.pages)
            output = []
            for index, row in DF.iterrows():
                urls = row['url']
                month = row['month']
                year = row['year']
                date = row['date']
                coderesa = row['codeRESA']
                for url in urls:
                    file = urlopen(url)
                    data = file.read()
                    file.close()
                    data = xmltodict.parse(data)
                    dict_ = data['ns2:JournalDesPublications']['ns2:ListePublications']
                    if isinstance(dict_['ns2:Publication'], list):
                        for i in dict_['ns2:Publication']:
                            dictout = dict(i)
                            dictout['month'] = month
                            dictout['year'] = year
                            dictout['date'] = date
                            dictout['codeRESA'] = coderesa
                            dictout['status'] = 'to_be_updated'
                            output.append(dictout)
                    elif isinstance(dict_['ns2:Publication'], dict):
                        dictout = dict(dict_['ns2:Publication'])
                        dictout['month'] = month
                        dictout['year'] = year
                        dictout['date'] = date
                        dictout['codeRESA'] = coderesa
                        dictout['status'] = 'to_be_updated'
                        output.append(dictout)

            self.mongoRESAparsed.insert(output)
        else:
            print("error at extract_xmls: nothing sraped yet from RESA")