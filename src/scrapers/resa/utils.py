import time
from datetime import datetime

from selenium.webdriver.support.select import Select
import pandas as pd
import bs4 as beautifulsoup
from urllib.request import urlopen
import xmltodict

from src.scrapers.scraper import scraper

today = datetime.today()
MONTHDICT = {1: 'Janvier', 2: 'Février', 3: 'Mars', 4: 'Avril', 5: 'Mai', 6: 'Juin', 7: 'Juillet',
             8: 'Août', 9: 'Septembre', 10: 'Octobre', 11: 'Novembre', 12: 'Décembre'}

MONTH = MONTHDICT[today.month]
YEAR = today.year


translator = str.maketrans({chr(10): '', chr(9): ''})


def generate_last_n_month(n):
    todayyear = datetime.today().year
    todaymonth = datetime.today().month
    monthlist = []

    for year in [todayyear-1, todayyear]:
        for month in MONTHDICT.keys():
            monthlist.append(f"{year}-{MONTHDICT[month]}")
            if month == todaymonth and year == todayyear:
                break
    monthlist = monthlist[-n:]
    return monthlist


def extract_table(page, month=MONTH, year=YEAR):
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
    def __init__(self, mongoRESA='', mongoRESAparsed='',mongorcs='', **kwargs):
        print("---running in resa mode---")
        self.type = 'resa'
        self.pages = []
        self.tobeupdated = []
        self.year = YEAR
        self.month = MONTH
        self.mongoRESA = mongoRESA
        self.mongoRESAparsed = mongoRESAparsed
        self.mongorcs=mongorcs
        scraper.__init__(self, type_='resa', **kwargs)

    def scrap_month(self, month=MONTH, year=YEAR):
        self.year = year
        self.month = month
        self.check_search_page()
        self.pages = self.build_resa_list()

    def build_resa_list(self):
        pages = pd.DataFrame()
        Y = self.driver.find_element_by_name('selectedYear')
        if self.year in [y.get_attribute("text") for y in Y.find_elements_by_tag_name("option")]:
            Select(Y).select_by_visible_text(self.year)
            time.sleep(10)
            M = self.driver.find_element_by_name('selectedMonth')

            if self.month in [m.get_attribute("text") for m in M.find_elements_by_tag_name("option")]:
                Select(M).select_by_visible_text(self.month)
                time.sleep(10)
                page_bs = beautifulsoup.BeautifulSoup(self.driver.page_source, features="html.parser")
                #pages = pages.append(extract_table(page_bs, month, year))
                page_new = pd.DataFrame.from_records(extract_table(page_bs, self.month, self.year))
                pages = pd.concat([pages, page_new])
        self.pages = pages
        #print(self.pages)
        return pages

    def push_pages_to_mongo(self):
        #print('push_pages_to_mongo')
        #print(self.month)
        #print(self.year)

        if self.pages.shape[0] > 0:
            #print('-*'*10)
            alreadydone = self.mongoRESA.find({'month':self.month, 'year': self.year, 'status':'done'})
            if alreadydone.shape[0] > 0:
                self.pages = self.pages[~self.pages['codeRESA'].isin(alreadydone['codeRESA'].to_list())].reset_index(drop=True)
            #print(self.pages)
            #print('='*10)
            self.pages['status'] = 'to_be_updated'
            self.mongoRESA.delete({'month':self.month, 'year': self.year,'status':'to_be_updated'})
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
            self.tobeupdated = pd.DataFrame.from_records(output[:30])#!!!!!!!
            self.mongoRESAparsed.insert(output[:30])#!!!!!!!

        else:
            print("error at extract_xmls: nothing sraped yet from RESA")

    def set_rcs_to_be_updated(self):
        if self.pages.shape[0] > 0:
            rcslist=self.tobeupdated['ns2:NumeroRCS'].unique().tolist()
            coderesalist=self.pages['codeRESA'].unique().tolist()
            self.mongorcs.insert_empty_RCS(rcslist, update_existing=True)
            self.mongoRESA.set_done(dictin={'codeRESA':{'$in':coderesalist}})
            self.mongoRESAparsed.set_done(dictin={'codeRESA':{'$in':coderesalist}})
        else:
            print('set_rcs_to_be_updated : pages DF is empty')
