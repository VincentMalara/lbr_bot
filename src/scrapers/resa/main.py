import time

from selenium.webdriver.support.select import Select
from selenium.webdriver.common.keys import Keys
import pandas as pd
import bs4 as beautifulsoup

from .utils import *
from src.scrapers.main import scraper
from src.utils.set_logger import main as set_logger

logger = set_logger()
translator = str.maketrans({chr(10): '', chr(9): ''})

starting_date = {'year': 2021, 'month': 12}
years = ['2021','2022']


months = ['Janvier', 'Février', 'Mars', 'Avril', 'Mai', 'Juin', 'Juillet', 'Août', 'Septembre', 'Octobre', 'Novembre', 'Décembre']
monthdict = {}
monthdictrev = {}
for i,m in enumerate(months):
    monthdict[i]=m
    monthdictrev[m]=i




class Resa(scraper):
    def __init__(self,  **kwargs):
        print("---running in resa mode---")
        self.type = 'resa'
        scraper.__init__(self, type_='resa',  **kwargs)

    def scrap_day(self):
        self.check_search_page()
        print(self.status)
        self.pages = self.build_resa_list()

    def build_resa_list(self):
        year = '2022'
        month = 'Février'
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
        return pages












