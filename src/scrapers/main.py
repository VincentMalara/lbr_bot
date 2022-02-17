from datetime import datetime
import random
import time
import sys

import bs4 as beautifulsoup
from fake_useragent import UserAgent
import pandas as pd
from selenium import webdriver
from selenium.webdriver.firefox.options import Options

from configs import settings
from src.utils.set_logger import main as set_logger
from src.scrapers.utils import check_page, answer_question, get_numbers
from src.utils.handle_RCS_list import HandleRcsList
from src.utils.task_index import main as task_index

logger = set_logger()

URL_LBR = settings.URL_LBR
URL_RBE = settings.URL_RBE
URL_RCS = settings.URL_RCS
URL_RESA = settings.URL_RESA

BASE_DICT = {
    'extraction_date': datetime.today().strftime("%d/%m/%Y"),
    'RCS': '',
    'status': '',
    'info': '',
    'scraper_version': settings.scraper_version,
    'scraping_index': ''
}


class scraper():
    def __init__(self, type_='rcs', headless=settings.headless, mongo=None):
        self.headless = headless
        self.type = type_
        self.status = True
        self.rcs_list = []
        self.rcs = ''
        self.info = ''
        self.page = ''
        self.dict_page = []
        self.Nlimit = 10
        if mongo is not None:
            self.Mongo = mongo
        if self.type == 'rbe':
            self.url = URL_RBE
        elif self.type == 'rcs':
            self.url = URL_RCS
        elif self.type == 'resa':
            self.url = URL_RESA
        else:
            self.status = False
            print(f"type {self.type} is not known")
            logger.info(f"type {self.type} is not known")
        self.task_index = int(task_index(task='scraper', lbrtype=self.type)) + 1 #increment task index to track company scraped together
        self.RUA = ''
        self.options = Options()
        self.useragent = UserAgent()
        self.profile = webdriver.FirefoxProfile()
        self.reset()

    def reset(self):
        #print(' - reset()')
        self.quit()
        self.options = Options()
        self.options.headless = self.headless
        self.profile = webdriver.FirefoxProfile()
        self.profile.set_preference("general.useragent.override", self.useragent.random)
        self.driver = webdriver.Firefox(options=self.options, firefox_profile=self.profile,
                                        executable_path=settings.executablepath)


    def get_url(self):
        ee = ''
        try:
            self.driver.get(self.url)
            self.status = True
        except Exception as e:
            self.status = False
            print(f'error at get_url: {e}')
            ee = e
        return ee


    def get_LBR_page(self):
        #print(' - get_LBR_page()')
        e = self.get_url()
        trial = 0
        if not self.status:
            while trial < 3:
                print(f"trial at get_LBR_page: {trial}")
                time.sleep(2*trial)
                self.reset()
                e = self.get_url()
                if self.status:
                    trial = 4
        if not self.status:
            self.quit()
            print(f'driver has been stop because of following error at get_LBR_page: {e}')
            logger.error(f'driver has been stop because of following error at get_LBR_page: {e}')
            sys.exit()

        time.sleep(random.random()+1.5)
        self.RUA = self.driver.execute_script("return navigator.userAgent;")
        print(self.RUA)
        logger.info(f'user agent: {self.RUA}')

    def quit(self):
        #print(' - quit()')
        try:
            self.driver.quit()
            print('driver has been closed')
            logger.info('driver has been closed')
        except Exception as e:
            logger.debug('not managed to close driver properly')
            print(f'error at quit : {e}')

    def accept_t(self):
        #print(' - accept_t()')
        if not check_page(self.driver, 'Conditions générales du LBR', 'h1'):
            logger.debug("page error at accept_t(driver) - Conditions générales")
            print("page error at accept_t(driver) - Conditions générales")
            self.status=False
        else:
            pass
        #print("accepting...")
        try:
            connection = self.driver.find_element_by_id('conditionsversionId')
            connection.click()
            time.sleep(random.random()+0.5)
            logger.info("terms and cond accepeted successfully")
            self.status = True
        except Exception as e:
            logger.error(f"error at accept_t: {e}")
            print(f"error at accept_t: {e}")
            self.status = False


    def break_captcha(self):
        #print(' - break_captcha()')
        #try:
        if not check_page(self.driver, 'Question de sécurité', 'h3'):
            logger.debug("page error at break_captcha(driver) - Question de sécurité")
            print("page error at break_captcha(driver) - Question de sécurité")
            self.status = False
        else:
            pass
        #answer the question
        connection = self.driver.find_element_by_id("captcha.result.uu")
        connection.clear()
        page_content = beautifulsoup.BeautifulSoup(self.driver.page_source, features="html.parser")
        zone = page_content.find('div', {"class": "field"})
        question = zone.find('label', {"class": "field-label"}).get_text()
        connection.send_keys(answer_question(question))
        #accept the terms and conditions
        connection = self.driver.find_element_by_name("acceptCIE")
        connection.click()
        time.sleep(random.random()+0.5)
        #connect
        connection = self.driver.find_element_by_name("connection")
        connection.click()
        time.sleep(random.random()+1.5)
        self.status = True
        #except Exception as e:
        #    print(f"error: {e} , during break captcha")
        #    logger.info(f"error: {e} , during break captcha")
        #    self.status = False


    def get_connected(self):
        #print(' - get_connected()')
        #click on connect
        try:
            connection = self.driver.find_element_by_class_name('deconnected')
            connection.click()
            time.sleep(random.random()+1.5)
        except Exception as e:
            self.status = False
            logger.error(f"error at get_connected : {e}")
            print(f"error at get_connected : {e}")

        if not check_page(self.driver, 'Utilisateur anonyme', 'span'):
            self.status = False
            logger.debug("page error at get_connected(driver) - Utilisateur anonyme")
            print("page error at get_connected(driver) - Utilisateur anonyme")
        else:
            try: #click on utilisateur anonyme
                connection = self.driver.find_element_by_link_text('Utilisateur anonyme')
                connection.click()
            except Exception as e:
                self.status = False
                logger.error(f"error at get_connected : {e}")
                print(f"error at get_connected : {e}")
            if self.status:
                time.sleep(random.random()+1.5)
                self.break_captcha()
            if self.status:
                self.accept_t() # accept the conditions générales
                try:
                    self.check_connected()
                except Exception as e:
                    print(f'self.status check_co error: {e}')
                    self.status = False
                print(f'self.status check_co : {self.status}')

    def get_search(self):
        if self.type == 'rbe':
            search_button = 'Rechercher un dossier RBE'
        elif self.type == 'rcs':
            search_button = 'Rechercher un dossier RCS'
        else:
            self.status = False
            print(f"type {self.type} is not valid for get search")
            logger.info(f"type {self.type} is not valid for get search")
        if self.status:
            self.driver.get(self.url)
            if not check_page(self.driver, 'Rechercher un dossier R', 'a'):
                logger.debug("page error at get_connected(driver) - Rechercher un dossier R")
                print("page error at get_connected(driver) - Rechercher un dossier R")
                self.status = False
            else:
                rech = self.driver.find_element_by_link_text(search_button)
                rech.click()
                time.sleep(random.random() + 0.5)
                self.status = True


    def get_journal(self):
        if self.type in ['resa']:
            if not check_page(self.driver, 'Journal des publications', 'a'):
                logger.debug("page error at get_journal() - Journal des publications")
                print("page error at get_journal() - Journal des publications")
                self.status = False
            else:
                journal = self.driver.find_element_by_link_text('Journal des publications')
                journal.click()
                time.sleep(random.random() + 0.5)
                self.status = True
        else:
            logger.debug(f"get_journal not valid for {self.type} type")
            print(f"get_journal not valid for {self.type} type")
            self.status = False

    def get_main_page(self):
        if self.type == 'resa':
            self.get_journal()
        elif self.type in ['rcs', 'rbe']:
            self.get_search()
        else:
            logger.debug(f"get_main_page not valid for {self.type} type")
            print(f"get_main_page not valid for {self.type} type")

    def check_connected(self):
        page_content = beautifulsoup.BeautifulSoup(self.driver.page_source, features="html.parser")
        found = page_content.find_all("a", {"class": "deconnected"})

        if len(found) > 0:
            logger.debug(f'Not connected anymore')
            print(f'Not connected anymore')
            output = False
            self.status = False
            n = 0
            test_deco_click = True
            while test_deco_click:
                test_deco_click = ((not output) and (n < 4))
                print(test_deco_click)
                n += 1
                print(n)
                try:
                    connection = self.driver.find_element_by_class_name('deconnected')
                    connection.click()
                    self.status = True
                    output = True
                except Exception as e:
                    self.status = False
                    output = False
                    print(f" error while trying to click on connection: {e}")
                time.sleep(random.random() + 1.5)
                print("clicked on connexion")
                page_content = beautifulsoup.BeautifulSoup(self.driver.page_source, features="html.parser")
                found = page_content.find_all("a", {"class": "deconnected"})
                if len(found) == 0:
                    logger.debug('reconnected after clicking on connexion')
                    print('reconnected after clicking on connexion')
                    self.status = True
        else:
            self.status = True

    def launch(self):
        self.get_LBR_page()
        time.sleep(2)
        self.get_connected()
        time.sleep(2)
        self.get_main_page()
        time.sleep(2)
        if check_page(self.driver, "Acceptation des conditions générales", "b"):
            self.accept_t()
            print('accepted')
        try:
            self.check_connected()
        except Exception as e:
            print(f'error at launch: {e}')
            self.status = False

    def check_page(self, check_phrase, type_):
        self.status = check_page(self.driver, check_phrase, type_)
        return self.status

    def extract_page(self):
        print(' - extract_LBR_page()')
        #page = beautifulsoup.BeautifulSoup(self.driver.page_source,  features="html.parser")
        page = self.page
        if self.type == 'rcs':
            dict_info = {"id": "content"}
            self.info = str(page.find('div', dict_info))
            self.status = True
        elif self.type == 'rbe':
            dict_info = {"class": "withInfoOut"}
            self.info = str(page.find_all('div', dict_info))
            self.status = True
        else:
            print(f'error at extract_page: {self.type} not accepted')
            logger.debug(f'error at extract_page: {self.type} not accepted')
            self.status = False
        #except Exception as e:
         #   print(f'error at extract_page: {self.rcs}, error : {e}')
          #  logger.debug(f'error at extract_page: {self.rcs}, error : {e}')
           # self.status = False

    def scrap_list(self, rcs_list=None):
        status = False
        msg=''
        if rcs_list is None:
            print('error at rcs.scrap_list : rcs_list input is missing')
            logger.error('error at rcs.scrap_list : rcs_list input is missing')
        else:
            self.rcs_list, status, msg = HandleRcsList(rcs_list)

        if not status:
            print(msg)
            logger.error(msg)
            sys.exit()

        if status:
            self.dict_page = []
            N = 0
            for rcs in self.rcs_list:
                N+=1
                print(f"N: {N}")
                print(f"rcs: {rcs}")
                self.rcs = rcs
                trial = 0
                if not self.check_search_page():
                    while trial < 3:
                        print(f"trial at scrap_list: {trial}")
                        time.sleep(trial*6)
                        trial += 1
                        self.quit()
                        self.launch()
                        if self.check_search_page():
                            trial = 4
                            print('managed to reconnect properly')
                        else:
                            print('not managed to reconnect properly, loop will be aborted')
                if self.check_search_page():
                    self.scrap_rcs()
                else:
                    self.status = False
                    break
                    print('break done')
                if N>self.Nlimit:
                    N=0
                    self.save()
                    self.dict_page = []
            if len(self.dict_page)>0:
                print("last save")
                self.save()
                self.dict_page = []


    def check_search_page(self):
        if self.type == 'rcs':
            self.check_page("Recherche d'une entité (Société, commerçant, ASBL, ...)", "h1")
        elif self.type == 'rbe':
            self.check_page("Recherche d'une entité (Société, ASBL, ...)", "h1")
        elif self.type == 'resa':
            self.check_page("Journal des publications", "h1")
        else:
            self.status = False
            print(f'error at check_search_page: {self.type} not accepted')
            logger.debug(f'error at check_search_page: {self.type} not accepted')
        return self.status

    def save(self):
        if hasattr(self, 'Mongo'):
            self.Mongo.insert(self.dict_page)
            print(f'{len(self.dict_page)} companies updated')
            self.Mongo.drop_duplicates(colsel='task_index', coldup='RCS')
        else:
            print(f'error at save: mongo not specified')
            logger.debug(f'error at save: mongo not specified')
            sys.exit()


    def record_RCS_content(self):
        #print(" - record_RCS_content(RCS, content)")
        dict_page = BASE_DICT.copy()
        dict_page['extraction_date'] = datetime.today().strftime("%d/%m/%Y")
        dict_page['RCS'] = self.rcs
        dict_page['status'] = 'scraped'
        dict_page['info'] = self.info
        dict_page['task_index'] = self.task_index
        dict_page['info'] = self.info
        self.dict_page.append(dict_page)

    def record_empty_RCS(self):
        #print(" - record_empty_RCS(RCS)")
        logger.info(f"record_empty_RCS({self.rcs})")
        dict_page = BASE_DICT.copy()
        dict_page['extraction_date'] = datetime.today().strftime("%d/%m/%Y")
        dict_page['RCS'] = self.rcs
        if self.type == 'rbe':
            dict_page['status'] = 'no_rbe_informations'
        else:
            dict_page['status'] = 'doesnt_exist'
        dict_page['task_index'] = self.task_index
        dict_page['info'] = self.info
        self.dict_page.append(dict_page)


    def record_changed_RCS(self):
        #print(" - record_empty_RCS(RCS)")
        logger.info(f"record_empty_RCS({self.rcs})")
        dict_page = BASE_DICT.copy()
        dict_page['extraction_date'] = datetime.today().strftime("%d/%m/%Y")
        dict_page['RCS'] = self.rcs
        dict_page['status'] = 'changed_RCS_number'
        dict_page['info'] = self.info
        dict_page['task_index'] =self.task_index
        self.dict_page.append(dict_page)

    def record_notregist_BO(self):
        #print(" - record_notregist_BO(RCS)")
        logger.info(f"record_empty_RCS({self.rcs})")
        dict_page = BASE_DICT.copy()
        dict_page['extraction_date'] = datetime.today().strftime("%d/%m/%Y")
        dict_page['RCS'] = self.rcs
        dict_page['info'] = self.info
        dict_page['status'] = 'not_registrated_BO'
        dict_page['task_index'] = self.task_index
        self.dict_page.append(dict_page)