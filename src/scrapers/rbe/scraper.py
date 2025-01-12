import time

from bs4 import BeautifulSoup
from selenium.webdriver.common.keys import Keys


from src.scrapers.scraper import scraper
from src.utils.set_logger import main as set_logger
from src.scrapers.utils import scrap_page_check


logger = set_logger()


class Rbe(scraper):
    def __init__(self,  **kwargs):
        print("---running in rbe mode---")
        self.type = 'rbe'
        if 'Mongo' in kwargs.keys():
            scraper.__init__(self, type_='rbe', mongo=kwargs['Mongo'])
        else:
            scraper.__init__(self, type_='rbe')
            print(f'info rbe scrper init: no Mongo set')
            logger.info(f'info rbe scrper init: no Mongo set')


    def scrap_rcs(self):
        self.check_search_page()
        self.info = ''
        self.page = ''
        test_page_changed_RCS = False
        if self.status:
            if isinstance(self.rcs, str):
                self.search_rcs()
                if self.status:
                    trial = 0
                    time.sleep(3) #can be improve depending of network speed
                    while trial < 10:
                        self.page = BeautifulSoup(self.driver.page_source, features="html.parser")
                        rbestatus = scrap_page_check(self.page, self.rcs)
                        test_page_changed_RCS = rbestatus['testvalid1'] or rbestatus['testvalid2']
                        if rbestatus['test_err']:
                            # RCS number des not exist (red banner)
                            print(f"RBE {self.rcs} doesn't exist")
                            self.status = True
                            self.extract_page()
                            self.record_empty_RCS()
                            trial = 10
                        elif rbestatus['test_err2']:
                            print(f"RBE {self.rcs} not_registrated_BO")
                            self.extract_page()
                            self.record_notregist_BO()
                            self.status = True
                            trial = 10
                        elif rbestatus['test_rcs']:
                            # RCS number exist --> scrap page
                            print(f"RBE {self.rcs} does exist")
                            self.extract_page()
                            self.record_RCS_content()
                            self.status = True
                            # print("going back to search")
                            back = self.driver.find_element_by_link_text('Recherche')
                            back.click()
                            trial = 10
                        else:
                            trial += 1
                            time.sleep(trial*0.5)
                            self.status = False
                            print(f'error after search rcs: {self.rcs} at check page results')
                            logger.debug(f'error after search rcs: {self.rcs} at check page results')
                    print(f'out of while of {self.rcs}: trial = {trial}, self.status = {self.status}')
                    if test_page_changed_RCS and not self.status:
                        # RCS number des not exist (red banner)
                        self.status = True
                        self.extract_page()
                        self.record_changed_RCS()
                        print(f'{self.rcs} probably changed RCS number')
                        logger.debug(f'{self.rcs} probably changed RCS number')
                        back = self.driver.find_element_by_link_text('Recherche')
                        back.click()
                        self.status = True
                else:
                    print('error at rcs.scrap_rcs: status=False after search')
                    logger.error('error at rcs.scrap_rcs: status=False after search')
        else:
            print('error at rcs.scrap_rcs: not at correct page')
            logger.error('error at rcs.scrap_rcs: not at correct page')

    def search_rcs(self):
        test_page_search = self.check_search_page()
        trial = 0
        while not test_page_search:
            print(f"trial at search_rcs: {trial}")
            trial += 1
            if not test_page_search:
                try:
                    back = self.driver.find_element_by_link_text('Recherche')
                    back.click()
                    print('clicked to recherche')
                except Exception:
                    pass
                test_page_search = self.check_search_page()
                if trial > 20:
                    print(f'error at search_rcs: max trial at going to search page reached')
                    logger.debug(f'error at search_rcs: max trial at going to search page reached')
                    test_page_search = True
                    self.status = False
        if self.status:
            try:
                filrcs = self.driver.find_element_by_id('rcsNumber')
                filrcs.clear()
                filrcs.send_keys(self.rcs)
                filrcs.send_keys(Keys.RETURN)
                self.status = True
            except Exception as e:
                print(f'error at search_rcs: {self.rcs}, error : {e}')
                logger.debug(f'error at search_rcs: {self.rcs}, error : {e}')
                self.status = False
        if self.status:
            self.check_connected()