import time


from selenium.webdriver.common.keys import Keys


from src.scrapers.main import connection
from src.utils.set_logger import main as set_logger


logger = set_logger()


class rcs(connection):
    def __init__(self,  **kwargs):
        print("---running in rcs mode---")
        self.type = 'rcs'
        connection.__init__(self, type_='rcs',  **kwargs)

    def scrap_rcs(self):
        self.check_search_page()
        if self.status:
            if isinstance(self.rcs, str):
                self.search_rcs()
                if self.status:
                    trial = 0
                    time.sleep(0.5)
                    while trial < 10:
                        test_err = self.check_page("Les erreurs suivantes ont été détectées", 'b')
                        test_rcs = self.check_page(self.rcs, 'h1')
                        print(f"trial at scrap_rcs: {trial}")
                        if test_err:
                            # RCS number des not exist (red banner)
                            print("RCS doesn't exist")
                            self.status = True
                            self.record_empty_RCS()
                            trial = 10
                        elif test_rcs:
                            # RCS number exist --> scrap page
                            print("RCS does exist")
                            self.extract_page()
                            self.record_RCS_content()
                            self.status = True
                            # print("going back to search")
                            back = self.driver.find_element_by_link_text('Recherche')
                            back.click()
                            trial = 10
                        else:
                            trial += 1
                            time.sleep(trial * 0.5)
                            self.status = False
                            print(f'error after search rcs: {self.rcs} at check page results')
                            logger.debug(f'error after search rcs: {self.rcs} at check page results')
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





