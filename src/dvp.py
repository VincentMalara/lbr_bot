from configs import settings
from src.scrapers.rbe.main import rbe
from src.scrapers.rcs.main import rcs

from .utils.timer import performance_timer

from src.mongo.main import mongo

timer_main = performance_timer()


mongo_ip = settings.mongo_ip
mongo_port = settings.mongo_port
mongo_DB = settings.mongo_DB
col_RCS = settings.col_RCS
col_RBE = settings.col_RBE

Mongo = mongo(db=mongo_DB,  col=col_RBE)

#RCS = Mongo.find({'status': 'to_be_scrapped'})
#print(RCS)

#RCS = ['']

#RCS = Mongo.find({'changed_RCS_number':1})
#print(RCS)


'''
RCS = ['E2245']

n=0
test = False
while not test and n<5:
    scraper = rbe(headless=False)
    scraper.launch()
    test = scraper.status
    print('---------')
    n += 1
    print(n)

print(f"connected in {str(timer_main.stop())}s")

timer_main = performance_timer()

scraper.scrap_list(RCS)
print(f'scraper.status: {scraper.status}')
Mongo.insert(scraper.dict_page)
print(f'{len(scraper.dict_page)} companies updated')
Mongo.drop_duplicates()
scraper.quit()

print(f"completed in {str(timer_main.stop())}s")

'''

Mongo = mongo(db=mongo_DB,  col=col_RCS)

RCS = Mongo.find({'status': 'to_be_scrapped'})
print(RCS)

#for type_ in ['rcs']: #'resa', 'rbe',
n=0
test = False
while not test and n<5:
    scraper = rcs(headless=False)
    scraper.launch()
    test = scraper.status
    print('---------')
    n += 1
    print(n)

print(f"connected in {str(timer_main.stop())}s")

timer_main = performance_timer()

scraper.scrap_list(RCS.sample(10))
print(f'scraper.status: {scraper.status}')
Mongo.insert(scraper.dict_page)
print(f'{len(scraper.dict_page)} companies updated')
Mongo.drop_duplicates()
scraper.quit()

print(f"completed in {str(timer_main.stop())}s")
