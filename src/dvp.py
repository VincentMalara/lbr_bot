from configs import settings
from src.scrapers.rbe.main import Rbe
from src.scrapers.rcs.main import Rcs
from src.utils.task_index import main as task_index
from src.html_parsers.main import rcs as rcsparser
from src.html_parsers.rcs.manage_changed_RCS import main as manage_changed_RCS

from src.html_parsers.main import rbe as rbeparser

from .utils.timer import performance_timer

from src.mongo.main import mongo



timer_main = performance_timer()


mongo_ip = settings.mongo_ip
mongo_port = settings.mongo_port
mongo_DB = settings.mongo_DB
col_RCS = settings.col_RCS
col_RBE = settings.col_RBE
col_RCSp = settings.col_RCSp
col_RBEp = settings.col_RBEp


print(task_index(task='scraper', lbrtype='rcs'))
print(task_index(task='scraper', lbrtype='rbe'))


Mongorcs = mongo(db=mongo_DB,  col=col_RCS)
Mongorcsp = mongo(db=mongo_DB,  col=col_RCSp)
Mongorbe = mongo(db=mongo_DB,  col=col_RBE)

#Mongorcs.set_to_be_updated()

RCS_to_scrap = Mongorcs.get_RCSlist({'status': 'to_be_updated'})
if len(RCS_to_scrap)>0:
    n=0
    test = False
    while not test:
        print('---------')
        print(n)
        scraper = Rcs(headless=False, Mongo=Mongorcs)
        scraper.launch()
        test = scraper.status
        n += 1
        if n>5:
            test=True

    print(f"connected in {str(timer_main.stop())}s")
    timer_main = performance_timer()
    scraper.scrap_list()
    scraper.quit()
    print(f"completed in {str(timer_main.stop())}s")


print('----Parsing RCS---')
RCS = Mongorcs.find()
RCSparsed = rcsparser(RCS)
Mongorcsp.insert(RCSparsed)
Mongorcsp.drop_duplicates(colsel='task_index', coldup='RCS')

manage_changed_RCS(Mongorcsp)
print('----RCS Parsed---')



n=0
test = False
while not test:
    print('---------')
    print(n)
    scraper = Rbe(headless=False,Mongo=Mongorbe)
    scraper.launch()
    test = scraper.status
    n += 1
    if n > 5:
        test = True

print(f"connected in {str(timer_main.stop())}s")
timer_main = performance_timer()
RCS_rbe_to_scrap = Mongorcsp.get_RCSlist({"exists": True, "changed_RCS_number": {'$in': ['no', 'new_one']}})
#RCS_rbe_to_scrap = ['B100007','B100008','B100009']
print("**************")
print(RCS_rbe_to_scrap)
print(len(RCS_rbe_to_scrap))
scraper.scrap_list(RCS_rbe_to_scrap)
scraper.quit()
print(f"completed in {str(timer_main.stop())}s")

Mongorcs.close()
Mongorcsp.close()