from configs import settings
from src.scrapers.rbe.main import main as rbe_scraper
from src.scrapers.rcs.main import main as rcs_scraper
from src.html_parsers.rcs.main import main as rcs_parser
from src.html_parsers.rbe.main import main as rbe_parser


from .utils.timer import performance_timer

from src.mongo.main import mongo
from src.mongo.utils import insert_empty_RCS



timer_main = performance_timer()


mongo_ip = settings.mongo_ip
mongo_port = settings.mongo_port
mongo_DB = settings.mongo_DB
col_RCS = settings.col_RCS
col_RBE = settings.col_RBE
col_RCSp = settings.col_RCSp
col_RBEp = settings.col_RBEp


Mongorcs = mongo(db=mongo_DB,  col=col_RCS)
Mongorcsp = mongo(db=mongo_DB,  col=col_RCSp)
Mongorbe = mongo(db=mongo_DB,  col=col_RBE)
Mongorbep = mongo(db=mongo_DB,  col=col_RBEp)

RCSlist = ["B120248", "B15268", "B15467", "B15425","B15489", "B15490", "B15492","B15489", "B255380", "E7905",
           'E7052',  'B1202461',   'B262114',  'B182934']


#0 - create not existing RCS in RCS collection
insert_empty_RCS(RCSlist, Mongorcs)

#0 - set_to_be_updated
Mongorcs.set_to_be_updated(RCSlist)

#2 - scrap RCS list in RCS
RCS_to_scrap = Mongorcs.get_RCSlist({'status': 'to_be_updated'})
rcs_scraper(RCS_to_scrap, Mongorcs)


#3 - parse RCS of RCS list
print('----Parsing RCS---')
rcs_parser(RCS=RCSlist, mongo=Mongorcs, mongoparsed=Mongorcsp)
print('----RCS Parsed---')


#4 - scrap RCS list in RBE: only the one in no ro new as changed RCS
print('----Scraping RBE---')
rbe_scraper(Mongorcs.get_RCSlist(RCSlist), Mongorbe)
print('----RBE Scraped---')


#5 - parse RBE of RCS list
print('----Parsing RCS---')
rbe_parser(RCS=RCSlist, mongo=Mongorbe, mongoparsed=Mongorbep)
print('----RCS Parsed---')




print(f"completed in {str(timer_main.stop())}s")





