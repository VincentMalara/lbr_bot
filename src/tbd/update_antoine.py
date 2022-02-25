from configs import settings
from src.scrapers.rcs.main import main as rcs_scraper
from src.html_parsers.rcs.main import main as rcs_parser


from src.utils.timer import performance_timer

from src.mongo.main import mongo


timer_main = performance_timer()


mongo_ip = settings.mongo_ip
mongo_port = settings.mongo_port
mongo_DB = settings.mongo_DB
col_RCS = settings.col_RCS
col_RCSp = settings.col_RCSp


Mongorcs = mongo(db=mongo_DB,  col=col_RCS)
Mongorcsp = mongo(db=mongo_DB,  col=col_RCSp)


#2 - scrap RCS list in RCS
RCS_to_scrap = Mongorcs.get_RCSlist({'status': 'to_be_updated'})
rcs_scraper(RCS_to_scrap, Mongorcs)


#3 - parse RCS of RCS list
print('----Parsing RCS---')
rcs_parser(mongo=Mongorcs, mongoparsed=Mongorcsp)
print('----RCS Parsed---')


print(f"completed in {str(timer_main.stop())}s")





