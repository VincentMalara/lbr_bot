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


Mongorcs = mongo(db=mongo_DB,  col=col_RCS, ip='146.59.152.231')
Mongorcsp = mongo(db=mongo_DB,  col=col_RCSp, ip='localhost')

Mongorcsp.delete()

#3 - parse RCS of RCS list
print('----Parsing RCS---')
rcs_parser(RCS=Mongorcs.get_RCSlist({'status':"scraped"}), mongo=Mongorcs, mongoparsed=Mongorcsp)
print('----RCS Parsed---')



print(f"completed in {str(timer_main.stop())}s")





