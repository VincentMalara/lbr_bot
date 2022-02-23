import pandas as pd

from configs import settings
from src.scrapers.rcs.main import main as rcs_scraper
from src.html_parsers.rcs.main import main as rcs_parser


from .utils.timer import performance_timer

from src.mongo.main import mongo
from src.mongo.utils import insert_empty_RCS


timer_main = performance_timer()


mongo_ip = settings.mongo_ip
mongo_port = settings.mongo_port
mongo_DB = settings.mongo_DB
col_RCS = settings.col_RCS
col_RCSp = settings.col_RCSp


Mongorcs = mongo(db=mongo_DB,  col=col_RCS, ip='146.59.152.231')
Mongorcsp = mongo(db=mongo_DB,  col=col_RCSp, ip='146.59.152.231')

print(Mongorcs.get_RCSlist())


RCSlist_df = pd.read_excel('Luxembourg_Juillet 2021_nouvelles entreprises (2).xlsx')

print(RCSlist_df)

RCSlist = RCSlist_df['Numéro de RCS'].to_list()
print(RCSlist)

Mongorcs.delete()
Mongorcsp.delete()

insert_empty_RCS(RCSlist, Mongorcs)


print(f"completed in {str(timer_main.stop())}s")





