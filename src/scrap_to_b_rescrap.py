from configs import settings
from src.scrapers.main import main as scraper
from src.html_parsers.main import main as rcs_parser
from src.html_parsers.rbe.main import main as rbe_parser
#from src.pdf_downloaders.main import main as pdf_downloader
#from src.pdf_parsers.financials.main import main as financials_parser
#from src.pdf_parsers.publications.main import main as publi_parser

from .utils.timer import performance_timer
from src.mongo.main import mongo

import pandas as pd

timer_main = performance_timer()


Mongorcs = mongo(db=settings.mongo_DB,  col=settings.col_RCS)
Mongorcsp = mongo(db=settings.mongo_DB,  col=settings.col_RCSp)
Mongorbe = mongo(db=settings.mongo_DB,  col=settings.col_RBE)
Mongorbep = mongo(db=settings.mongo_DB,  col=settings.col_RBEp)
Mongopdf = mongo(db=settings.mongo_DB,  col=settings.col_pdfs)
Mongofinancials = mongo(db=settings.mongo_DB,  col=settings.col_finan)
Mongopubli = mongo(db=settings.mongo_DB,  col=settings.col_publi)



# 1 - create not existing RCS in RCS collection, and update the existing ones
'''
RCSlist=Mongorcsp.get_RCSlist({'ToRescrap':True})
print(RCSlist)
Mongorcs.insert_empty_RCS(RCSlist,  update_existing=True)
#Mongorbe.insert_empty_RCS(RCSlist, update_existing=False)

# 2 - scrap RCS list in RCS
print('----Scraping RCS---')
RCSlist_scr = scraper(type_='RCS', mongo=Mongorcs, to_be_updated=True)
print(RCSlist_scr)
print('----RCS Scraped---')
'''
RCSlist=Mongorcs.get_RCSlist({'extraction_date':"03/03/2022"})
# 3 - parse RCS of RCS list
print('----Parsing RCS---')
rcs_parser(type_='rcs',RCS=RCSlist, mongo=Mongorcs, mongoparsed=Mongorcsp,  onlynew=False)
print('----RCS Parsed---')


print(f"completed in {str(timer_main.stop())}s")





