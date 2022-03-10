from configs import settings
from src.scrapers.main import main as scraper
from src.html_parsers.main import main as rcs_parser
#from src.pdf_downloaders.main import main as pdf_downloader
#from src.pdf_parsers.financials.main import main as financials_parser
#from src.pdf_parsers.publications.main import main as publi_parser

from .utils.timer import performance_timer
from src.mongo.main import mongo

import pandas as pd

timer_main = performance_timer()


Mongorcs = mongo(ip='146.59.152.231', db='LBR_test', col='RCS')
Mongorbe = mongo(ip='146.59.152.231', db='LBR_test', col='RBE')
Mongorcsp = mongo(ip='146.59.152.231', db='LBR_test', col='RCS_parsed')
Mongorbep = mongo(ip='146.59.152.231', db='LBR_test', col='RBE_parsed')
Mongoresa = mongo(ip='146.59.152.231', db='LBR_test', col='RESA_parsed')


# 4 - scrap RCS list in RBE: only the one in no ro new as changed RCS
print('----Scraping RBE---')
RBElist = scraper(type_='RBE', mongo=Mongorbe, to_be_updated=True)
print('----RBE Scraped---')

# 5 - parse RBE of RCS list
print('----Parsing RCS---')
rcs_parser(type_='rbe', mongo=Mongorbe, mongoparsed=Mongorbep,  onlynew=True)
print('----RCS Parsed---')
