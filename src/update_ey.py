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
Mongorcsp = mongo(ip='146.59.152.231', db='LBR_test', col='RCS_parsed')


print(Mongorcsp.find({'Forme juridique':{ '$regex' : '.*' + 'commandite spéciale' + '.*'}}))
