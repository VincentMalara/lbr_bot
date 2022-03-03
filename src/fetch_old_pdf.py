from configs import settings
from src.scrapers.main import main as scraper
from src.html_parsers.rcs.main import main as rcs_parser
from src.html_parsers.rbe.main import main as rbe_parser
#from src.pdf_downloaders.main import main as pdf_downloader
#from src.pdf_parsers.financials.main import main as financials_parser
#from src.pdf_parsers.publications.main import main as publi_parser

from .utils.timer import performance_timer
from src.mongo.main import mongo

import pandas as pd

timer_main = performance_timer()