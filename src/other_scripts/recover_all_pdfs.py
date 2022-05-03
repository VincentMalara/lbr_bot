from configs import settings
from src.scrapers.main import main as scraper
from src.html_parsers.main import main as rcs_parser
from src.pdf_downloaders.main import main as pdf_downloader
from src.pdf_parsers.financials.main import main as financials_parser
from src.pdf_parsers.publications.main import main as publi_parser
from src.utils.RCS_spliter import main as rcs_spliter


from src.utils.timer import performance_timer
from src.mongo.main import mongo

import pandas as pd

timer_main = performance_timer()


Mongorcs = mongo(ip='146.59.152.231', db='LBR_test', col='RCS')
Mongorbe = mongo(ip='146.59.152.231', db='LBR_test', col='RBE')
Mongorcsp = mongo(ip='146.59.152.231', db='LBR_test', col='RCS_parsed')
Mongorbep = mongo(ip='146.59.152.231', db='LBR_test', col='RBE_parsed')
Mongoresa = mongo(ip='146.59.152.231', db='LBR_test', col='RESA_parsed')
Mongopdf= mongo(ip='146.59.152.231', db='LBR_test', col='all_pdfs')
Mongopubli = mongo(ip='146.59.152.231', db='LBR_test', col='publications')
Mongofinan = mongo(ip='146.59.152.231', db='LBR_test', col='financials')



RCSlist = Mongorcsp.get_RCSlist()


RCS_splited_lists = rcs_spliter(RCSlist, 1000)

i=0
for rcslist in RCS_splited_lists:
    print(i)
    i+=1
    # 6 - download new pdf not yet downloaded
    print('----Downloading pdfs---')
    pdf_downloader(RCS=rcslist,mongo_rcsparsed=Mongorcsp, mongo_pdfs=Mongopdf) #always consider only new "depot"
    print('----pdfs Downloaded ---')
