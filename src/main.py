from configs import settings
from src.scrapers.main import main as scraper
from src.html_parsers.main import main as rcs_parser
from src.pdf_downloaders.main import main as pdf_downloader
from src.pdf_parsers.financials.main import main as financials_parser
from src.pdf_parsers.publications.main import main as publi_parser
from src.utils.RCS_spliter import main as rcs_spliter


from .utils.timer import performance_timer
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



#0 build RCS list
DF = Mongorcs.find(dictin={}, dictout={'RCS': 1,'extraction_date':1, '_id': 0})
DF['extraction_date'] = pd.to_datetime(DF['extraction_date'], format='%d/%m/%Y')
DF = DF[DF['extraction_date']>pd.to_datetime('01/12/2021', format='%d/%m/%Y')].reset_index(drop=True)
print(DF)
RCSlist_rcs = list(DF['RCS'].unique())

DF = Mongorbe.find(dictin={}, dictout={'RCS': 1,'extraction_date':1, '_id': 0})
DF['extraction_date'] = pd.to_datetime(DF['extraction_date'], format='%d/%m/%Y')
DF = DF[DF['extraction_date']>pd.to_datetime('01/12/2021', format='%d/%m/%Y')].reset_index(drop=True)
print(DF)
RCSlist_rbe = list(DF['RCS'].unique())

'''
#1 - parse RCS
rcs_parser(RCS=RCSlist_rcs,type_='rcs', mongo=Mongorcs, mongoparsed=Mongorcsp,  onlynew=False)

#2 - parse RBE
rcs_parser(RCS=RCSlist_rbe,type_='rbe', mongo=Mongorbe, mongoparsed=Mongorbep,  onlynew=False)
'''



RCS_splited_lists = rcs_spliter(RCSlist_rcs, 1000)

for rcslist in RCS_splited_lists:
    print('----Downloading pdfs---')
    pdf_downloader(RCS=rcslist,mongo_rcsparsed=Mongorcsp, mongo_pdfs=Mongopdf)
    print('----pdfs Downloaded ---')


RCSlist = Mongorcs.get_RCSlist()
RCS_splited_lists = rcs_spliter(RCSlist, 10000)

for rcslist in RCS_splited_lists:
    print('----parsing publi---')
    publi_parser(RCS=rcslist, mongo=Mongopdf, mongoparsed=Mongopubli, onlynew=False)
    print('----publi parsed ---')
    print('----parsing financials ---')
    financials_parser(RCS=rcslist, mongo=Mongopdf, mongoparsed=Mongofinan, onlynew=False)
    print('----financials parsed ---')
