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


Mongorcs = mongo(db=settings.mongo_DB,  col=settings.col_RCS)
Mongorcsp = mongo(db=settings.mongo_DB,  col=settings.col_RCSp)
Mongorbe = mongo(db=settings.mongo_DB,  col=settings.col_RBE)
Mongorbep = mongo(db=settings.mongo_DB,  col=settings.col_RBEp)
Mongopdf = mongo(db=settings.mongo_DB,  col=settings.col_pdfs)
Mongofinancials = mongo(db=settings.mongo_DB,  col=settings.col_finan)
Mongopubli = mongo(db=settings.mongo_DB,  col=settings.col_publi)



RCSlist = ['A39456', 'B100006', 'B100007', 'B100008', 'B100009', 'B106338', 'B120246', 'B1202461', 'B120247', 'B120248',
           'B14674', 'B14710', 'B14815', 'B15268', 'B15425', 'B15467', 'B15489', 'B15490', 'B15491', 'B15492', 'B15590',
           'B15846', 'B15904', 'B16286', 'B16336', 'B16468', 'B16607', 'B16677', 'B16768', 'B16844', 'B16854', 'B16855',
           'B16923', 'B16924', 'B17015', 'B17016', 'B17020', 'B17218', 'B17286', 'B17298', 'B17479', 'B182934', 'B215643',
           'B221018', 'B221019', 'B248373', 'B249879', 'B255380', 'B262114', 'B39099', 'B56047', 'E1879', 'E2226', 'E229',
           'E2818', 'E4589', 'E4738', 'E5555', 'E7052', 'E7905', 'E831','B256373']

RCSlist = ['B16468','B182934', 'B215643','B221018', 'B221019', 'B248373']


RCSlist = [
    'A28576',
    'A31099',
    'B37592',
    'B107798',
    'B177788',
    'B33950',
    'B200011',
    'B244342',
    'B243280',
    'B13038',
    'B19760',
    'B13089'
]

RCSlist = ['B212831', 'B214530']


RCSlist = [
    'B16153',
    'B110797',
    'B105296',
    'B110291',
    'B102685',
    'F462',
    'B252738',
    'B252738',
    'B51793',
    'E2234',
    'A24829',
    'A23114',
    'B233468',
    'B169659',
    'B125213',
    'E876',
    'B239090',
    'B78425',
    'B127516',
    'E6078',
    'B239418',
    'B233398',
    'B161376',
    'B197884',
    'B136657',
    'F10989',
    'B128558',
    'B64459',
    'B166604',
    'E6355',
    'J135',
    'F9894',
    'B102924',
    'B102930',
    'B253196',
    'B163690',
    'F907'
    ]





# 1 - create not existing RCS in RCS collection, and update the existing ones
#Mongorcs.insert_empty_RCS(RCSlist,  update_existing=False)
#Mongorbe.insert_empty_RCS(RCSlist, update_existing=False)

# 2 - scrap RCS list in RCS
print('----Scraping RCS---')
#RCSlist_scr = scraper(type_='RCS', mongo=Mongorcs, to_be_updated=True)
#print(RCSlist_scr)
print('----RCS Scraped---')


# 3 - parse RCS of RCS list
print('----Parsing RCS---')


#Mongorcs = mongo('LBR_new',  col='LBR_RCS_31052021')

#rcs_parser(type_='rcs', mongo=Mongorcs, mongoparsed=Mongorcsp,  onlynew=False)

print('----RCS Parsed---')


# 4 - scrap RCS list in RBE: only the one in no ro new as changed RCS
print('----Scraping RBE---')
#RBElist = scraper(type_='RBE', mongo=Mongorbe, to_be_updated=True)
#print(RBElist)
print('----RBE Scraped---')

# 5 - parse RBE of RCS list
print('----Parsing RCS---')

#Mongorbe = mongo('LBR_new',  col='LBR_RBE')

rcs_parser(type_='rbe', mongo=Mongorcs, mongoparsed=Mongorcsp,  onlynew=True)
print('----RCS Parsed---')

# 6 - download new pdf not yet downloaded
print('----Downloading pdfs---')
#pdf_downloader(mongo_rcsparsed=Mongorcsp, mongo_pdfs=Mongopdf) #always consider only new "depot"
print('----pdfs Downloaded ---')

'''
# 7 - parsing bilan from downloaded pdf
print('----parsing bilans---')
#financials_parser(mongo=Mongopdf, mongoparsed=Mongofinancials, onlynew=False)
print('----bilans parsed ---')


# 8 - parsing publi from downloaded pdf
print('----parsing publi---')
#publi_parser(mongo=Mongopdf, mongoparsed=Mongopubli, onlynew=False)
print('----publi parsed ---')

'''
print(f"completed in {str(timer_main.stop())}s")





