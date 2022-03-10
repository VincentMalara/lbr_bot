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


DF = Mongorcsp.find({'Forme juridique':{ '$regex' : '.*' + 'commandite spéciale' + '.*'}})
RCSlist1 = DF['RCS'].to_list()
DF1 = Mongoresa.find({"ns2:Prestation":"Immatriculation",
                      "ns2:NumeroRCS":{ '$regex' : '.*' + 'B' + '.*'},
                     "month":{"$in":["Janvier", "Février"]}})

DF2 = Mongoresa.find({"ns2:NumeroRCS":{ "$in": RCSlist1},
                     "month":{"$in":["Janvier", "Février"]}})
RCSlist = DF1["ns2:NumeroRCS"].to_list() + DF2["ns2:NumeroRCS"].to_list()

print(RCSlist)
print(len(RCSlist))

Mongorcs.insert_empty_RCS(RCSlist,  update_existing=False)


print('----Scraping RCS---')
RCSlist_scr = scraper(type_='RCS', mongo=Mongorcs, to_be_updated=True)
print('----RCS Scraped---')


# 3 - parse RCS of RCS list
print('----Parsing RCS---')
rcs_parser(type_='rcs', mongo=Mongorcs, mongoparsed=Mongorcsp,  onlynew=False)
print('----RCS Parsed---')


print('reupdate of rcs list after parsing')
DF = Mongorcsp.find({'Forme juridique':{ '$regex' : '.*' + 'commandite spéciale' + '.*'}})
print(DF.shape)

DF2 = Mongoresa.find({"ns2:NumeroRCS":{ "$in": DF['RCS'].to_list()},
                     "month":{"$in":["Janvier", "Février"]}})
print(DF2.shape)
Mongorbe.insert_empty_RCS(DF2["ns2:NumeroRCS"].to_list(), update_existing=False)

# 4 - scrap RCS list in RBE: only the one in no ro new as changed RCS
print('----Scraping RBE---')
RBElist = scraper(type_='RBE', mongo=Mongorbe, to_be_updated=True)
print('----RBE Scraped---')

# 5 - parse RBE of RCS list
print('----Parsing RCS---')
rcs_parser(type_='rbe', mongo=Mongorbe, mongoparsed=Mongorbep,  onlynew=True)
print('----RCS Parsed---')