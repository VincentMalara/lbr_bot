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


#Mongorcs.set_status(newstatus='scraped',dictin={'status' : 'to_be_updated', 'extraction_date':{'$nin':["10/03/2022"]}})
#Mongorbe.set_status(newstatus='scraped',dictin={'status' : 'to_be_updated', 'extraction_date':{'$nin':["10/03/2022"]}})

'''
DF = Mongorcsp.find({'Forme juridique':{ '$regex' : '.*' + 'commandite spéciale' + '.*'}})
RCS_SCSP_exist = DF['RCS'].to_list()
print(len(RCS_SCSP_exist))
#print(Mongorcs.find_from_RCSlist(RCS_SCSP_exist))

DF1 = Mongoresa.find({"ns2:Prestation":"Immatriculation",
                      "ns2:NumeroRCS":{ '$regex' : '.*' + 'B' + '.*'},
                     "month":{"$in":["Janvier", "Février"]}})
RCS_B_created = DF1['ns2:NumeroRCS'].to_list()
print(len(RCS_B_created))
#print(Mongorcs.find_from_RCSlist(RCS_B_created))


tokeepklit =['Modification',
                   'Modification non statutaire des mandataires',
                   'Démission',
                   'Changement de la forme juridique',
                   'Inscription - Succursale',
                   'Modification - succursale']

def tokeep(x):
    y = False
    if isinstance(x, str):
        y = any(i.lower() in x.lower() for i in tokeepklit)
    return y

DF2 = Mongoresa.find({"ns2:NumeroRCS":{ "$in": RCS_SCSP_exist},
                     "month":{"$in":["Janvier", "Février"]}})

# keep only the one who add following modif
DF2 = DF2[DF2['ns2:Prestation'].apply(tokeep)].reset_index(drop=True)
RCS_SCSP_changed = DF2['ns2:NumeroRCS'].to_list()
print(len(RCS_SCSP_changed))
#print(Mongorcs.find_from_RCSlist(RCS_SCSP_changed))

RCSlist = RCS_SCSP_changed + RCS_B_created

print(len(RCSlist))

#Mongorcs.set_to_be_updated(RCS=RCSlist)
#Mongorbe.set_to_be_updated(RCS=RCSlist)
'''

#Mongorcs.set_to_be_updated(RCS=DF2["ns2:NumeroRCS"].to_list(), dictin={'extraction_date':{'$nin':['10/03/2022']}})
#Mongorcs.insert_empty_RCS(RCSlist,  update_existing=False)
RCSlist = Mongorcsp.get_RCSlist(dictin={"Forme juridique":{ '$regex' : '.*' + 'commandite spéciale' + '.*'},
                                        'Code NACE (Information mise à jour mensuellement)':{'$exists':False}})

print('----Scraping RCS---')
RCSlist_scr = scraper(type_='RCS', mongo=Mongorcs, to_be_updated=True)
print('----RCS Scraped---')


# 3 - parse RCS of RCS list
print('----Parsing RCS---')
rcs_parser(RCS=RCSlist, type='rcs', mongo=Mongorcs, mongoparsed=Mongorcsp,  onlynew=False)
print('----RCS Parsed---')

'''
# 4 - scrap RCS list in RBE: only the one in no ro new as changed RCS
print('----Scraping RBE---')
RBElist = scraper(type_='RBE', mongo=Mongorbe, to_be_updated=True)
print('----RBE Scraped---')

# 5 - parse RBE of RCS list
print('----Parsing RBE---')
rcs_parser(type_='rbe', mongo=Mongorbe, mongoparsed=Mongorbep,  onlynew=True)
print('----RCBE Parsed---')

RCSlist = Mongorcsp.get_RCSlist(dictin={"Forme juridique":{ '$regex' : '.*' + 'commandite spéciale' + '.*'}})
print(len(RCSlist))

RCS_splited_lists = rcs_spliter(RCSlist, 1000)

#RCS_splited_lists = ["B168088", "B174897", "B187247", "B196099" ]

for rcslist in RCS_splited_lists:
    # 6 - download new pdf not yet downloaded
    print('----Downloading pdfs---')
    #pdf_downloader(RCS=rcslist,mongo_rcsparsed=Mongorcsp, mongo_pdfs=Mongopdf) #always consider only new "depot"
    print('----pdfs Downloaded ---')
    print('----parsing publi---')
    publi_parser(RCS=rcslist, mongo=Mongopdf, mongoparsed=Mongopubli, onlynew=True)
    print('----publi parsed ---')


# 7 - parsing bilan from downloaded pdf
print('----parsing bilans---')
financials_parser(RCS=RCSlist,mongo=Mongopdf, mongoparsed=Mongofinan, onlynew=False)
print('----bilans parsed ---')


# 8 - parsing publi from downloaded pdf
'''

