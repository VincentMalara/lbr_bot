from configs import settings
from src.scrapers.main import main as scraper
from src.html_parsers.main import main as rcs_parser
from src.pdf_downloaders.main import main as pdf_downloader
from src.pdf_parsers.financials.main import main as financials_parser
from src.pdf_parsers.publications.main import main as publi_parser
from src.utils.RCS_spliter import main as rcs_spliter
from src.scrapers.resa.utils import Resa


from src.utils.timer import performance_timer
from src.mongo.main import mongo

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
RCSlist = Mongoresa.find({"month":{"$in":["Mars"]}, "year":{"$in":["2022"]}})
RCSlist = RCSlist['ns2:NumeroRCS'].to_list()
print(len(RCSlist))

'''
Mongorcs.set_status('scraped')
Mongorbe.set_status('scraped')

Mongorcs.insert_empty_RCS(RCS=RCSlist, update_existing=True)
Mongorbe.insert_empty_RCS(RCS=RCSlist, update_existing=True)
'''

print('----Parsing RCS---')
RCSlist_ =scraper(type_='RCS', mongo=Mongorcs, to_be_updated=True)
print('----RCS Parsed---')

print('----Parsing RCS---')
RBElist =scraper(type_='RBE', mongo=Mongorbe, to_be_updated=True)
print('----RCS Parsed---')


#1 - parse RCS
rcs_parser(RCS=RCSlist_,type_='rcs', mongo=Mongorcs, mongoparsed=Mongorcsp,  onlynew=False)

#2 - parse RBE
rcs_parser(RCS=RBElist,type_='rbe', mongo=Mongorbe, mongoparsed=Mongorbep,  onlynew=False)



RCS_splited_lists = rcs_spliter(RCSlist, 1000)

for rcslist in RCS_splited_lists:
    print('----Downloading pdfs---')
    pdf_downloader(RCS=rcslist,mongo_rcsparsed=Mongorcsp, mongo_pdfs=Mongopdf)
    print('----pdfs Downloaded ---')


#RCSlist = Mongorcs.get_RCSlist()
#RCS_splited_lists = rcs_spliter(RCSlist, 10000)

for i, rcslist in enumerate(RCS_splited_lists):
    print(f"{i} on {len(RCS_splited_lists)}")
    print('----parsing publi---')
    N = publi_parser(RCS=rcslist, mongo=Mongopdf, mongoparsed=Mongopubli, onlynew=False)
    print(f'----publi parsed : {N}---')
    print('----parsing financials ---')
    N = financials_parser(RCS=rcslist, mongo=Mongopdf, mongoparsed=Mongofinan, onlynew=False)
    print(f'----financials parsed : {N}---')
    print(f"completed in {str(timer_main.stop())}s")




print(f"completed in {str(timer_main.stop())}s")
