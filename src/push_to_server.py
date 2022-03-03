from .utils.timer import performance_timer
from src.mongo.main import mongo
from src.html_parsers.rcs.main import main as rcs_parser
from src.html_parsers.rbe.main import main as rbe_parser
import pandas as pd

timer_main = performance_timer()

RCS = [
'B158195',
'B158194',
'B195389',
'B113285',
'B62466',
'B13069',
'B190703',
'B77001',
'B63694',
'B79748',
'B225866',
'B113003',
'B70221',
'B254096',

]



Mongorcs_old = mongo(db='LBR_new',  col='LBR_RCS_31052021')
Mongorcs_server = mongo( db='LBR_test',  col='RCS')

print('loading')
data = Mongorcs_old.find_from_RCSlist(RCS=RCS)
print(f"loaded in {str(timer_main.stop())}s")

data = data[['info', 'RCS', 'status', 'extraction_date', 'scrapper_version']]
data.rename(columns={'scrapper_version':'scraper_version'}, inplace=True)
data['task_index']=-1
data['status']=data['status'].replace({'scrapped':'scraped'})
data['scraper_version'] = data['scraper_version'].astype(str)
print('processed')

print(data.head())

Mongorcs_server.insert(data)
Mongorcs_server.drop_duplicates(colsel='task_index', coldup='RCS')


print(f"completed in {str(timer_main.stop())}s")


timer_main = performance_timer()


Mongorcs_old = mongo(db='LBR_new',  col='LBR_RBE')
Mongorcs_server = mongo( db='LBR_test',  col='RBE')


print('loading')
data = Mongorcs_old.find_from_RCSlist(RCS=RCS)
print(f"loaded in {str(timer_main.stop())}s")

data = data[['info', 'RCS', 'status', 'extraction_date', 'scrapper_version']]
data.rename(columns={'scrapper_version':'scraper_version'}, inplace=True)
data['task_index']=-1
data['status']=data['status'].replace({'scrapped':'scraped'})
data['scraper_version'] = data['scraper_version'].astype(str)
print('processed')

print(data.head())

Mongorcs_server.insert(data)

'''
Mongorcs_server = mongo(ip='146.59.152.231', db='LBR_test',  col='RCS')
Mongorbe_server = mongo(ip='146.59.152.231', db='LBR_test',  col='RBE')

timer_main = performance_timer()
print('Mongorcs_server.drop_duplicates')
Mongorcs_server.drop_duplicates(colsel='task_index', coldup='RCS')
print('Mongorbe_server.drop_duplicates')
Mongorbe_server.drop_duplicates(colsel='task_index', coldup='RCS')
print('processed')



Mongorcs = mongo(ip='146.59.152.231', db='LBR_test',  col='RCS')
Mongorcsp = mongo(ip='146.59.152.231', db='LBR_test',  col='RCS_parsed')

print('----Parsing RCS---')
rcs_parser(mongo=Mongorcs, mongoparsed=Mongorcsp,  onlynew=False)
print('----RCS Parsed---')


Mongorbe = mongo(ip='146.59.152.231', db='LBR_test',  col='RBE')
Mongorbep = mongo(ip='146.59.152.231', db='LBR_test',  col='RBE_parsed')

print('----Parsing RCS---')
rbe_parser(mongo=Mongorbe, mongoparsed=Mongorbep,  onlynew=False)
print('----RCS Parsed---')

'''

print(f"completed in {str(timer_main.stop())}s")
