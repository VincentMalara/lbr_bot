from src.utils.timer import performance_timer
from src.mongo.main import mongo
import pandas as pd

from src.utils.RCS_spliter import main as rcs_spliter

Mongorcs_old = mongo(db='LBR_test', col='RCS_old')
Mongorcs_server = mongo(db='LBR_test', col='RCS')



base_RCS_list = Mongorcs_old.find_from_RCSlist(RCS=RCSlist, only=True)
print(base_RCS_list)


base_RCS_list = Mongorcs_old.get_RCSlist()
alreadydone_RCS_list = Mongorcs_server.get_RCSlist()
print(len(base_RCS_list))
base_RCS_list = [rcs for rcs in base_RCS_list if rcs not in alreadydone_RCS_list]
print(len(base_RCS_list))


RCS_splited_lists = rcs_spliter(base_RCS_list, 30)
print(len(RCS_splited_lists))

timer_main = performance_timer()



for count, rcslist in enumerate(RCS_splited_lists):
    print('count :',count)
    print('rcslist :',len(rcslist))

    print('loading')
    data = Mongorcs_old.find_from_RCSlist(RCS=rcslist)
    print(f"loaded in {str(timer_main.stop())}s")

    print(data)
    if 'scrapper_version' in data.columns:
        data = data[['info', 'RCS', 'status', 'extraction_date', 'scrapper_version']]
        data.rename(columns={'scrapper_version': 'scraper_version'}, inplace=True)
        data['scraper_version'] = data['scraper_version'].fillna('0.0')
    else:
        data = data[['info', 'RCS', 'status', 'extraction_date']]
        data['scraper_version'] = '0.0'

    data['task_index']=-1
    data['status']=data['status'].replace({'scrapped':'scraped'})
    data['scraper_version'] = data['scraper_version'].astype(str)
    data['newdate'] = pd.to_datetime(data['extraction_date'], format='%d/%m/%Y')
    data.sort_values(by='newdate', ascending=False, inplace=True)
    data.drop_duplicates(subset='RCS',keep='first', inplace=True)
    print(data.shape)
    data.drop(columns=['newdate'], inplace=True)
    print(data.shape)
    print('processed')
    print(data.head())
    Mongorcs_server.insert(data)
    print(f"step {count} done")
    #Mongorcs_server.drop_duplicates(colsel='task_index', coldup='RCS')

print(f"completed in {str(timer_main.stop())}s")

'''
timer_main = performance_timer()


Mongorcs_old = mongo(db='LBR_new',  col='LBR_RCS_31052021')
Mongorcs_server = mongo(ip='146.59.152.231', db='LBR_test',  col='RCS')


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



print(f"completed in {str(timer_main.stop())}s")
'''