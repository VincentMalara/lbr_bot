from .utils.timer import performance_timer
from src.mongo.main import mongo
import pandas as pd

from src.utils.RCS_spliter import main as rcs_spliter

Mongo_old_bilan = mongo(ip='146.59.152.231',db='LBR_test', col='all_pdf_old')
Mongo_allpdfs = mongo(ip='146.59.152.231', db='LBR_test', col='all_pdfs')


Mongo_old_bilan_DF = Mongo_old_bilan.find(dictin={}, dictout={'N_depot': 1, '_id': 0})
Mongo_old_bilan_depot = Mongo_old_bilan_DF['N_depot'].unique().tolist()

Mongo_allpdfs_bilan_DF = Mongo_allpdfs.find(dictin={}, dictout={'N_depot': 1, '_id': 0})
Mongo_new_bilan_depot = Mongo_allpdfs_bilan_DF['N_depot'].unique().tolist()

print(len(Mongo_old_bilan_depot))

base_depot_list = Mongo_old_bilan_DF[~Mongo_old_bilan_DF['N_depot'].isin(Mongo_new_bilan_depot)]['N_depot'].to_list()

print(len(base_depot_list))

#'_id', 'N_depot', 'Date', 'Type_de_depot', 'Detail', 'depot', 'RCS','file', 'extraction_date', 'task_index'


depot_splited_lists = rcs_spliter(base_depot_list, 1000)
print(len(depot_splited_lists))

timer_main = performance_timer()


for count, depotlist in enumerate(depot_splited_lists):
    print('count :',count)
    print('depotlist :',len(depotlist))

    print('loading')
    data = Mongo_old_bilan.find(dictin={'N_depot': {'$in': depotlist }})
    print(f"loaded in {str(timer_main.stop())}s")

    print(data)
    data = data[['N_depot', 'Date', 'Type_de_depot', 'Detail', 'depot', 'RCS','file', 'extraction_date']]
    data['task_index']=-1
    data['newdate'] = pd.to_datetime(data['extraction_date'], format='%d/%m/%Y')
    data.sort_values(by='newdate', ascending=False, inplace=True)
    data.drop_duplicates(subset='N_depot',keep='first', inplace=True)
    print(data.shape)
    data.drop(columns=['newdate'], inplace=True)
    print(data.shape)
    print('processed')
    print(data.head())
    Mongo_allpdfs.insert(data, col='N_depot')
    print(f"step {count} done")
    #Mongorcs_server.drop_duplicates(colsel='task_index', coldup='RCS')

print(f"completed in {str(timer_main.stop())}s")