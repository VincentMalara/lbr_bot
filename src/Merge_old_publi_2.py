from configs import settings
from src.pdf_downloaders.utils import PdfDownloader, check_temp_exist
from src.mongo.main import mongo
from .utils.timer import performance_timer
from src.utils.RCS_spliter import main as rcs_spliter

import pandas as pd

check_temp_exist() #needed in case temp folder is not in the project


Mongo_publi_old = mongo(ip='146.59.152.231', db='LBR_test', col='publi_old')
#Mongo_publi_old = mongo(db='LBR_new', col='LBR_Adm_and_Asso')

Mongorcsp = mongo(ip='146.59.152.231', db='LBR_test', col='RCS_parsed')

Mongo_allpdfs = mongo(ip='146.59.152.231', db='LBR_test', col='all_pdfs')

base_RCS_list = Mongorcsp.get_RCSlist()

RCS_splited_lists = rcs_spliter(base_RCS_list, 10000)
print(len(RCS_splited_lists))

timer_main = performance_timer()



for count, rcslist in enumerate(RCS_splited_lists):
    print('count :',count)
    print('rcslist :',len(rcslist))
    print(f"building pdf list")
    pdfdownloader = PdfDownloader(RCS=rcslist, mongo_rcsparsed=Mongorcsp, mongo_pdfs=Mongo_allpdfs)
    pdf_list = pdfdownloader.get_pdfs_list()
    print(pdf_list)
    print(' - - ')


    DF = Mongo_publi_old.find(dictin={'N_depot': {'$in': pdf_list['N_depot'].unique().tolist() }},
                         dictout={'N_depot': 1,'file':1,'extraction_date':1 , '_id': 0})

    print(DF.shape)
    if DF.shape[0]>0:
        DF['newdate'] = pd.to_datetime(DF['extraction_date'], format='%d/%m/%Y')
        DF.sort_values(by='newdate', ascending=False, inplace=True)
        DF.drop_duplicates(subset='N_depot',keep='first', inplace=True)
        print(DF.shape)
        DF.drop(columns=['newdate'], inplace=True)


        DF2 = pdf_list[pdf_list['N_depot'].isin(DF['N_depot'].to_list())].reset_index(drop=True)

        DF = DF.merge(DF2, on ='N_depot', how='left')

        DF = DF[DF['file'].notna()].reset_index(drop=True)


        Mongo_allpdfs.insert(DF, col='N_depot')


