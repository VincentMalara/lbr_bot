from collections import ChainMap

import pandas as pd

from configs import settings
from src.merger.utils import *
from src.utils.timer import performance_timer
from src.mongo.main import mongo
from src.utils.RCS_spliter import main as rcs_spliter
from src.pdf_parsers.publications.main import main as publi_parser


timer_main = performance_timer()

from src.utils.timer import performance_timer
from src.mongo.main import mongo

import pandas as pd

timer_main = performance_timer()

Mongopdf= mongo(ip='146.59.152.231', db='LBR_test', col='all_pdfs')
Mongopubli = mongo(ip='146.59.152.231', db='LBR_test', col='publications')
Mongorcsp = mongo(ip='146.59.152.231', db='LBR_test', col='RCS_parsed')



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




timer_main = performance_timer()

LBR_RCS_file_DF=pd.DataFrame()

RCSlist = Mongorcsp.get_RCSlist()
RCS_splited_lists = rcs_spliter(RCSlist, 10000)

for i, rcslist in enumerate(RCS_splited_lists):
    print(f"{i} on {len(RCS_splited_lists)}")
    N = publi_parser(RCS=rcslist, mongo=Mongopdf, mongoparsed=Mongopubli, onlynew=False)
    print(f'----financials parsed : {N}---')
    print(f"completed in {str(timer_main.stop())}s")
    break

RCSlist = rcslist

LBR_RCS_file_DFnew = Mongopubli.find_from_RCSlist(RCS=RCSlist)
if LBR_RCS_file_DFnew.shape[0]>0:
    LBR_RCS_file_DFnew['Date'] = pd.to_datetime(LBR_RCS_file_DFnew['Date'], format='%d/%m/%Y')
    LBR_RCS_file_DF = pd.concat([LBR_RCS_file_DF, LBR_RCS_file_DFnew])
    print(LBR_RCS_file_DF.shape)


LBR_RCS_file_DF.sort_values(by='Date', ascending=True, inplace=True)
immat_df = LBR_RCS_file_DF.fillna('').groupby('RCS').agg(list)

print(immat_df)


todel = ['_id',  'Detail', 'lang', 'fr', 'de', 'readable',
       'donnees_a_modifier', 'missing', 're_splitted', 'Siège social',
       'Dénomination ou raison sociale', 'Durée', 'Forme juridique',
       'Date de constitution', 'Objet social',   'Type_de_depot',  'task_index',
        'Objet',
       'splitted_file_start', 'Capital social / Fonds social',
       'Enseigne(s) commerciale(s)', 'Date','depot','N_depot']

for label in todel:
    try:
        immat_df.drop(columns=[label], inplace=True)
    except Exception:
        pass

print(immat_df.columns)

labelisttoclean = list_personne

print(list_personne)
list_col = []
for label_ in labelisttoclean:
    if label_ in immat_df.columns:
        immat_df[label_+ '_base'] = immat_df[label_].apply(clean_list)
        immat_df[label_]= immat_df[label_+ '_base'].apply(build_hist)
        list_col.append(label_)


immat_df = immat_df[list_col].reset_index()
print('saving')
immat_df.to_excel('adm_file_.xlsx', index=False)
print(f"ADM/ASSO done, timer : {str(timer_main.stop())}s")





