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


timer_main = performance_timer()

LBR_RCS_file_DF = pd.DataFrame()

RCSlist = Mongorcsp.get_RCSlist()
RCS_splited_lists = rcs_spliter(RCSlist, 10000)

for i, rcslist in enumerate(RCS_splited_lists):
    print(f"{i} on {len(RCS_splited_lists)}")
    N = publi_parser(RCS=rcslist, mongo=Mongopdf, mongoparsed=Mongopubli, onlynew=False)
    print(f'----financials parsed : {N}---')
    print(f"completed in {str(timer_main.stop())}s")
    #break

#RCSlist = rcslist

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


def cleanjusqua(x):
    if x!='':
        x = str(x)
        x = x.replace("jusqu'à", "jusqu à")
        x = eval(x)
    return x

immat_df['Gérant/Administrateur'] = immat_df['Gérant/Administrateur'].fillna('').apply(cleanjusqua)
immat_df['Délégué à la gestion journalière'] = immat_df['Délégué à la gestion journalière'].fillna('').apply(cleanjusqua)
immat_df['Personne(s) chargée(s) du contrôle des comptes'] = immat_df['Personne(s) chargée(s) du contrôle des comptes'].fillna('').apply(cleanjusqua)


immat_df.to_excel('update_15042022.xlsx', index=False)
immat_df.to_csv('update_15042022.csv', sep=';')
print(f"completed in {str(timer_main.stop())}s")




