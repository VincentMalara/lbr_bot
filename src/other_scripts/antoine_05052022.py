from src.merger.utils import *

from src.utils.timer import performance_timer
from src.mongo.main import mongo

import pandas as pd

timer_main = performance_timer()


Mongopubli = mongo(ip='146.59.152.231', db='LBR_test', col='publications')
Mongofinan = mongo(ip='146.59.152.231', db='LBR_test', col='financials')


Data = pd.read_excel('Neossys - Mandates or UBO - SCHUT Hille-Paul.xlsx')

LBR_RCS_file_DF = Mongopubli.find_from_RCSlist(RCS=Data['RCS number'].to_list())
financ = Mongofinan.find_from_RCSlist(RCS=Data['RCS number'].to_list())

financ = financ.sort_values(by=['RCS','year','correction','N_depot'])
financ = financ.groupby(by=['RCS']).agg('last').reset_index().rename(columns={'RCS':'RCS number'})
keptlist = ['RCS number','year','depot']
Data = Data.merge(financ[keptlist], on='RCS number', how='left')


LBR_RCS_file_DF['Date'] = pd.to_datetime(LBR_RCS_file_DF['Date'], format='%d/%m/%Y')
LBR_RCS_file_DF.sort_values(by='Date', ascending=True, inplace=True)
immat_df = LBR_RCS_file_DF.fillna('').groupby('RCS').agg(list)


todel = ['_id',  'Detail', 'lang', 'fr', 'de', 'readable',
               'donnees_a_modifier', 'missing', 're_splitted', 'Siège social',
               'Dénomination ou raison sociale', 'Durée', 'Forme juridique',
               'Date de constitution', 'Objet social',   'Type_de_depot',  'task_index',
                'Objet',
               'splitted_file_start', 'Capital social / Fonds social',
               'Enseigne(s) commerciale(s)', 'Date','depot','N_depot']

for label in todel:
    if label in immat_df.columns:
        immat_df.drop(columns=[label], inplace=True)

labelisttoclean = list_personne

list_col = []
for label_ in labelisttoclean:
    if label_ in immat_df.columns:
        immat_df[label_+ '_base'] = immat_df[label_].apply(clean_list)
        immat_df[label_]= immat_df[label_+ '_base'].apply(build_hist)
        list_col.append(label_)

immat_df = immat_df[list_col].reset_index()

SHP = 'Schut Hille-paul'

def checkSHP(x):
    y = ''
    if isinstance(x, list):
        for dict_ in x:
            if isinstance(dict_, dict):
                if 'name' in dict_.keys():
                    if str.lower(dict_['name']) == str.lower(SHP):
                        if 'date' in dict_.keys():
                            y = dict_['date']
    return y


for col in list_col:
    immat_df[col + '_SHP_date'] = immat_df[col].apply(checkSHP)



Data = Data.merge(immat_df.rename(columns={'RCS':'RCS number'}), on='RCS number', how='left')


Data.to_excel('Neossys - Mandates or UBO - SCHUT Hille-Paul_completed.xlsx')
