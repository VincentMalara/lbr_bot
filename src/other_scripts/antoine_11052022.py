from src.merger.utils import *
from src.utils.timer import performance_timer
from src.mongo.main import mongo

import pandas as pd

timer_main = performance_timer()


Mongopubli = mongo(ip='146.59.152.231', db='LBR_test', col='publications')
publi = Mongopubli.find(dictout={'RCS': 1,
                                 "Gérant/Administrateur": 1,
                                 "Délégué à la gestion journalière": 1,
                                 "Actionnaire/Associé": 1,
                                 "Personne(s) chargée(s) du contrôle des comptes": 1,
                                 "Société de gestion": 1,
                                 '_id': 0})

def findHPS2(x):
    y = 0
    if isinstance(x, list):
        for i in x:
            if isinstance(i, dict):
                if 'name' in i.keys():
                    k=0
                    for name in ['hille','paul','schut']:
                        names = str.lower(i['name']).replace('-', ' ').replace(',', ' ')
                        names = names.split()
                        if name in names:
                            k+=1
                    if k > 2:
                        y = 1
                        break
                    elif k==2:
                        y = 0.5
                    else:
                        y = 0
    return y

list_personne = [
            "Gérant/Administrateur", "Délégué à la gestion journalière", "Actionnaire/Associé",
            "Personne(s) chargée(s) du contrôle des comptes", "Société de gestion"
                           ]

aggdict = {}
for label in list_personne:
    publi[label + '_isHPS'] = publi[label].apply(findHPS2)
    aggdict[label] = list
    aggdict[label + '_isHPS'] = max



publi = publi.groupby('RCS').agg(aggdict)

#publi.to_excel('tbd.xlsx')

Mongorbe = mongo(ip='146.59.152.231', db='LBR_test', col='RBE_parsed')

rbe = Mongorbe.find(dictout={'RCS': 1, 'Benef Economiques': 1, '_id':0})

def findHPS(x):
    y = 0
    if isinstance(x, list):
        for i in x:
            if isinstance(i, dict):
                if 'Nom, Prénom(s)' in i.keys():
                    k=0
                    for name in ['hille','paul','schut']:
                        names = str.lower(i['Nom, Prénom(s)']).replace('-', ' ').replace(',', ' ')
                        names = names.split()
                        if name in names:
                            k+=1
                    if k > 2:
                        y = 1
                        break
                    elif k==2:
                        y = 0.5
                    else:
                        y = 0
    return y


rbe['rbe_isHPS'] = rbe['Benef Economiques'].apply(findHPS)

#rbe.to_excel('tbd.xlsx')

publi = publi.merge(rbe, on='RCS', how='left')


def countHPS(row):
    y = False
    for name in [ "Gérant/Administrateur", "Délégué à la gestion journalière", "Actionnaire/Associé",
            "Personne(s) chargée(s) du contrôle des comptes", "Société de gestion", 'rbe']:
        if row[name+ '_isHPS'] > 0.5:
            y = True
            break
    return y


publi['isHPS'] = publi.apply(countHPS, axis=1)
publi = publi[publi['isHPS']].reset_index(drop=True)

Mongorcs = mongo(ip='146.59.152.231', db='LBR_test', col='RCS_parsed')
RCS_output = Mongorcs.find(dictin={'RCS': {'$in': publi['RCS'].to_list()}},
                           dictout={'RCS': 1, "Dénomination(s) ou raison(s) sociale(s)":1, 'company name': 1, '_id':0})

RCS_output['name'] = RCS_output['company name'].apply(get_name)
RCS_output['Denomination'] = RCS_output["Dénomination(s) ou raison(s) sociale(s)"]

RCS_output = RCS_output.merge(publi, on='RCS', how='left')

RCS_output = RCS_output[['RCS', 'name',	'Denomination',	'Gérant/Administrateur_isHPS', 'Délégué à la gestion journalière_isHPS',
            'Actionnaire/Associé_isHPS', 'Personne(s) chargée(s) du contrôle des comptes_isHPS', 'rbe_isHPS']]

for label in RCS_output.columns:
    if '_isHPS' in label:
        RCS_output.rename(columns={label + 'isHPS': label}, inplace=True)


RCS_output.to_excel('HillePaulSchut_RCS.xlsx')
