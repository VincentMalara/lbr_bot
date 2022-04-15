from datetime import datetime
import math
import re
import numpy as np


import pandas as pd
''''
NEWCOLUMNDICT = {
    'Numéro de RCS': "RCS",
    'Nom': "name",
    'Dénomination commerciale (si existante)': "Denomination",
    'Année de création': "yearOfCreation",
    "Date d'immatriculation": "Date d'immatriculation",
    'Type de société': "keyword",
    'Statut légal': "legalStatus",
    'Adresse': "address1",
    'Code postal': "czip",
    'Ville': "city",
    'Pays': "country",
    'Bénéficiaire Effectif': "UBO",
    "Autorisation d'établissement": 'Autorisation(s)',
    'Capital social / Fonds social':'Capital social / Fonds social',
    'Données personnelles - Indépendants': 'Données personnelles',
    'Objet': "Objet_new",
    'Associés commandités': 'Associé(s) commandité(s)',
    'Administrateurs': 'Administrateur(s) / Gérant(s)',
    'Associés': 'Associé(s)',
    'Associé(s) solidaire(s)':'Associé(s) solidaire(s)',
    'Personne(s) ayant le pouvoir d engager la société': "Personne(s) ayant le pouvoir d engager la société",
    'Représentant(s) permanent(s) pour l activité de la succursale': "Représentant(s) permanent(s) pour l activité de la succursale",
    'Président / directeur(s)': 'Président / directeur(s)',
    'Personne(s) autorisée(s) à gérer, administrer et signer':'Personne(s) autorisée(s) à gérer, administrer et signer',
    'Personne(s) chargée(s) du contrôle des comptes':'Personne(s) chargée(s) du contrôle des comptes',
    'Société de gestion':'Société de gestion',
    'Délégué(s) à la gestion journalière':'Délégué(s) à la gestion journalière'
}
'''

list_personne = [
            "Gérant/Administrateur", "Délégué à la gestion journalière", "Actionnaire/Associé",
            "Personne(s) chargée(s) du contrôle des comptes", "Société de gestion"
                           ]
PERSON_MERGED_REVERSED = {
    "Gérant/Administrateur":["Administrateur(s) / Gérant(s)",
                             "Représentant(s) permanent(s) pour l’activité de la succursale",
                             "Président / directeur(s)", "Gérant(s)"],
    "Délégué à la gestion journalière":["Délégué(s) à la gestion journalière", "Personne(s) ayant le pouvoir d’engager la société",
                                        "Personne(s) autorisée(s) à gérer, administrer et signer"],
    "Actionnaire/Associé":["Associé(s) solidaire(s)", "Associé(s) commandité(s)"],
    "Personne(s) chargée(s) du contrôle des comptes": ["Personne(s) chargée(s) du contrôle des comptes"],
    "Société de gestion": ["Société de gestion"]}

def get_list_of_modif(list_):
  output={}
  for label in list_personne:
    output[label] = {}
    output[label]['needed_full'] = 0

  if isinstance(list_, list):
    for depot in list_:
      if isinstance(depot, dict):
        #if depot['Type_de_depot'] in FILE_LABEL_LIST or True:
        for label in list_personne:
          if isinstance(depot['Detail'], str):
            for pmr in PERSON_MERGED_REVERSED[label]:
              if depot['Detail'].find(pmr) != -1:
                output[label]['needed_full'] +=1
                break

  return output

def clean_list(x):
    y = []
    for j in x:
        if j != '':
            y.append(j)
    if len(y)==0:
        y = 'empty'
    return y


def build_hist(x):
    y = pd.DataFrame()
    for j in x:
        if isinstance(j, list):
            for i in j:
                if isinstance(i, dict):
                    if i['status'] in ['nouveau']:
                        y = pd.concat([y, pd.DataFrame.from_records([i])])
                    if i['status'] in ['modifié']:
                        j = i.copy()
                        if 'date' in j.keys():
                            j['date'] = ''
                        y = pd.concat([y, pd.DataFrame.from_records([j])])
                    if i['status'] == 'rayé':
                        if y.shape[0] > 0:
                            y = y[y['name'] != i['name']].reset_index(drop=True)

    if y.shape[0]>0:
        #y.drop(columns=['status'], inplace=True)
        #y.drop_duplicates(subset='name', keep='last', inplace=True)
        dictagg = {key:'last' for key in y.columns if key not in ['name']}
        if 'date' in y.columns:
            dictagg['date'] = 'first'
        y = y.groupby('name').agg(dictagg).reset_index()

        #y = y.to_dict('records')
        y = y.to_dict(orient='records')
    else:
        y = ''
    return y


def build_hist_simple(x):
    y = pd.DataFrame()
    for j in x:
        if isinstance(j, list):
            for i in j:
                if isinstance(i, dict):
                    if i['status'] in ['modifié', 'nouveau']:
                        y = pd.concat([y, pd.DataFrame.from_records([i])])
                    if i['status'] == 'rayé':
                        if y.shape[0]>0:
                            y = y[y['name'] != i['name']].reset_index(drop=True)

    if y.shape[0]>0:
        #y.drop(columns=['status'], inplace=True)
        y.drop_duplicates(subset='name', keep='last', inplace=True)
        #y = y.to_dict('records')
        y = '; '.join(y['name'].to_list())
    else:
        y = ''
    return y

def clean_date(x):
    if x['Date']:
        y = x['Date']
    elif x['Date_publi']:
        y = x['Date_publi']
    else:
        y = ''
    return y

NEWCOLUMNDICT = {
    'Numéro de RCS': "RCS",
    'Nom': "name",
    'Dénomination commerciale (si existante)': "Denomination",
    'Année de création': "yearOfCreation",
    "Date d'immatriculation": "Date d'immatriculation",
    'Type de société': "keyword",
    'Statut légal': "legalStatus",
    'Adresse': "address1",
    'Code postal': "czip",
    'Ville': "city",
    'Pays': "country",
    'Bénéficiaire Effectif': "UBO",
    "Autorisation d'établissement": 'Autorisation(s)',
    'Capital social / Fonds social':'Capital social / Fonds social',
    'Données personnelles - Indépendants': 'Données personnelles',
    'Objet': "Objet_new",
    'Nom du fonds': 'Nom du fonds',
    'Actionnaire/Associé': 'Actionnaire/Associé',
    'Gérant/Administrateur': 'Gérant/Administrateur',
    'Délégué à la gestion journalière':'Délégué à la gestion journalière',
    'Personne(s) chargée(s) du contrôle des comptes': 'Personne(s) chargée(s) du contrôle des comptes',
    'Société de gestion': 'Société de gestion'
}



dict_trad = {
    "captiaux_propres": 'equity', #erreur de copie à capitaux, volontaire pour matcher avec les finacials
    "captital_souscrit": "capital",
    "creance_un an": "longterm_receivables",
    "dettes": "debts",
    "dettes_un_an": "longterm_debts",
    "fond_roulement": "working_capital",
    "passif_circulant": "liabilities",
    "ratio_liquidite_generale": "current_ratio",
    "resultat_net": "result",
    "total_actif_passif": "total_assets_liabilities",
    "ca_n": "revenues",
    "actif_circulant_non_financier_court_terme": "non_financials_assets",
    "actif_immobilisé": "fixed_asset",
    "amortissements_provisions": "amortization_and_provisions",
    "besoin_fond_roulement": "working_capital_requirement",
    "charge_financiere": "finance_charges",
    "dette_non_financier_court_terme": "non_financials_debts",
    "ebe": "gross_operating_income",
    "ebitda": "ebitda",
    "impot_taxe": "taxes",
    "marge_net": "margin",
    "produits_financiers": "financial_products",
    "resultat_brut": "gross_result",
    "taux_rentabilité_capitaux_propre": "return_on_equity",
    "taux_rentabilité_net": "net_profitability_rate",
    "tresorerie_net": "net_cash",
    "ratio_endettement":"debt_to_equity"
}

dict_legal_status = {
"A" : "Commerçant personne physique",
"B" : "Société commerciale",
"C" : "G.I.E.",
"D" : "G.E.I.E.",
"E" : "Société civile",
"F" : "Association sans but lucratif",
"G" : "Fondation",
"H" : "Association agricole",
"I" : "Association d'épargne-pension",
"J" : "Etablissement public",
"K" : "Fonds commun de placement",
"L" : "FIAR-art.10bis RGD du 23/01/2003",
"M" : "Mutuelles"
}

regexens = re.compile(r"^1 ")


def clean_enseigne(x):
    y = ''
    if isinstance(x, dict):
        if 'Enseigne(s) commerciale(s) Le cas échéant, abréviation(s) utilisée(s)' in x.keys():
            y = x['Enseigne(s) commerciale(s) Le cas échéant, abréviation(s) utilisée(s)']
            y = regexens.sub('', y)
    return y


def clean_objetnew2(row):
    y = ''
    for label in ['Objet social','Activités',	'Objet', 'Objet du commerce']:
        if label in row.keys():
            if isinstance(row[label], dict):
                if 'objet' in row[label].keys():
                    y = row[label]['objet']
                    break
    return y

def clean_objetnew(row):
    if row['Objet du commerce'] == '':
        y = row['Objet']
    else:
        y = row['Objet du commerce']
    return y


def extractnamelist(x):
    output = []
    if isinstance(x, list):
        for y in x:
            if isinstance(y, dict):
                if 'name' in y.keys():
                    output.append(y['name'])
    return output


def clean_empty_(list_):
    output=[]
    if isinstance(list_, list):
        for i in list_:
            if i not in ['', " ", ", ", " ,", " , "]:
                j = i.replace(',', ' ')
                k = j.split()
                k = [str(x).lower().capitalize() for x in k]
                if k[0].isdecimal():
                    k=k[1:]
                j = " ".join(k)
                output.append(j.strip())
    if len(output) == 0:
        output = ""
    return output

def clean_empty(x):
    y = x
    if isinstance(x, dict):
        if len(x.keys()) == 0:
            y = ''
    if isinstance(x, list):
        if len(x) == 0:
            y = ''
    return y

def clean_brackets(x):
    y = x
    output=[]
    if isinstance(x, list):
        for jj in x:
            if isinstance(jj, dict):
                output2=[]
                for key in jj.keys():
                    output2.append(f'{key} : {jj[key]}')
                output.append(', '.join(output2))
            else:
                output.append(jj)
        y = '; '.join(output)
    if isinstance(x, dict):
        if x == {}:
            y = ''
        else:
            for key in x.keys():
                output.append(f'{key} : {x[key]}')
            y = ', '.join(output)
    return y

def clean_objet(x):
    y = x
    if isinstance(x, list):
        y = ' '.join(x)
    return y

def clean_capitalize(list_):
    output= []
    y=''
    if isinstance(list_, list):
        for name in list_:
            aa= name
            if not re.search('B(\d)+', name):
                aa = ' '.join([xx.capitalize() for xx in name.split(' ')])
            output.append(aa)

        if len(output)>1:
            y = (', ').join(output)
        elif len(output)==1:
            y = output[0]

    return y

cleansarldict = {
    'S.C.S.': 'SCS',
    'S.C.SP.':'SCSp',
    'S.C.SP': 'SCSp',
    'S.C.Sp.': 'SCSp',
    'S.C.Sp': 'SCSp',
    'a.s.b.l':'ASBL',
    'S.à r.l.-s': 'SARL-S',
    'S.à r.l.-S': 'SARL-S',
    'S.a r.l.-S': 'SARL-S',
    'SARL-S': 'SARL-S',
    'SÀRL-S': 'SARL-S',
    'Sàrl-S': 'SARL-S',
    'S.A R.L.-S': 'SARL-S',
    'S.À R.L.-S': 'SARL-S',
    'Sàrl.-S' : 'SARL-S',
    'Sarl-s': 'SARL-S',
    'Sarl-S': 'SARL-S',
    'S.A.R.L': 'SARL',
    'sarl': 'SARL',
    'sàrl': 'SARL',
    'S.à r.l.': 'SARL',
    's.a.r.l.': 'SARL',
    'S.à r.l': 'SARL',
    's.a.r.l': 'SARL',
    'S.à.R.L.': 'SARL',
    'S.À.R.L.': 'SARL',
    'S.à.R.L': 'SARL',
    'S.À.R.L': 'SARL',
    'Sàrl.': 'SARL',
    'Sàrl': 'SARL',
    'S.àr.l.': 'SARL',
    'S.à.r.l.': 'SARL',
    'S.A': 'SA',
    'S.A.': 'SA',
    's.a': 'SA',
    's.a.': 'SA',
    'S.a': 'SA',
    'S.a.': 'SA',
    'SA.': 'SA'
}


def is_not_reg(x):
    return x == "not_registrated_BO"


def cleanubo(ubos):
    output = []
    if isinstance(ubos, list):
        for ubo in ubos:
            if 'name' in ubo.keys():
                name = ubo['name']
                if 'interests' in ubo.keys():
                    interests = ubo['interests']
                    name = name + ' (' + interests + ')'
                output.append(name)

    return '; '.join(output)


def clean_sarl(x):
    y = x
    if isinstance(x, str):
        for i in cleansarldict.keys():
            if y.find(' ' + i) != -1:
                y = y.replace(' ' + i, ' ' + cleansarldict[i])
                break
    return y


def findtobedel(row):
    y = False
    if 'is not Lux' in row.keys():
        if row['is not Lux']:
            y = True
    if 'changed RCS number' in row.keys():
        if row['changed RCS number'] == "old_one":
            y = True
    return y


def replaced_RCS(x):
    y = ""
    if isinstance(x, str):
        if " Le numéro RCS saisi a été remplacé par le numéro RCS" in x:
            y = x.split(" Le numéro RCS saisi a été remplacé par le numéro RCS ")[1]
            regex = r'[ABCDEFGHIJKLM]\d+'
            bb = re.findall(regex, x)
            if len(bb) > 0:
                y = bb[0]
    return y


def is_succur(x):
    y = False
    if isinstance(x, str):
        if "succursale" in str.lower(x) and "luxembourgeoise" in str.lower(x):
            y = True
    return y


def firstletter_as_KW(x):
    if isinstance(x, str):
        if x[0] in dict_legal_status.keys():
            y = dict_legal_status[x[0]]
        else:
            y = ""
    else:
        y = ""
    return y


labels = dict_trad.values()


def format_finan(dict_in):
    output = []
    year = dict_in['year']
    source = dict_in['source']
    for label in labels:
        try:
            if dict_in[label] != '':
                dict_ = {}
                dict_['type'] = label
                dict_['value'] = dict_in[label]
                dict_['year'] = year
                dict_['source'] = source
                dict_['currency'] = "EUR"
                output.append(dict_)
        except Exception:
            pass
    if len(output) == 0:
        output = ''

    return output




def extract_bilan_value(DF):
    output = pd.DataFrame(columns=['RCS', 'financials'])

    for RCS in DF['RCS'].value_counts().index:
        AA = DF[DF['RCS']==RCS].sort_values(by=['year','correction'], ascending=False).groupby(['year']).agg('last').reset_index()
        BB = AA.to_dict('records')
        financials=[]
        for bb in BB:
            for key in bb.keys():
                if key not in ['year', '_id', 'correction', 'RCS', 'depot', 'type compte resultat', 'type bilan']:
                    if not str.isnumeric(key):
                        if not math.isnan(bb[key]):
                            dict_={}
                            dict_['year'] = bb['year']
                            dict_['source'] = bb['depot']
                            dict_['currency'] = "EUR"

                            try:
                                dict_['type'] = dict_trad[key]
                            except Exception:
                                print('-------------')
                                print(key)
                                print('-------------')
                                dict_['type'] = key

                            dict_['value'] = bb[key]
                            financials.append(dict_)


        dictout={}
        dictout['RCS'] = RCS
        dictout['financials'] = financials
        output = output.append(dictout, ignore_index=True)

    return output


def get_ubo(UBOs):
    ubolist = []
    if isinstance(UBOs, list):
        for ubo in UBOs:
            Dict_ = {}
            j = ubo['Nom, Prénom(s)'].split(',')
            fn = ' '.join([str(x).lower().capitalize() for x in j[1].split()])
            ln = ' '.join([str(x).lower().capitalize() for x in j[0].split()])
            Dict_['firstname'] = fn.strip()
            Dict_['lastname'] = ln.strip()
            Dict_['name'] = ' '.join([ln, fn])
            if 'Nature des intérêts (Etendue)'in ubo.keys():
                Dict_['interests'] = ubo['Nature des intérêts (Etendue)']
            if 'Fonction' in ubo.keys():
                Dict_['title'] = ubo['Fonction']
            if 'Nationalité(s)' in ubo.keys():
                Dict_['nationality'] = ubo['Nationalité(s)']
            if 'Pays de résidence' in ubo.keys():
                Dict_['country'] = ubo['Pays de résidence']
            if 'Date et lieu de naissance' in ubo.keys():
                birth = ubo['Date et lieu de naissance'].split('à')
                if '/' in birth[0]:
                    Dict_['birthdate'] = birth[0]
                if '/' not in birth[1]:
                    Dict_['birthplace'] = birth[1]
            ubolist.append(Dict_)
        if len(ubolist) == 0:
            ubolist = ""
    else:
        ubolist = ""
    return ubolist


def get_ubo_old(UBOs):
    ubolist = []
    if isinstance(UBOs, list):
        for ubo in UBOs:
            Dict_ = {}
            j = ubo['Nom, Prénom(s)'].replace(',', ' ')
            k = j.split()
            k = [str(x).lower().capitalize() for x in k]
            j = " ".join(k)
            Dict_['name'] = j.strip()
            if  'Nature des intérêts (Etendue)'in ubo.keys():
                Dict_['interests'] = ubo['Nature des intérêts (Etendue)']
            elif 'Fonction' in ubo.keys():
                Dict_['title'] = ubo['Fonction']
            else:
                pass
            ubolist.append(Dict_)

        if len(ubolist)==0:
            ubolist = ""
    else:
        ubolist = ""
    return ubolist


def get_ubo_old1(DF):
    Outup_df = pd.DataFrame(columns=['RCS', 'UBO', 'Loi_2004'])
    for index, row in DF.iterrows():
        ubolist = []
        dict_out = {}
        if isinstance(row['Benef Economiques'], list):
            for ubo in row['Benef Economiques']:
                Dict_ = {}

                j = ubo['Nom, Prénom(s)'].replace(',', ' ')
                k = j.split()
                k = [str(x).lower().capitalize() for x in k]
                j = " ".join(k)
                Dict_['name'] = j.strip()

                if  'Nature des intérêts (Etendue)'in ubo.keys():
                    Dict_['interests'] = ubo['Nature des intérêts (Etendue)']
                elif 'Fonction' in ubo.keys():
                    Dict_['title'] = ubo['Fonction']
                else:
                    pass
                ubolist.append(Dict_)
            dict_out['UBO'] = ubolist
        else:
            dict_out['UBO'] = ""
        dict_out['RCS'] = row['RCS']
        dict_out['Loi_2004'] = row['Loi_2004']
        Outup_df = Outup_df.append(dict_out, ignore_index=True)

    return Outup_df



def get_nb_sub(x):
    if isinstance(x, list):
        return len(x)
    else:
        return 0

def get_year(x):
    try:
        date_object = datetime.strptime(x, "%d/%m/%Y")
        Y = date_object.year
        return str(int(Y))
    except:
        return ""



def manage_adress2(x):
    output={}
    if isinstance(x, str) and 'L -' in x:
        AAA = x.split('L -')[-1]

        #regex = r'\d{1,2}\/\d{2}\/\d{4}'
        regex = r'\d{2,4}\ *\)'
        bb = re.findall(regex, AAA)

        if len(bb)>0:
            AAA = AAA.split('(')[0]

        regex = r'\d{2,5}'
        bb = re.findall(regex, AAA)
        if bb:
            output['czip'] = bb[-1]
            output['city'] = AAA.split(bb[-1])[-1].strip()
        else:
            output['czip'] = ''
            output['city'] = AAA.strip()


        output['address1'] = x.split('L -')[0].strip()

    else:
        output['czip'] = ''
        output['city'] = ''
        if x=='Aucune':
            x = ""
        output['address1'] = x

    #if output['city'] == ')':
     #   print(output)
      #  print(x)
      #  print(AAA)


    return output



def manage_adress(DF):
    OutputDF = pd.DataFrame()
    DF['Siège social'] = DF['Siège social'].fillna("")
    DF["Adresse où s'exerce l'activité commerciale"] = DF["Adresse où s'exerce l'activité commerciale"].fillna("")
    for index, row in DF.iterrows():
        output = {}
        if row['Siège social']:
            address = row['Siège social']
        elif row["Adresse où s'exerce l'activité commerciale"]:
            address = row["Adresse où s'exerce l'activité commerciale"]
        else:
            address = ""

        if address and 'L -' in address:
            AAA = address.split('L -')[-1]
            for ttd in ['(dénoncé le', '(siège dénoncé', '(Dénoncé', '(aufgekündigt', '(abgekündigt ',
                        '(aufgekünkigt', '(gekündigt', '(aufgekünfigt','(Aufgekündigt' ]:
                if ttd in AAA:
                    AAA = AAA.split(ttd)[0]
            regex = r'\d{2,5}'
            bb = re.findall(regex, AAA)
            if bb:
                output['czip'] = bb[-1]
                output['city'] = AAA.split(bb[-1])[-1].strip()
            else:
                output['czip'] = ''
                output['city'] = AAA.strip()


            output['address1'] = address.split('L -')[0].strip()

        else:
            output['czip'] = ''
            output['city'] = ''
            if address=='Aucune':
                address = ""
            output['address1'] = address

        OutputDF = OutputDF.append(output, ignore_index=True)
    return OutputDF

def manage_adress_succur(DF):
    OutputDF = pd.DataFrame()
    DF['address'] = DF['address'].fillna("")
    for index, row in DF.iterrows():
        output = {}
        if row['address']:
            address = row['address']
        else:
            address = ""

        if address:
            regex = r'\d{4}'
            bb = re.findall(regex, address.split('L -')[-1])
            if bb:
                output['czip'] = bb[-1]
                output['city'] = address.split('L -')[-1].split(bb[-1])[-1].strip()
            else:
                output['czip'] = ''
                output['city'] = address.split('L -')[-1].strip()

            output['address1'] = address.split('L -')[0].strip()

        else:
            output['czip'] = ''
            output['city'] = ''
            output['address1'] = address

        OutputDF = OutputDF.append(output, ignore_index=True)
    return OutputDF


def get_name_old(DF):
    DF['company name'] = DF['company name'].fillna("")
    DF['Nom'] = DF['Nom'].fillna("")
    DF['Prénom(s)'] = DF['Prénom(s)'].fillna("")
    OutputDF=pd.DataFrame()
    for index, row in DF.iterrows():
        output={}
        if row['company name']:
            company_name = row['company name']
        elif row['Nom'] and row['Prénom(s)']:
            nomprenom = row['Nom'] + ', ' + row['Prénom(s)']

            company_name = (nomprenom).lower()
            company_name = company_name.capitalize()
        else:
            company_name = ""
        output['name'] = company_name
        output['RCS'] = row['RCS']
        OutputDF = OutputDF.append(output, ignore_index=True)
    return OutputDF

def get_name(x):
    y=x
    if isinstance(x, str):
        if "Nom : " in x and "Prénom(s) : " in x:
            nom = x.split("Prénom(s) : ")[0].replace("Nom : ", '').strip().lower().capitalize()
            prenom = x.split("Prénom(s) : ")[1].strip().lower().capitalize()
            y = (', ').join([nom, prenom])
    return y

def get_nace(x):
    y = ''
    if isinstance(x, str):
        regex = r'\d{2}.\d+'
        bb = re.findall(regex, x)
        if bb:
            y = bb[0].replace(".","")
    return y


def checkaddress_k(x):
    y=''
    if isinstance(x, str):
        if 'Employees' in x:
            y = x.replace('Employees' , '').strip()
    return y

def clean_short_desc_K(x):
    y=''
    if isinstance(x, str):
        y = x.replace('See the Kompass classification', '').strip()
        y = y.replace('\n', '').strip()
    return y


def manage_adress_pj(DF):
    DF['address-street'] = DF['address-street'].fillna("")
    DF['address-city_street'] = DF['address-city_street'].fillna("")
    regex = r'\d{4}'
    OutputDF = pd.DataFrame()
    for index, row in DF.iterrows():
        output = {}
        czip=""
        city=""
        if row['address-street']:
            street = row['address-street']
            if row['address-city_street']:
                cityzip=row['address-city_street'].replace(street, '').strip()
                bb = re.findall(regex, cityzip)
                if bb:
                    czip = bb[0]
                    city = cityzip.replace(czip, '').strip()

        output['czip'] = czip
        output['city'] = city
        OutputDF = OutputDF.append(output, ignore_index=True)
    return OutputDF

def get_nace_K(x):
    y = ''
    if isinstance(x, str):
        regex = r'\d+'
        bb = re.findall(regex, x)
        if bb:
            y = bb[0]
    return y

def clean_sitelink_K(x):
    y=''
    if isinstance(x, str):
        if 'Supplier of:' in x:
            Y = x.split('Supplier of:')[-1].strip()
            #y = ' | '.join(Y.split('\n'))
            y=re.sub("( )+", " ", Y)
            y=re.sub(r'(\n)+', ' | ', y)
            y = y.replace(' |   | ', ' |')
    return y

def clean_sitelink_K2(x):
    y=''
    if isinstance(x, str):
        if 'Secondary activities within the  Kompass classification' in x:
            y = x.split('Secondary activities within the  Kompass classification')[-1].strip()
            y = y.replace('\t', '')
            y = [x.strip() for x in y.splitlines()]
            y= (' | ').join([x for x in y if x])
    return y


def manage_adress_wedo(DF):
    OutputDF = pd.DataFrame()
    DF['address'] = DF['address'].fillna("")
    for index, row in DF.iterrows():
        output = {}
        address = row['address']


        if address:
            if '(' in address:
                address = address.split('(')[0]

            regex = r'\d{4}'
            bb = re.findall(regex, address.split(', L-')[-1])
            if bb:
                output['czip'] = bb[-1]
                output['city'] = address.replace(', L-', '').split(bb[-1])[-1].replace(',', '').strip()
                output['address1'] = address.replace(bb[-1],'').split(', L-')[0].replace(',', '').strip()
            else:
                output['czip'] = ''
                output['city'] = address.split(', L-')[-1].replace(',', '').strip()
                output['address1'] = address.split(', L-')[0].replace(',', '').strip()



        else:
            output['czip'] = ''
            output['city'] = ''
            output['address1'] = address



        OutputDF = OutputDF.append(output, ignore_index=True)
    return OutputDF

def getyearwedo(x):
    y=''
    if isinstance(x, str):
        if '/' in x:
            aa = x.split('/')
            y=aa[-1]
        else:
            y=x
    return y

def getkw_wedo(DF):
    DF['activities'] = DF['activities-and-services'].fillna("")
    DF['products'] = DF['products-and-brands'].fillna("")
    outputlist=[]
    for index, row in DF.iterrows():
        output =['Artisan']
        act=""
        prod=""
        if row['activities']:
            act=(' | ').join([x.strip() for x in row['activities'].split(';')])
            output.append(act.strip())
        if row['products']:
            prod = (' | ').join([x.strip().lower().capitalize() for x in row['products'].split('\n')])
            output.append(prod.strip())

        bb=(' | ').join(output)
        outputlist.append(bb.replace(' | | ', ' | '))
    return outputlist
