from collections import ChainMap

import pandas as pd

from configs import settings
from src.merger.utils import *
from src.utils.timer import performance_timer
from src.mongo.main import mongo
from src.utils.RCS_spliter import main as rcs_spliter


timer_main = performance_timer()


Mongorcs = mongo(ip='146.59.152.231', db='LBR_test', col='RCS')
Mongorbe = mongo(ip='146.59.152.231', db='LBR_test', col='RBE')
Mongorcsp = mongo(ip='146.59.152.231', db='LBR_test', col='RCS_parsed')
Mongorbep = mongo(ip='146.59.152.231', db='LBR_test', col='RBE_parsed')
Mongoresa = mongo(ip='146.59.152.231', db='LBR_test', col='RESA_parsed')
Mongopdf= mongo(ip='146.59.152.231', db='LBR_test', col='all_pdfs')
Mongopubli = mongo(ip='146.59.152.231', db='LBR_test', col='publications')
Mongofinan = mongo(ip='146.59.152.231', db='LBR_test', col='financials')

RCSlist = Mongorcs.get_RCSlist() #[0:100000]

try:
    RCS_output = pd.read_pickle('rcs_file.pkl')
except:
    RCS_list_DF = Mongorcsp.find_from_RCSlist(RCS=RCSlist)
    print(RCS_list_DF.shape)

    print(f"LBR RCS loaded, timer : {str(timer_main.stop())}s")
    RCS_output = pd.DataFrame()
    print('processing RCS, name, denom, register numb')
    RCS_output['RCS'] = RCS_list_DF['RCS']
    print('processing names')
    RCS_output['name'] = RCS_list_DF['company name'].apply(get_name)
    print(f"names processed, timer : {str(timer_main.stop())}s")
    RCS_output['Denomination'] = RCS_list_DF["Dénomination(s) ou raison(s) sociale(s)"]
    RCS_output['registerNumber'] = RCS_list_DF['RCS']
    print('processing year')
    RCS_output['yearOfCreation'] = RCS_list_DF["Date d'immatriculation"].apply(get_year)
    print('processing nace, forme juridique')
    RCS_output['classification'] = RCS_list_DF['Code NACE (Information mise à jour mensuellement)'].apply(get_nace)
    RCS_output['legalStatus'] = RCS_list_DF['Forme juridique']
    RCS_output['importStatus'] = "CREATE"
    print('processing nb subsi')
    RCS_output['numberOfSubsidiaries'] = RCS_list_DF['succursales'].apply(get_nb_sub)
    print('processing is active, is HQ')
    RCS_output['IsActive'] = RCS_list_DF['company status'].isnull()
    RCS_output['IsHQ'] = True
    print('processing address')
    RCS_output['Siège social'] = RCS_list_DF['Siège social']

    RCS_output['address_tbd'] = RCS_list_DF['Siège social'].apply(manage_adress2)
    RCS_output[['czip', 'city', 'address1']] = RCS_output['address_tbd'].apply(pd.Series)
    RCS_output['country'] = "Luxembourg"

    RCS_output['changed RCS number'] = RCS_list_DF['changed_RCS_number']
    try:
        RCS_output['Replaced by'] = RCS_list_DF['Replaced by']
    except:
        pass
    try:
        RCS_output['old RCS number'] = RCS_list_DF['old RCS']
    except:
        pass

    RCS_output['Dénonciation du contrat de domiciliation'] = RCS_list_DF['Dénonciation du contrat de domiciliation']

    RCS_output['is not Lux'] = RCS_output['name'].apply(is_succur)
    RCS_output['to be del'] = findtobedel(RCS_output)

    RCS_output.to_excel('rcs_file.xlsx', index=False)
    RCS_output.to_pickle('rcs_file.pkl')


#RBE

try:
    RBE_output = pd.read_pickle('rbe_file.pkl')
except:

    RBE_list_DF = Mongorbep.find_from_RCSlist(RCS=RCSlist)
    print(RBE_list_DF.shape)
    print('processing RBE')
    RBE_output = pd.DataFrame()
    RBE_output['RCS'] = RBE_list_DF['RCS']
    RBE_output['Loi_2004'] = RBE_list_DF['Loi_2004']
    RBE_output['UBO'] = RBE_list_DF['Benef Economiques'].apply(get_ubo)

    RBE_output['not registrated BO'] = RBE_list_DF['status'].apply(is_not_reg)

    RBE_output['UBO'] = RBE_output['UBO'].apply(cleanubo)

    print(f"RBE processed, timer : {str(timer_main.stop())}s")
    print('saving')
    RBE_output.to_excel('rbe_file.xlsx', index=False)
    RBE_output.to_pickle('rbe_file.pkl')
    print(f"RBE merged, timer : {str(timer_main.stop())}s")



FILE_LABEL_LIST = ['Modification',
                   'Modification non statutaire des mandataires',
                   'Immatriculation',
                   'Démission',
                   'Modification - Changement de la forme juridique',
                   'Inscription - Succursale',
                   'Modification - succursale']



# Admin and asso
try:
    immat_df = pd.read_pickle('adm_file.pkl')
except:
    print(f"Build company history starting on: {datetime.now()}")
    timer_main = performance_timer()

    # find all the SCSP companies from RCS parsed
    #RCSlist = Mongorcsp.get_RCSlist(dictin={"Forme juridique": {'$regex': '.*' + 'commandite spéciale' + '.*'}})
    print(len(RCSlist))

    RCS_splited_lists = rcs_spliter(RCSlist, 50000)
    LBR_RCS_file_DF=pd.DataFrame()

    for rcslist in RCS_splited_lists:
        print('----')
        print(len(rcslist))
        LBR_RCS_file_DFnew = Mongopubli.find_from_RCSlist(RCS=rcslist)
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
    immat_df.to_excel('adm_file.xlsx', index=False)
    immat_df.to_pickle('adm_file.pkl')
    print(f"ADM/ASSO done, timer : {str(timer_main.stop())}s")

try:
    bilan_DF_new = pd.read_pickle('financials.pkl')
except:
    RCS_splited_lists = rcs_spliter(RCSlist, 50000)
    Bilan_list_DF = pd.DataFrame()
    for rcslist in RCS_splited_lists:
        print('----')
        print(len(rcslist))
        Bilan_list_DF_new = Mongofinan.find_from_RCSlist(RCS=rcslist)
        if Bilan_list_DF_new.shape[0] > 0:
            Bilan_list_DF = pd.concat([Bilan_list_DF, Bilan_list_DF_new])
            print(Bilan_list_DF.shape)


    Bilan_list_DF = Bilan_list_DF.rename(columns={'depot': 'source'})
    Bilan_list_DF = Bilan_list_DF.rename(columns=dict_trad)


    collist = ['total_assets_liabilities', 'equity', 'result', 'debts',
               'capital', 'fixed_asset', 'revenues', 'margin',
               'liabilities', 'net_profitability_rate', 'return_on_equity', 'current_ratio',
               'working_capital', 'non_financials_debts', 'ebitda', 'gross_result',
               'finance_charges', 'financial_products', 'amortization_and_provisions',
               'longterm_debts', 'gross_operating_income', 'taxes', 'non_financials_assets',
               'result', 'working_capital_requirement', 'longterm_receivables', 'debt_to_equity']

    collist = collist + ['RCS', 'correction', 'source', 'year']
    bilan_DF_new = Bilan_list_DF[collist].copy()

    print('step assemble dicts')
    bilan_DF_new['metadata'] = bilan_DF_new[collist].fillna("").to_dict(orient='records')

    print(f"done in: {str(timer_main.stop())}s")
    print('format')

    bilan_DF_new['financials'] = bilan_DF_new['metadata'].apply(format_finan)
    bilan_DF_new.drop(columns=['metadata'], inplace=True)
    print(bilan_DF_new)
    print('---')


    print(f"done in: {str(timer_main.stop())}s")
    print('agg')


    bilan_DF_new = bilan_DF_new[[ 'RCS', 'correction', 'year', 'financials']].sort_values(by=['RCS', 'year', 'correction'], ascending=True)
    print(bilan_DF_new)
    print('---')

    bilan_DF_new = bilan_DF_new.groupby(['RCS', 'year']).agg('last').reset_index()
    print(bilan_DF_new)
    print('---')

    bilan_DF_new.drop(columns=['correction', 'year'], inplace=True)
    print(bilan_DF_new)
    print('---')

    bilan_DF_new = bilan_DF_new[bilan_DF_new['financials'].apply(lambda x: isinstance(x, list))]

    bilan_DF_new = bilan_DF_new.groupby('RCS').agg({'financials':sum}).reset_index()
    print(bilan_DF_new.shape)
    print(bilan_DF_new.head())


    print(f"financials processed, timer : {str(timer_main.stop())}s")
    print('saving')
    bilan_DF_new.to_excel('financials.xlsx', index=False)
    bilan_DF_new.to_pickle('financials.pkl')
    print(f"financials saved, timer : {str(timer_main.stop())}s")



print('Merging RBE')
RCS_output = pd.merge(RCS_output, RBE_output, how='left', on='RCS')
print('Merging ADM')
RCS_output = pd.merge(RCS_output, immat_df, how='left', on='RCS')
print('Merging financials')
RCS_output = pd.merge(RCS_output, bilan_DF_new, how='left', on='RCS')


RCS_output.to_excel('update_28032022.xlsx', index=False)
RCS_output.to_csv('update_28032022.csv', sep=';')

print(f"adm Merged, timer : {str(timer_main.stop())}s")



print(f"completed in {str(timer_main.stop())}s")
