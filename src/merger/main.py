from collections import ChainMap

import pandas as pd

from configs import settings
from src.merger.utils import *
from src.utils.timer import performance_timer
from src.mongo.main import mongo
from src.utils.RCS_spliter import main as rcs_spliter


timer_main = performance_timer()



def main(Mongorcs, Mongorbe, Mongorcsp, Mongorbep, Mongopdf, Mongopubli, Mongofinan, rcs_list):

    RCS_list_DF = Mongorcsp.find_from_RCSlist(RCS=rcs_list)

    if 'old RCS' in RCS_list_DF.columns:
        print('adding changed RCS')
        additonalRCS = RCS_list_DF[RCS_list_DF['old RCS'].notna()]['old RCS'].unique().tolist()
        rcs_list = rcs_list + additonalRCS

    RCS_list_DF = Mongorcsp.find_from_RCSlist(RCS=rcs_list)

    if RCS_list_DF.shape[0] > 0:
        RCS_output = pd.DataFrame()
        RCS_output['RCS'] = RCS_list_DF['RCS']
        RCS_output['name'] = RCS_list_DF['company name'].apply(get_name)
        RCS_output['Denomination'] = RCS_list_DF["Dénomination(s) ou raison(s) sociale(s)"]
        RCS_output['registerNumber'] = RCS_list_DF['RCS']
        RCS_output['yearOfCreation'] = RCS_list_DF["Date d'immatriculation"].apply(get_year)
        RCS_output['classification'] = RCS_list_DF['Code NACE (Information mise à jour mensuellement)'].apply(get_nace)
        RCS_output['legalStatus'] = RCS_list_DF['Forme juridique']
        RCS_output['importStatus'] = "CREATE"

        #RCS_output['numberOfSubsidiaries'] = RCS_list_DF['succursales'].apply(get_nb_sub)
        RCS_output = calc_if_exist(RCS_list_DF, RCS_output, 'succursales', 'numberOfSubsidiaries', function=get_nb_sub)

        try:
            RCS_output['IsActive'] = RCS_list_DF['company status'].isnull() #to be replaced by calc_if_exist with apply pd.isnull())
        except Exception:
            pass

        RCS_output['IsHQ'] = True
        #RCS_output['Siège social'] = RCS_list_DF['Siège social']
        RCS_output = calc_if_exist(RCS_list_DF, RCS_output, 'Siège social',
                                   'Siège social', function=None)
        #RCS_output['address_tbd'] = RCS_list_DF['Siège social'].apply(manage_adress2)
        RCS_output = calc_if_exist(RCS_list_DF, RCS_output,'Siège social', 'address_tbd',
                                   function=manage_adress2)
        RCS_output[['czip', 'city', 'address1']] = RCS_output['address_tbd'].apply(pd.Series)
        RCS_output.drop(columns=['address_tbd'], inplace=True)
        RCS_output['country'] = "Luxembourg"
        RCS_output['changed RCS number'] = RCS_list_DF['changed_RCS_number']

        #RCS_output['Replaced by'] = RCS_list_DF['Replaced by']
        RCS_output = calc_if_exist(RCS_list_DF, RCS_output, 'Replaced by', 'Replaced by', function=None)

        #RCS_output['old RCS number'] = RCS_list_DF['old RCS']
        RCS_output = calc_if_exist(RCS_list_DF, RCS_output,'old RCS', 'old RCS number', function=None)

        #RCS_output['Dénonciation du contrat de domiciliation'] = RCS_list_DF['Dénonciation du contrat de domiciliation']
        RCS_output = calc_if_exist(RCS_list_DF, RCS_output, 'Dénonciation du contrat de domiciliation',
                                   'Dénonciation du contrat de domiciliation', function=None)

        #RCS_output['is not Lux'] = RCS_output['name'].apply(is_succur)
        RCS_output = calc_if_exist(RCS_output, RCS_output, 'name',
                                   'is not Lux', function=is_succur)


        RCS_output['to be del'] = RCS_output.apply(findtobedel, axis=1)

        #RBE
        RBE_list_DF = Mongorbep.find_from_RCSlist(RCS=rcs_list)
        RBE_output = pd.DataFrame()
        RBE_output['RCS'] = RBE_list_DF['RCS']
        RBE_output['Loi_2004'] = RBE_list_DF['Loi_2004']
        RBE_output['UBO'] = RBE_list_DF['Benef Economiques'].apply(get_ubo)
        RBE_output['not registrated BO'] = RBE_list_DF['status'].apply(is_not_reg)
        #RBE_output['UBO'] = RBE_output['UBO'].apply(cleanubo)

        FILE_LABEL_LIST = ['Modification',
                           'Modification non statutaire des mandataires',
                           'Immatriculation',
                           'Démission',
                           'Modification - Changement de la forme juridique',
                           'Inscription - Succursale',
                           'Modification - succursale']


        # Admin and asso
        RCS_splited_lists = rcs_spliter(rcs_list, 50000)
        LBR_RCS_file_DF=pd.DataFrame()
        for sub_rcs_list in RCS_splited_lists:
            LBR_RCS_file_DFnew = Mongopubli.find_from_RCSlist(RCS=sub_rcs_list)
            if LBR_RCS_file_DFnew.shape[0] > 0:
                LBR_RCS_file_DFnew['Date'] = pd.to_datetime(LBR_RCS_file_DFnew['Date'], format='%d/%m/%Y', errors='coerce')
                LBR_RCS_file_DFnew['Date'] = LBR_RCS_file_DFnew['Date'].fillna('empty')
                LBR_RCS_file_DFnew = LBR_RCS_file_DFnew[LBR_RCS_file_DFnew['Date'] != 'empty'].reset_index(drop=True)
                LBR_RCS_file_DF = pd.concat([LBR_RCS_file_DF, LBR_RCS_file_DFnew])
                #print(LBR_RCS_file_DF.shape)

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

        #print(immat_df.columns)

        labelisttoclean = list_personne

        #print(list_personne)
        list_col = []
        for label_ in labelisttoclean:
            if label_ in immat_df.columns:
                immat_df[label_+ '_base'] = immat_df[label_].apply(clean_list)
                immat_df[label_]= immat_df[label_+ '_base'].apply(build_hist)
                list_col.append(label_)

        immat_df = immat_df[list_col].reset_index()

        #financials
        RCS_splited_lists = rcs_spliter(rcs_list, 50000)
        Bilan_list_DF = pd.DataFrame()
        for rcslist in RCS_splited_lists:
            #print('----')
            #print(len(rcslist))
            Bilan_list_DF_new = Mongofinan.find_from_RCSlist(RCS=rcslist)
            if Bilan_list_DF_new.shape[0] > 0:
                Bilan_list_DF = pd.concat([Bilan_list_DF, Bilan_list_DF_new])
                #print(Bilan_list_DF.shape)

        Bilan_list_DF = Bilan_list_DF.rename(columns={'depot': 'source'})
        Bilan_list_DF = Bilan_list_DF.rename(columns=dict_trad)


        collist = ['total_assets_liabilities', 'equity', 'result', 'debts',
                   'capital', 'fixed_asset', 'revenues', 'margin',
                   'liabilities', 'net_profitability_rate', 'return_on_equity', 'current_ratio',
                   'working_capital', 'non_financials_debts', 'ebitda', 'gross_result',
                   'finance_charges', 'financial_products', 'amortization_and_provisions',
                   'longterm_debts', 'gross_operating_income', 'taxes', 'non_financials_assets',
                   'working_capital_requirement', 'longterm_receivables', 'debt_to_equity']


        collist = collist + ['RCS', 'correction', 'source', 'year']
        bilan_DF_new = Bilan_list_DF[collist].copy()
        bilan_DF_new['metadata'] = bilan_DF_new[collist].fillna("").to_dict(orient='records')
        bilan_DF_new['financials'] = bilan_DF_new['metadata'].apply(format_finan)
        bilan_DF_new.drop(columns=['metadata'], inplace=True)


        bilan_DF_new = bilan_DF_new[[ 'RCS', 'correction', 'year', 'financials']].sort_values(by=['RCS', 'year', 'correction'], ascending=True)
        bilan_DF_new = bilan_DF_new.groupby(['RCS', 'year']).agg('last').reset_index()
        bilan_DF_new.drop(columns=['correction', 'year'], inplace=True)
        bilan_DF_new = bilan_DF_new[bilan_DF_new['financials'].apply(lambda x: isinstance(x, list))]
        bilan_DF_new = bilan_DF_new.groupby('RCS').agg({'financials':sum}).reset_index()


        RCS_output = pd.merge(RCS_output, RBE_output, how='left', on='RCS')
        del RBE_output
        RCS_output = pd.merge(RCS_output, immat_df, how='left', on='RCS')
        del immat_df
        RCS_output = pd.merge(RCS_output, bilan_DF_new, how='left', on='RCS')
        del bilan_DF_new

        RCS_output['Gérant/Administrateur'] = RCS_output['Gérant/Administrateur'].fillna('').apply(cleanjusqua)
        RCS_output['Délégué à la gestion journalière'] = RCS_output['Délégué à la gestion journalière'].fillna('').apply(cleanjusqua)
        RCS_output['Personne(s) chargée(s) du contrôle des comptes'] = RCS_output['Personne(s) chargée(s) du contrôle des comptes'].fillna('').apply(cleanjusqua)
    else:
        RCS_output = pd.DataFrame()
    return RCS_output

if __name__ == '__main__':
    main()