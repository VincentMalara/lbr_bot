from configs import settings
from src.scrapers.main import main as scraper
from src.html_parsers.main import main as rcs_parser
from src.pdf_downloaders.main import main as pdf_downloader
from src.pdf_parsers.financials.main import main as financials_parser
from src.pdf_parsers.publications.main import main as publi_parser
from src.utils.RCS_spliter import main as rcs_spliter

from src.utils_merger import *


from .utils.timer import performance_timer
from src.mongo.main import mongo

import pandas as pd

timer_main = performance_timer()

Mongorcsp = mongo(ip='146.59.152.231', db='LBR_test', col='RCS_parsed')
Mongorbep = mongo(ip='146.59.152.231', db='LBR_test', col='RBE_parsed')
Mongoresa = mongo(ip='146.59.152.231', db='LBR_test', col='RESA_parsed')
Mongopdf= mongo(ip='146.59.152.231', db='LBR_test', col='all_pdfs')
Mongopubli = mongo(ip='146.59.152.231', db='LBR_test', col='publications')

#find all the SCSP companies from RCS parsed
#RCSlist = Mongorcsp.get_RCSlist(dictin={"Forme juridique":{ '$regex' : '.*' + 'commandite spéciale' + '.*'}})

# added for Zarb mizi
DF = Mongorcsp.find({'Siège social':{ '$regex' : '.*' + 'Raiffeisen' + '.*'}})

DF=DF[DF['Siège social'].str.contains('15', case=False, regex=True)].reset_index(drop=True)
DF=DF[DF['Siège social'].str.contains('2411', case=False, regex=True)].reset_index(drop=True)

RCSlist = DF['RCS'].to_list()
### end zarb


print(len(RCSlist))


try:
    1/0
    RCS_output = pd.read_pickle('rcs_file.pkl')
except:
    #search by rcs number of SCSP companies from RCS parsed
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
    #print('processing nb subsi')
    #RCS_output['numberOfSubsidiaries'] = RCS_list_DF['succursales'].apply(get_nb_sub)
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
    RCS_output.to_excel('rcs_file.xlsx', index=False)
    RCS_output.to_pickle('rcs_file.pkl')


#RBE

try:
    1/0
    RBE_output = pd.read_pickle('rbe_file.pkl')
except:
    #search by rcs number of SCSP companies from RCS parsed


    RBE_list_DF = Mongorbep.find_from_RCSlist(RCS=RCSlist)
    print(RBE_list_DF.shape)
    print('processing RBE')
    RBE_output = pd.DataFrame()
    RBE_output['RCS'] = RBE_list_DF['RCS']
    RBE_output['Loi_2004'] = RBE_list_DF['Loi_2004']
    RBE_output['UBO'] = RBE_list_DF['Benef Economiques'].apply(get_ubo)

    def is_not_reg(x):
        return x == "not_registrated_BO"

    RBE_output['not registrated BO'] = RBE_list_DF['status'].apply(is_not_reg)

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
    1/0
    immat_df = pd.read_pickle('adm_file.pkl')

except:
    print(f"Build company history starting on: {datetime.now()}")
    timer_main = performance_timer()

    # find all the SCSP companies from RCS parsed
    #RCSlist = Mongorcsp.get_RCSlist(dictin={"Forme juridique": {'$regex': '.*' + 'commandite spéciale' + '.*'}})
    print(len(RCSlist))

    LBR_RCS_file_DF = Mongopubli.find_from_RCSlist(RCS=RCSlist)
    LBR_RCS_file_DF['Date'] = pd.to_datetime(LBR_RCS_file_DF['Date'], format='%d/%m/%Y')
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
            immat_df[label_+ '_base']  = immat_df[label_].apply(clean_list)
            immat_df[label_]= immat_df[label_+ '_base'].apply(build_hist)
            list_col.append(label_)


    immat_df = immat_df[list_col].reset_index()
    print('saving')
    immat_df.to_excel('adm_file.xlsx', index=False)
    immat_df.to_pickle('adm_file.pkl')
    print(f"ADM/ASSO done, timer : {str(timer_main.stop())}s")


print('Merging RBE')
RCS_output = pd.merge(RCS_output, RBE_output, how='left', on='RCS')
print('Merging ADM')
RCS_output = pd.merge(RCS_output, immat_df, how='left', on='RCS')

RCS_output=RCS_output[RCS_output['IsActive']].reset_index(drop=True)

RCS_output.to_excel('full_file.xlsx', index=False)
print(f"adm Merged, timer : {str(timer_main.stop())}s")







'''
    if ADMASSO_FROM_CSV:
        print('Loading financials from CSV')
        AdmAsso_DF_new = pd.read_csv(file_admasso + '.csv', sep=';')
        print(f'Financials loaded from CSV, timer : {str(timer_main.stop())}s')
    else:
        CHECK_FILELIST = 'C:/Users/Utilisateur/PycharmProjects/LBR_collect_and_process/src/Data/check_filelist'
        AdmAsso_DF = pd.read_pickle(CHECK_FILELIST + '_3.pkl')
        print(AdmAsso_DF.shape)
        print(f"LBR Adm Asso loaded, timer : {str(timer_main.stop())}s")
        print('processing adm asso')
        AdmAsso_DF_new = pd.DataFrame()
        AdmAsso_DF_new['RCS'] = AdmAsso_DF['RCS']


        def cleanratecompletaion(data):
            output={}
            if isinstance(data, dict):
                for i in data.keys():
                    dict_={}
                    if data[i]['needed']>0:
                        output[i + ' rating : feasible_ratio'] = round(data[i]['utilisé']/data[i]['needed'],2)

                    if data[i]['needed_full'] > 0:
                        output[i + ' rating : overall_ratio'] = round(data[i]['utilisé'] / data[i]['needed_full'],2)
            return output

        AdmAsso_DF_new_ = AdmAsso_DF['rate_completion_clean'].apply(cleanratecompletaion).apply(pd.Series)



        contactlist = list_personne

        for label in contactlist:
            try:
                AdmAsso_DF_new[label] = AdmAsso_DF[label+'_clean'].fillna('').apply(lambda x: '; '.join(x))
            except:
                pass
            for ii in [ ' rating : feasible_ratio', ' rating : overall_ratio']:
                try:
                    AdmAsso_DF_new[label + ii] = AdmAsso_DF_new_[label + ii]
                except:
                    pass
        AdmAsso_DF_new['rate_completion_clean'] = AdmAsso_DF['rate_completion_clean']



print('Merging RBE')
RCS_output = pd.merge(RCS_output, RBE_output, how='left', on='RCS')
print(f"RBE Merged, timer : {str(timer_main.stop())}s")
print('Merging Adm Asso')
RCS_output = pd.merge(RCS_output, AdmAsso_DF_new, how='left', on='RCS')
print(f"Adm Asso merged, timer : {str(timer_main.stop())}s")
RCS_output = RCS_output.drop(columns=['IsHQ', 'Siège', 'numberOfSubsidiaries'])
print('saving')
RCS_output.to_excel(f'Luxembourg_merged_{suffix}.xlsx', index=False)
RCS_output.to_csv(f'Luxembourg_merged_{suffix}.csv', index=False, sep=';')
print(f"saved, timer : {str(timer_main.stop())}s")
'''
