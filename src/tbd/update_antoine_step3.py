from configs import settings
from src.scrapers.rcs.main import main as rcs_scraper
from src.html_parsers.rcs.main import main as rcs_parser


from src.utils.timer import performance_timer
from .utils_antoine import *

from src.mongo.main import mongo


timer_main = performance_timer()


mongo_ip = settings.mongo_ip
mongo_port = settings.mongo_port
mongo_DB = settings.mongo_DB
col_RCS = settings.col_RCS
col_RCSp = settings.col_RCSp


Mongorcs = mongo(db=mongo_DB,  col=col_RCS, ip='146.59.152.231')
Mongorcsp = mongo(db=mongo_DB,  col=col_RCSp, ip='146.59.152.231')


RCS_list_DF = Mongorcsp.find()

RCS_list_DF.to_excel('1320companies_check.xlsx', index=False)


print(RCS_list_DF.shape)

RCS_output = pd.DataFrame()
print('processing RCS, name, denom, register numb')
RCS_output['RCS'] = RCS_list_DF['RCS']
print('processing names')
RCS_output['name'] = RCS_list_DF['company name'].apply(get_name)
print(f"names processed, timer : {str(timer_main.stop())}s")
RCS_output['Denomination'] = RCS_list_DF['Dénomination(s)']
RCS_output['registerNumber'] = RCS_list_DF['RCS']
print('processing year')
RCS_output['yearOfCreation'] = RCS_list_DF["Date d'immatriculation"].apply(get_year)
print('processing nace, forme juridique')
RCS_output['NACE'] = RCS_list_DF['Code NACE (Information mise à jour mensuellement)'].apply(get_nace)
RCS_output['legalStatus'] = RCS_list_DF['Forme juridique']
print('processing nb subsi')
RCS_output['numberOfSubsidiaries'] = RCS_list_DF['succursales'].apply(get_nb_sub)
print('processing is active, is HQ')
RCS_output['IsActive'] = RCS_list_DF['company status'].isnull()
RCS_output['IsHQ'] = True
print('processing address')
RCS_output['Siège social'] = RCS_list_DF['Siège social']

for label in ['Siège', "Adresse où s'exerce l'activité commerciale"]:
    try:
        RCS_output[label] = RCS_list_DF[label]
    except:
        RCS_output[label] = ''


RCS_output['address_tbd'] = RCS_output['Siège social'].fillna("") +RCS_output["Adresse où s'exerce l'activité commerciale"].fillna("") +RCS_output["Siège"].fillna("")
RCS_output['address_tbd'] = RCS_output['address_tbd'].apply(manage_adress2)
RCS_output[['czip', 'city', 'address1']] = RCS_output['address_tbd'].apply(pd.Series)
RCS_output['country'] = "Luxembourg"
try:
    RCS_output['Replaced by'] = RCS_list_DF['Replaced by']
    RCS_output['old RCS'] = RCS_list_DF['old RCS']
except:
    RCS_output['Replaced by'] = ''
    RCS_output['old RCS'] = ''


RCS_output['is not Lux'] = RCS_output['name'].apply(is_succur)
RCS_output['to be del'] = RCS_output.fillna('').apply(findtobedel, axis=1)


RCS_output = RCS_output.drop(columns=['address_tbd','Siège social', "Adresse où s'exerce l'activité commerciale"])

print(f"address processed, timer : {str(timer_main.stop())}s")
print('saving')
RCS_output.to_excel('1320companies_update.xlsx', index=False)
RCS_output.to_csv('1320companies_update.csv', sep=';')
print(f"RCS processed, timer : {str(timer_main.stop())}s")




print(f"completed in {str(timer_main.stop())}s")





