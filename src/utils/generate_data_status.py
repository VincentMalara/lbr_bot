from datetime import datetime

import pandas as pd

from src.utils.timer import performance_timer
from src.mongo.main import mongo

timer_main = performance_timer()

DBstatus = mongo(ip='146.59.152.231', db='LBR_test', col='DB_status')

def countlist(x):
    if isinstance(x, list):
        y = len(x)
    else:
        y=0
    return y



#RCS parsed
Mongorcsp = mongo(ip='146.59.152.231', db='LBR_test', col='RCS_parsed')

DBstatus.delete({'collection':'RCS_parsed'})
DF = Mongorcsp.find()
DF['extraction_date'] = pd.to_datetime(DF['extraction_date'], format="%d/%m/%Y")

output = {}
output['collection'] = 'RCS_parsed'
output['date'] = datetime.today().strftime("%d/%m/%Y")
output['last_update'] = DF['extraction_date'].max().strftime("%d/%m/%Y")
output['oldest_update'] = DF['extraction_date'].min().strftime("%d/%m/%Y")
output['size'] = DF.shape[0]
output['exists']= DF[DF['exists']].shape[0]
output['company status'] =pd.DataFrame(DF['company status'].fillna('active').value_counts()).to_dict(orient='dict')['company status']

output['Forme juridique'] =pd.DataFrame(DF['Forme juridique'].fillna('unknown').value_counts()).to_dict(orient='dict')['Forme juridique']

DBstatus.insert(output)
del output, DF
Mongorcsp.close()


#RBE parsed
Mongorbep = mongo(ip='146.59.152.231', db='LBR_test', col='RBE_parsed')

DBstatus.delete({'collection':'RBE_parsed'})
DF = Mongorbep.find()
DF['extraction_date'] = pd.to_datetime(DF['extraction_date'], format="%d/%m/%Y")

output = {}
output['collection'] = 'RBE_parsed'
output['date'] = datetime.today().strftime("%d/%m/%Y")
output['last_update'] = DF['extraction_date'].max().strftime("%d/%m/%Y")
output['oldest_update'] = DF['extraction_date'].min().strftime("%d/%m/%Y")
output['size'] = DF.shape[0]
output['Loi_2004'] = int(DF['Loi_2004'].fillna(False).sum())
output['aucun_RBE'] = int(DF['aucun_RBE'].fillna(False).sum())


output['Total_RBE_persons'] = int(DF['Benef Economiques'].apply(countlist).sum())

print(output)

DBstatus.insert(output)
del output, DF
Mongorbep.close()


#Financials parsed
Mongofinan = mongo(ip='146.59.152.231', db='LBR_test', col='financials')

DBstatus.delete({'collection':'financials'})
DF = Mongofinan.find()
DF['extraction_date'] = pd.to_datetime(DF['extraction_date'], format="%d/%m/%Y")

output = {}
output['collection'] = 'financials'
output['date'] = datetime.today().strftime("%d/%m/%Y")
output['last_update'] = DF['extraction_date'].max().strftime("%d/%m/%Y")
output['oldest_update'] = DF['extraction_date'].min().strftime("%d/%m/%Y")

output['size'] = DF.shape[0]
output['company']= len(DF['RCS'].unique().tolist())
output['type bilan'] =pd.DataFrame(DF['type bilan'].fillna('aucun').value_counts()).to_dict(orient='dict')['type bilan']
output['type compte resultat'] =pd.DataFrame(DF['type compte resultat'].fillna('aucun').value_counts()).to_dict(orient='dict')['type compte resultat']

output['per year'] =pd.DataFrame(DF['year'].astype(str).fillna('unknown').value_counts()).to_dict(orient='dict')['year']


DBstatus.insert(output)
del output, DF
Mongofinan.close()


#publi parsed
Mongopubli = mongo(ip='146.59.152.231', db='LBR_test', col='publications')

list_label = [ 'RCS', 'Type_de_depot', 'lang', 'readable', 'Gérant/Administrateur',
              'Personne(s) chargée(s) du contrôle des comptes', 'Actionnaire/Associé','Délégué à la gestion journalière']

dictout = {key:1 for key in list_label}
dictout['_id']=0
print(dictout)

DBstatus.delete({'collection':'publications'})
DF = Mongopubli.find(dictout=dictout)

output = {}
output['collection'] = 'publications'
output['date'] = datetime.today().strftime("%d/%m/%Y")

output['size'] = DF.shape[0]
output['company']= len(DF['RCS'].unique().tolist())
output['Type_de_depot'] =pd.DataFrame(DF['Type_de_depot'].fillna('aucun').value_counts()).to_dict(orient='dict')['Type_de_depot']
output['language'] =pd.DataFrame(DF['lang'].astype(str).fillna('unknown').value_counts()).to_dict(orient='dict')['lang']
output['readable'] = int(DF['readable'].fillna(False).sum())

output['Total_Gérant/Administrateur'] = int(DF['Gérant/Administrateur'].apply(countlist).sum())
output['Total_controle_compte'] = int(DF['Personne(s) chargée(s) du contrôle des comptes'].apply(countlist).sum())
output['Total_Actionnaire/Associé'] = int(DF['Actionnaire/Associé'].apply(countlist).sum())
output['Total_gestion_journalière'] = int(DF['Délégué à la gestion journalière'].apply(countlist).sum())


DBstatus.insert(output)
del output, DF
Mongopubli.close()


DBstatus.close()



print(f"completed in {str(timer_main.stop())}s")
