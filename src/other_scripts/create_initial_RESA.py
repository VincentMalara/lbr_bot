from configs import settings
from src.scrapers.rbe.scraper import Rbe
from src.scrapers.resa.utils import Resa
from src.utils.task_index import main as task_index


from src.utils.timer import performance_timer

from src.mongo.main import mongo


timer_main = performance_timer()

from urllib.request import urlopen
import xmltodict


Mongoresa = mongo(ip='146.59.152.231', db='LBR_test', col='RESA')
Mongoresaparsed = mongo(ip='146.59.152.231', db='LBR_test', col='RESA_parsed')

#Mongo.delete()
#Mongo_parsed.delete()

n=0
test = False
while not test and n<5:
    scraper = Resa(mongoRESA=Mongoresa, mongoRESAparsed=Mongoresaparsed)
    scraper.launch()
    test = scraper.status
    print('---------')
    n += 1
    print(n)

monthlist = [ 'Juillet-2021', 'Août-2021', 'Septembre-2021', 'Octobre-2021', 'Novembre-2021', 'Décembre-2021',
                    'Janvier-2022', 'Février-2022']

monthlist=['Mars-2022']

for monthyear in monthlist:
    month = monthyear.split('-')[0]
    year = monthyear.split('-')[1]
    scraper.scrap_month(year, month)
    scraper.push_pages_to_mongo()
    scraper.extract_xmls()


print('scrapping done')
'''
print('starting dl and parsing')
DF = Mongo.find()
output = []
for index, row in DF.iterrows():
    urls = row['url']
    month = row['month']
    year = row['year']
    date = row['date']
    coderesa = row['codeRESA']
    for url in urls:
        file = urlopen(url)
        data = file.read()
        file.close()
        data = xmltodict.parse(data)
        dict_ = data['ns2:JournalDesPublications']['ns2:ListePublications']
        if isinstance(dict_['ns2:Publication'], list):
            for i in dict_['ns2:Publication']:
                dictout = dict(i)
                dictout['month'] = month
                dictout['year'] = year
                dictout['date'] = date
                dictout['codeRESA'] = coderesa
                dictout['status'] = 'to_be_updated'
                output.append(dictout)
        elif isinstance(dict_['ns2:Publication'], dict):
            dictout = dict(dict_['ns2:Publication'])
            dictout['month'] = month
            dictout['year'] = year
            dictout['date'] = date
            dictout['codeRESA'] = coderesa
            dictout['status'] = 'to_be_updated'
            output.append(dictout)

Mongo_parsed.insert(output)
'''

print(f"connected in {str(timer_main.stop())}s")

