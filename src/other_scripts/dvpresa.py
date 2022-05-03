from configs import settings
from src.scrapers.rbe.scraper import Rbe
from src.scrapers.resa.utils import Resa
from src.utils.task_index import main as task_index


from src.utils.timer import performance_timer

from src.mongo.main import mongo


timer_main = performance_timer()

from urllib.request import urlopen
import xmltodict


Mongo = mongo(col=settings.col_RESA)
Mongo_parsed = mongo(col=settings.col_RESAp)

Mongo.delete()

n=0
test = False
while not test and n<5:
    scraper = Resa(headless=False)
    scraper.launch()
    test = scraper.status
    print('---------')
    n += 1
    print(n)

for month in ['Novembre', "Décembre"]:
    scraper.scrap_day('2021', month)
    Mongo.insert(scraper.pages)

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

print(f"connected in {str(timer_main.stop())}s")

