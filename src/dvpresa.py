from configs import settings
from src.scrapers.rbe.scraper import Rbe
from src.scrapers.resa.main import Resa
from src.utils.task_index import main as task_index


from .utils.timer import performance_timer

from src.mongo.main import mongo


timer_main = performance_timer()
col_RESA = settings.col_RESA

'''
import urllib2
import xmltodict

def homepage(request):
    file = urllib2.urlopen('https://www.goodreads.com/review/list/20990068.xml?key=nGvCqaQ6tn9w4HNpW8kquw&v=2&shelf=toread')
    data = file.read()
    file.close()

    data = xmltodict.parse(data)
    return render_to_response('my_template.html', {'data': data})

'''


Mongo = mongo(col=col_RESA)

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

print(f"connected in {str(timer_main.stop())}s")

timer_main = performance_timer()
