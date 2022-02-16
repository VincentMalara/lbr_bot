from configs import settings
from src.scrapers.rbe.scraper import Rbe
from src.scrapers.resa.main import Resa
from src.utils.task_index import main as task_index


from .utils.timer import performance_timer

from src.mongo.main import mongo


timer_main = performance_timer()
col_RESA = settings.col_RESA

Mongo = mongo(col=col_RESA)

n=0
test = False
while not test and n<5:
    scraper = Resa(headless=False)
    scraper.launch()
    test = scraper.status
    print('---------')
    n += 1
    print(n)


scraper.scrap_day()

Mongo.insert(scraper.pages)

print(f"connected in {str(timer_main.stop())}s")

timer_main = performance_timer()
