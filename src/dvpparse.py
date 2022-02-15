import pandas as pd

from configs import settings
from src.scrapers.rbe.main import Rbe
from src.scrapers.rcs.main import Rcs
from src.html_parsers.main import rcs as rcsparser
from src.html_parsers.main import rbe as rbeparser

from src.utils.task_index import main as task_index


from .utils.timer import performance_timer

from src.mongo.main import mongo



timer_main = performance_timer()


mongo_ip = settings.mongo_ip
mongo_port = settings.mongo_port
mongo_DB = settings.mongo_DB
col_RCS = settings.col_RCS
col_RBE = settings.col_RBE
col_RCSp = settings.col_RCSp
col_RBEp = settings.col_RBEp
for col, col2 in zip([col_RCS, col_RBE], [col_RCSp, col_RBEp]):
    Mongo = mongo(db=mongo_DB,  col=col)
    Mongoparsed = mongo(db=mongo_DB,  col=col2)


    if col == col_RCS:
        RCS = Mongo.find()
        RCSparsed = rcsparser(RCS)
    else:
        RCS = Mongo.get_RCSlist({'status':'scraped'})
        print(RCS)
        RCSparsed = rbeparser(RCS)

    Mongoparsed.insert(RCSparsed)
    Mongoparsed.drop_duplicates(colsel='task_index', coldup='RCS')
    Mongo.close()
    Mongoparsed.close()


print('------')
Mongo = mongo(col=col_RCSp)
RCSlist = Mongo.get_RCSlist({"changed_RCS_number":True})
print(RCSlist)
Mongo.close()

Mongo = mongo(col=col_RCS)
Mongo.set_status('changed_RCS_number',RCSlist)
Mongo.close()

Mongo = mongo(col=col_RBE)
Mongo.set_status('changed_RCS_number',RCSlist)
Mongo.close()

#set old name
print('------')





