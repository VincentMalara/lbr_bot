from datetime import datetime

from configs import settings
from src.mongo.main import mongo


col_RCS = settings.col_RCS
col_RBE = settings.col_RBE
col_RCSp = settings.col_RCSp
col_RBEp = settings.col_RBEp


dict_tasks = {
    'scraper': {'rcs': settings.col_RCS, 'rbe': settings.col_RBE},
    'parser': {'rcs': settings.col_RCSp, 'rbe': settings.col_RBEp},
              }


def main(task='scraper', lbrtype='rcs'):
    status = False
    index = 0
    if task in dict_tasks.keys():
        if lbrtype in dict_tasks[task].keys():
            col = dict_tasks[task][lbrtype]
            status = True
        else:
            print(f"unknown lbrtype: {lbrtype}")
    else:
        print(f"unknown task: {task}")

    if status:
        Mongo = mongo(col=col)
        index = Mongo.get_index_max()
        Mongo.close()
    return index
