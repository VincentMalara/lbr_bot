from src.merger.utils import *

from src.utils.timer import performance_timer
from src.mongo.main import mongo

import pandas as pd

timer_main = performance_timer()


Mongopubli = mongo(ip='146.59.152.231', db='LBR_test', col='publications')
Mongorbe = mongo(ip='146.59.152.231', db='LBR_test', col='RBE_parsed')

rbe = Mongorbe.find()

publis = Mongorbe.find(dictout={'RCS': 1, 'Benef Economiques': 1, '_id':0})


print(publis)

#financ = Mongofinan.find_from_RCSlist(RCS=Data['RCS number'].to_list())
