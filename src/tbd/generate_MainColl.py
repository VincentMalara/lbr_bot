from configs import settings
from datetime import datetime
import pandas as pd


from src.utils.timer import performance_timer
from src.utils.create_initial_RCS import create_exhaustive_list

from src.mongo.main import mongo


timer_main = performance_timer()
col_RCS = settings.col_RCS
col_RBE = settings.col_RBE

Mongo = mongo(col=col_RCS)
Mongorbe = mongo(col=col_RBE)

"""
RCS = create_exhaustive_list()
"""
RCS = ['E831', 'B249879', 'B221018', 'B221019', 'B100006', 'B100007', 'B100008', 'B100009', 'B106338', 'B56047',
       'B248373', 'E5555', 'E1879', 'E4738', 'B182934', 'B215643', 'B120246', 'B120247', 'E229', 'E2818', 'E2226',
       'E4589', 'B14674', 'B14710', 'B14815', 'B15268', 'B15467', 'B15489', 'B15590', 'B15846', 'B15904', 'B16286',
       'B16336', 'B16468', 'B16607', 'B16677', 'B16768', 'B16844', 'B16854', 'B16855', 'B16923', 'B17016', 'B16924',
       'B17015', 'B17020', 'B17218', 'B17286', 'B17298', 'B17479', 'B39099', 'A39456']


Mongo.delete()
Mongorbe.delete()

DF = pd.DataFrame()
DF['RCS'] = RCS
DF['extraction_date'] = datetime.today().strftime("%d/%m/%Y")
DF['status'] = "to_be_updated"
DF['info'] = ''
DF['scraper_version'] = ''
DF['task_index'] = 0

print(DF.shape)

Mongo.insert(DF)
Mongo.close()
Mongorbe.close()