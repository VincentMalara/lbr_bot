import pandas as pd
from configs import settings
from src.scrapers.main import main as scraper
from src.html_parsers.main import main as rcs_parser
from src.pdf_downloaders.main import main as pdf_downloader
from src.pdf_parsers.financials.main import main as financials_parser
from src.pdf_parsers.publications.main import main as publi_parser
from src.utils.RCS_spliter import main as rcs_spliter
from src.scrapers.resa.utils import Resa


from .utils.timer import performance_timer
from src.mongo.main import mongo


Mongofinan = mongo(ip='146.59.152.231', db='LBR_test', col='financials')

axiomatic = pd.read_excel('AXIOMATIC.xlsx')

RCSlist = axiomatic['Register_Number_Neossys'].to_list()

bilans = Mongofinan.find_from_RCSlist(RCS = RCSlist)

bilans = bilans[bilans['year']==2019].reset_index()
print(bilans)
bilans = bilans[['RCS', 'depot', 'correction']]
bilans.sort_values(by='correction', inplace = True)
bilans=bilans.groupby('RCS').agg('last').reset_index()
bilans.columns = ['Register_Number_Neossys', 'depot', 'correction']
print(bilans)


axiomatic = axiomatic.merge(bilans, on='Register_Number_Neossys', how='left')
print(axiomatic)

axiomatic.to_excel('AXIOMATIC_modified.xlsx')
