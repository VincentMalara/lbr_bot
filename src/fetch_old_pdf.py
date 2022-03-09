from configs import settings
from .utils.timer import performance_timer
from src.mongo.main import mongo

from src.pdf_downloaders.main import main as pdf_downloader
from src.pdf_parsers.publications.main import main as parser


import pandas as pd

timer_main = performance_timer()

Mongorcs = mongo(db=settings.mongo_DB,  col=settings.col_RCS)
Mongorcsp = mongo(db=settings.mongo_DB,  col=settings.col_RCSp)
Mongorbe = mongo(db=settings.mongo_DB,  col=settings.col_RBE)
Mongorbep = mongo(db=settings.mongo_DB,  col=settings.col_RBEp)
Mongopdf = mongo(db=settings.mongo_DB,  col=settings.col_pdfs)
Mongofinancials = mongo(db=settings.mongo_DB,  col=settings.col_finan)
Mongopubli = mongo(db=settings.mongo_DB,  col=settings.col_publi)

RCSlist = ['A39456', 'B100006', 'B100007', 'B100008', 'B100009', 'B106338', 'B120246', 'B1202461', 'B120247', 'B120248',
           'B14674', 'B14710', 'B14815', 'B15268', 'B15425', 'B15467', 'B15489', 'B15490', 'B15491', 'B15492', 'B15590',
           'B15846', 'B15904', 'B16286', 'B16336', 'B16468', 'B16607', 'B16677', 'B16768', 'B16844', 'B16854', 'B16855',
           'B16923', 'B16924', 'B17015', 'B17016', 'B17020', 'B17218', 'B17286', 'B17298', 'B17479', 'B182934', 'B215643',
           'B221018', 'B221019', 'B248373', 'B249879', 'B255380', 'B262114', 'B39099', 'B56047', 'E1879', 'E2226', 'E229',
           'E2818', 'E4589', 'E4738', 'E5555', 'E7052', 'E7905', 'E831','B256373']

RCSlist = ['B204458', "B151181", "B103420", 'B211048', 'B48681', 'B239074'  ]

'''
Mongorcsp = mongo(ip= '146.59.152.231',db=settings.mongo_DB,  col=settings.col_RCSp)

print(Mongorcsp.find_from_RCSlist(RCSlist))

pdf_downloader(RCS=RCSlist, mongo_rcsparsed=Mongorcsp, mongo_pdfs=Mongopdf)

print(pdflist.head())
pdflist.to_pickle('pdflist.pkl')


pdflist = pd.read_pickle('pdflist.pkl')

N_depot = pdflist['N_depot'].to_list()

Mongoadmasso = mongo(db="LBR_new",  col="LBR_Adm_and_Asso")

dict_ = {"N_depot": {'$in': N_depot}}

Mongoadmasso_df = Mongoadmasso.find(dict_)

Mongoadmasso_df = Mongoadmasso_df[['N_depot', 'file']]

pdflist = Mongoadmasso_df.merge(pdflist, on='N_depot', how='left')
pdflist.to_pickle('pdflist2.pkl')

Mongopubli = mongo(db=settings.mongo_DB,  col=settings.col_publi)

pdflist = pd.read_pickle('pdflist2.pkl').sample(1000, random_state=0)

Mongorcsp = mongo(ip= '146.59.152.231',db=settings.mongo_DB,  col=settings.col_RCSp)
pdf_downloader(RCS=['B48681','B239074'], mongo_rcsparsed=Mongorcsp, mongo_pdfs=Mongopdf)
Mongopubli.delete()
'''

#pdflist=pdflist[pdflist['RCS'] == "F11549"]   #B218577  B231625  B179072" B213483  B252807  B218577  B245085


parser(mongo=Mongopdf, mongoparsed=Mongopubli, onlynew=False)
