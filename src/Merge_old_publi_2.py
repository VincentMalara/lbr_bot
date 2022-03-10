from configs import settings
from src.pdf_downloaders.utils import PdfDownloader, check_temp_exist
from src.mongo.main import mongo

check_temp_exist() #needed in case temp folder is not in the project

mongo_DB = settings.mongo_DB
col_RCSp = settings.col_RCSp
col_pdfs = settings.col_pdfs


Mongo_publi_old = mongo(ip='146.59.152.231', db='LBR_test', col='publi_old')

Mongorcsp = mongo(ip='146.59.152.231', db='LBR_test', col='RCS_parsed')

Mongo_allpdfs = mongo(ip='146.59.152.231', db='LBR_test', col='all_pdfs')

print(f"building pdf list")
pdfdownloader = PdfDownloader( mongo_rcsparsed=Mongorcsp, mongo_pdfs=Mongo_allpdfs)
pdf_list = pdfdownloader.get_pdfs_list()
print(pdf_list)


Mongo_publi_old.find(dictin={'N_depot': {'$in': pdf_list['depot'].unique().tolist() }},
                     dictout={'N_depot': 1,'file':1,'extraction_date':1 , '_id': 0})