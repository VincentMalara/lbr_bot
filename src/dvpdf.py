from configs import settings
from .pdf_downloaders.get_new_pdfs.main import main as dl_pdfs
from .pdf_downloaders.get_new_pdfs.utils import clean_temp

from src.mongo.main import mongo
from .utils.timer import performance_timer

timer_main = performance_timer()
clean_temp()
print(f"***cleanded in {str(timer_main.stop())}s***")

'''
mongo_ip = settings.mongo_ip
mongo_port = settings.mongo_port
mongo_DB = settings.mongo_DB
col_RCSp = settings.col_RCSp
col_pdfs = settings.col_pdfs



Mongorcsp = mongo(db=mongo_DB,  col=col_RCSp)
Mongopdf = mongo(db=mongo_DB,  col=col_pdfs)




print("***Downloading PDFs***")
dl_pdfs(mongo_rcsparsed=Mongorcsp, mongo_pdfs=Mongopdf)

print(f"***completed in {str(timer_main.stop())}s***")
'''

