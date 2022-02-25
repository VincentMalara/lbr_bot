from configs import settings
from .pdf_downloaders.get_new_pdfs.main import main as dl_pdfs
from src.mongo.main import mongo
from .utils.timer import performance_timer


mongo_ip = settings.mongo_ip
mongo_port = settings.mongo_port
mongo_DB = settings.mongo_DB
col_RCSp = settings.col_RCSp
col_pdfs = settings.col_pdfs

timer_main = performance_timer()

Mongorcsp = mongo(db=mongo_DB,  col=col_RCSp)
Mongopdf = mongo(db=mongo_DB,  col=col_pdfs)


print("***Downloading PDFs***")
dl_pdfs(mongo_rcsparsed=Mongorcsp, mongo_pdfs=Mongopdf)

print(f"***completed in {str(timer_main.stop())}s***")

