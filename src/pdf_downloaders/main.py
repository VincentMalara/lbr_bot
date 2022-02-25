from .get_new_pdfs.main import main as get_new_pdfs
from configs import settings
from src.mongo.main import mongo

mongo_ip = settings.mongo_ip
mongo_port = settings.mongo_port
mongo_DB = settings.mongo_DB
col_RCSp = settings.col_RCSp
col_pdfs = settings.col_pdfs

