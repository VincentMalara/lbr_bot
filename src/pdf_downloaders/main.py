from configs import settings
from .utils import PdfDownloader, check_temp_exist
from src.mongo.main import mongo

check_temp_exist() #needed in case temp folder is not in the project

mongo_DB = settings.mongo_DB
col_RCSp = settings.col_RCSp
col_pdfs = settings.col_pdfs

Mongorcsp = mongo(db=mongo_DB,  col=col_RCSp)
Mongopdf = mongo(db=mongo_DB,  col=col_pdfs)


def main(RCS=None, mongo_rcsparsed=Mongorcsp, mongo_pdfs=Mongopdf):
    print(f"building pdf list")
    pdfdownloader = PdfDownloader(RCS=RCS, mongo_rcsparsed=mongo_rcsparsed, mongo_pdfs=mongo_pdfs)
    pdfdownloader.get_pdfs_list()
    print(f"pdf listbuild")
    print(f"downloading pdfs")
    n = pdfdownloader.store_pdfs()
    print(f"{n} pdf downloaded successfully")


if __name__ == '__main__':
    main()