from configs import settings
from .utils import build_pdfs_list, dl_pdfs_list
from src.mongo.main import mongo


mongo_DB = settings.mongo_DB
col_RCSp = settings.col_RCSp
col_pdfs = settings.col_pdfs

Mongorcsp = mongo(db=mongo_DB,  col=col_RCSp)
Mongopdf = mongo(db=mongo_DB,  col=col_pdfs)


def main(RCS=None, mongo_rcsparsed=Mongorcsp, mongo_pdfs=Mongopdf):
    print("---building file list---")
    DF = build_pdfs_list(RCS, mongo_rcsparsed=mongo_rcsparsed, mongo_pdfs=mongo_pdfs)
    print("---File list done---")
    print(f"---Downloading pdf files and storing to {mongo_pdfs.col}---")
    N = dl_pdfs_list(DF, mongo_pdfs)
    print(f"---{N} pdf files downloaded---")


if __name__ == '__main__':
    main()