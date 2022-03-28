from src.utils.handle_RCS_list import main as rcs_input_checker
from src.pdf_parsers.publications.parser import main as parser
from src.pdf_parsers.publications.utils import *
from src.pdf_downloaders.utils import is_valid_publi


def main(RCS=None, mongo='', mongoparsed='', onlynew=True):
    task_index = mongoparsed.get_index_max() + 1
    dict_ = {}
    if RCS is not None:
        list, dict_rcs,  status, msg = rcs_input_checker(RCS=RCS)
        dict_ = {**dict_, **dict_rcs}

    RCSDF = mongo.find(dict_)
    RCSDF = RCSDF[RCSDF.apply(is_valid_publi, axis=1)].reset_index(drop=True)

    if onlynew:
        alreadydone = mongoparsed.find_from_RCSlist(RCSDF)
        if 'N_depot' in alreadydone.columns:
            if alreadydone.shape[0] > 0:
                dict_on = {"N_depot": {'$nin': alreadydone['N_depot'].to_list()}}
                dict_ = {**dict_, **dict_on}
                RCSDF = mongo.find(dict_)
    else: #--> reparse all in ths case
        print("All publi will be reparsed")
        mongoparsed.delete()
        task_index = -1

    if RCSDF.shape[0] > 0:
        print(RCSDF['Type_de_depot'].value_counts())
        RCSparsed = RCSDF.apply(lambda x: parser(x, task_index), axis=1).to_list()
        mongoparsed.insert(RCSparsed)
        mongoparsed.drop_duplicates(colsel='task_index', coldup='N_depot')
    else:
        print("error in pdf parser financials : empty dataframe")


if __name__ == '__main__':
    main()