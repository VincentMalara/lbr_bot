import sys


import pandas as pd


from configs import settings
from src.html_parsers.rcs.parser import main as parser
from .manage_changed_RCS import main as manage_changed_RCS
from src.utils.handle_RCS_list import main as rcs_input_checker
from src.utils.RCS_spliter import main as rcs_spliter


NMAX = settings.NMAX

def main(RCS=None, mongo='', mongoparsed='', onlynew=True):
    if not onlynew:
        mongoparsed.delete()

    task_index = mongoparsed.get_index_max() + 1
    dict_ = {'status':'scraped'}
    if RCS is not None:
        list_, dict_rcs, status, msg = rcs_input_checker(RCS=RCS)
        if not status:
            print(msg)
            sys.exit()
        dict_ = {**dict_, **dict_rcs}

    base_RCS_list = mongo.get_RCSlist(dict_)


    RCS_splited_lists = rcs_spliter(base_RCS_list, NMAX)

    RCSDF = pd.DataFrame()
    test = False

    for rcslist in RCS_splited_lists:
        if onlynew:
            alreadydone = mongoparsed.find_from_RCSlist(rcslist)
            if 'RCS' in alreadydone.columns:
                if alreadydone.shape[0] > 0:
                    dict_on = {"RCS": {'$nin': alreadydone['RCS'].to_list()}}
                    dict_ = {**dict_, **dict_on}
                RCSDF = mongo.find(dict_)
        else:
            if RCS is None: #--> repars all in ths case
                #print("All RCS will be reparsed")
                RCSDF = mongo.find_from_RCSlist(rcslist)
                task_index = -1

        if RCSDF.shape[0] > 0:
            RCSparsed = RCSDF.apply(lambda x: parser(x, task_index), axis=1).to_list()
            if onlynew:
                mongoparsed.delete(data=RCSDF, RCS=True)
            mongoparsed.insert(RCSparsed)
            test = True
        else:
            print("error in rcs parser : empty dataframe")

    if test:
        manage_changed_RCS(mongoparsed)


if __name__ == '__main__':
    main()