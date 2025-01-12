import sys


import pandas as pd
import numpy as np


from configs import settings
from src.utils.handle_RCS_list import main as rcs_input_checker
from src.utils.RCS_spliter import main as rcs_spliter


NMAX = settings.NMAX

def main(type_='rcs', rcs=None, mongo='', mongoparsed='', onlynew=True):

    if type_ == 'rcs':
        from src.html_parsers.rcs.parser import main as parser
        from src.html_parsers.rcs.manage_changed_RCS import main as manage_changed_RCS
    elif type_ == 'rbe':
        from src.html_parsers.rbe.parser import main as parser

    if not onlynew and rcs is None:
        mongoparsed.delete()

    task_index = mongoparsed.get_index_max() + 1

    if rcs is not None:
        list_, dict_rcs, status, msg = rcs_input_checker(RCS=rcs)
        if not status:
            print(msg)
            sys.exit()
        mongoparsed.delete(dict_rcs)
        base_RCS_list = list_
    else:
        dict_ = {'status': 'scraped'}
        #dict_ = {}
        base_RCS_list = mongo.get_RCSlist(dict_)


    RCS_splited_lists = rcs_spliter(base_RCS_list, NMAX)
    #print(len(RCS_splited_lists))

    RCSDF = pd.DataFrame()
    test = False

    for count, rcslist in enumerate(RCS_splited_lists):
        status = True
        #print(count)
        if onlynew:
            #print('A')
            alreadydone = mongoparsed.find_from_RCSlist(rcslist)
            #print('already done done')
            if 'RCS' in alreadydone.columns:
                if alreadydone.shape[0] > 0:
                    #print('B')
                    rcslist = np.array(rcslist)
                    #print(rcslist.shape)
                    rcslist=rcslist[~np.isin(rcslist, alreadydone['RCS'])].tolist()
                    #print(len(rcslist))
            if len(rcslist) > 0:
                #print('C')
                dict_split = {"RCS": {'$in': rcslist}}
                dict_ = {**dict_, **dict_split}
                RCSDF = mongo.find(dict_)
                #print(RCSDF.head())
                #print('RCSDF done')
            else:
                status = False
        else: #not only new
            if rcs is None: #--> repars all in ths case
                #print("All RCS will be reparsed")
                task_index = -1
            RCSDF = mongo.find_from_RCSlist(rcslist)


        if RCSDF.shape[0] == 0:
            print('RCSDF is empty')
            status = False

        if status:
            #print('processing')
            RCSparsed = RCSDF.apply(lambda x: parser(x, task_index), axis=1).to_list()
            #print(len(RCSparsed))
            #print(RCSparsed[0])
            #print('processed')
            if onlynew:
                mongoparsed.delete(data=RCSDF, RCS=True)
                #print('deleted')
            mongoparsed.insert(RCSparsed)
            #print('inserted')
            test = True

    if test and (type_ == 'rcs'):
        manage_changed_RCS(mongoparsed)


if __name__ == '__main__':
    main()